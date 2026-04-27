import { randomUUID } from "crypto";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import express, { Request, Response } from "express";
import { z } from "zod";

// ─────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────

const REDIS_URL = process.env.UPSTASH_REDIS_REST_URL!;
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN!;
const PORT = parseInt(process.env.PORT || "3001", 10);

if (!REDIS_URL || !REDIS_TOKEN) {
  throw new Error("Missing UPSTASH_REDIS_REST_URL or UPSTASH_REDIS_REST_TOKEN");
}

// ─────────────────────────────────────────────
// CREDIT PACKS
// ─────────────────────────────────────────────

type PackCode = "basic" | "standard" | "plus" | "ultimate";

interface CreditPack {
  code: PackCode;
  name: string;
  priceCents: number;
  credits: number;
  description: string;
}

const CREDIT_PACKS: CreditPack[] = [
  {
    code: "basic",
    name: "Basic Pack",
    priceCents: 599,
    credits: 20,
    description: 'The "Quick Hit" for curious users.',
  },
  {
    code: "standard",
    name: "Standard Pack",
    priceCents: 999,
    credits: 40,
    description: 'The "Mid-Tier" casualty.',
  },
  {
    code: "plus",
    name: "Plus Pack",
    priceCents: 1999,
    credits: 115,
    description: "Forced choice between Training or FaceTime.",
  },
  {
    code: "ultimate",
    name: "Ultimate Pack",
    priceCents: 9999,
    credits: 750,
    description: "The whale pack.",
  },
];

const PACK_BY_CODE = new Map<PackCode, CreditPack>(
  CREDIT_PACKS.map((pack) => [pack.code, pack]),
);

// ─────────────────────────────────────────────
// TYPES
// ─────────────────────────────────────────────

type LedgerEntryType = "purchase" | "usage" | "refund" | "adjustment";
type ReferenceType = "pack" | "generation" | "refund" | "manual" | "system";

interface LedgerEntry {
  id: string;
  userId: string;
  entryType: LedgerEntryType;
  amount: number;
  balanceAfter: number;
  referenceType: ReferenceType;
  referenceId: string;
  idempotencyKey?: string;
  description?: string;
  createdAt: string;
}

interface ApplyCreditChangeArgs {
  userId: string;
  amount: number;
  entryType: LedgerEntryType;
  referenceType: ReferenceType;
  referenceId: string;
  idempotencyKey?: string;
  description?: string;
}

// ─────────────────────────────────────────────
// REDIS KEYS
// ─────────────────────────────────────────────

const walletKey = (userId: string) => `credits:${userId}:wallet`;
const ledgerKey = (userId: string) => `credits:${userId}:ledger`;
const idempotencyKeyFor = (userId: string, key: string) =>
  `credits:${userId}:idem:${key}`;
const lockKey = (userId: string) => `credits:${userId}:lock`;

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeUserId(raw: string): string {
  return raw.trim().toLowerCase();
}

function safeParseNumber(value: unknown, fallback = 0): number {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  }
  return fallback;
}

function jsonText(payload: unknown) {
  return {
    content: [
      {
        type: "text" as const,
        text: JSON.stringify(payload, null, 2),
      },
    ],
  };
}

async function redisCommand<T = unknown>(
  ...parts: Array<string | number>
): Promise<T | null> {
  const encoded = parts
    .map((part) => encodeURIComponent(String(part)))
    .join("/");
  const res = await fetch(`${REDIS_URL}/${encoded}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${REDIS_TOKEN}`,
    },
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Redis command failed: ${parts[0]} ${res.status} ${text}`);
  }

  const data = (await res.json()) as { result?: T };
  return (data.result ?? null) as T | null;
}

async function getBalance(userId: string): Promise<number> {
  const result = await redisCommand<string>("GET", walletKey(userId));
  return safeParseNumber(result, 0);
}

async function setBalance(userId: string, balance: number): Promise<void> {
  await redisCommand("SET", walletKey(userId), balance);
}

async function pushLedgerEntry(
  userId: string,
  entry: LedgerEntry,
): Promise<void> {
  await redisCommand("RPUSH", ledgerKey(userId), JSON.stringify(entry));
  await redisCommand("LTRIM", ledgerKey(userId), -1000, -1);
}

async function getLedgerPage(userId: string, cursor = 0, limit = 25) {
  const total = safeParseNumber(
    await redisCommand<number>("LLEN", ledgerKey(userId)),
    0,
  );

  if (total === 0) {
    return {
      entries: [] as LedgerEntry[],
      nextCursor: null as string | null,
      hasMore: false,
      total,
    };
  }

  const offset = Math.max(0, cursor);
  const start = Math.max(total - offset - limit, 0);
  const end = total - offset - 1;

  if (end < 0 || start > end) {
    return {
      entries: [] as LedgerEntry[],
      nextCursor: null as string | null,
      hasMore: false,
      total,
    };
  }

  const raw =
    (await redisCommand<string[]>("LRANGE", ledgerKey(userId), start, end)) ||
    [];

  const entries = raw
    .map((item) => {
      try {
        return JSON.parse(item) as LedgerEntry;
      } catch {
        return null;
      }
    })
    .filter(Boolean) as LedgerEntry[];

  entries.reverse();

  const nextOffset = offset + entries.length;
  const hasMore = start > 0;

  return {
    entries,
    nextCursor: hasMore ? String(nextOffset) : null,
    hasMore,
    total,
  };
}

async function withUserLock<T>(
  userId: string,
  fn: () => Promise<T>,
): Promise<T> {
  const key = lockKey(userId);
  const token = randomUUID();
  const timeoutMs = 5000;
  const started = Date.now();

  while (Date.now() - started < timeoutMs) {
    const acquired = await redisCommand<string>(
      "SET",
      key,
      token,
      "NX",
      "EX",
      5,
    );

    if (acquired === "OK") {
      try {
        return await fn();
      } finally {
        const owner = await redisCommand<string>("GET", key);
        if (owner === token) {
          await redisCommand("DEL", key);
        }
      }
    }

    await sleep(100);
  }

  throw new Error("Wallet is busy. Try again in a second.");
}

async function applyCreditChange(args: ApplyCreditChangeArgs) {
  const {
    userId,
    amount,
    entryType,
    referenceType,
    referenceId,
    idempotencyKey,
    description,
  } = args;

  if (!Number.isInteger(amount) || amount === 0) {
    throw new Error("Amount must be a non-zero integer.");
  }

  return withUserLock(userId, async () => {
    if (idempotencyKey) {
      const existing = await redisCommand<string>(
        "GET",
        idempotencyKeyFor(userId, idempotencyKey),
      );

      if (existing) {
        const entry = JSON.parse(existing) as LedgerEntry;
        return {
          duplicated: true,
          balance: entry.balanceAfter,
          entry,
        };
      }
    }

    const currentBalance = await getBalance(userId);
    const nextBalance = currentBalance + amount;

    if (nextBalance < 0) {
      throw new Error(
        `Insufficient credits. Current balance: ${currentBalance}, requested: ${Math.abs(
          amount,
        )}.`,
      );
    }

    const entry: LedgerEntry = {
      id: randomUUID(),
      userId,
      entryType,
      amount,
      balanceAfter: nextBalance,
      referenceType,
      referenceId,
      idempotencyKey,
      description,
      createdAt: new Date().toISOString(),
    };

    await setBalance(userId, nextBalance);
    await pushLedgerEntry(userId, entry);

    if (idempotencyKey) {
      await redisCommand(
        "SET",
        idempotencyKeyFor(userId, idempotencyKey),
        JSON.stringify(entry),
        "EX",
        60 * 60 * 24 * 30,
      );
    }

    return {
      duplicated: false,
      balance: nextBalance,
      entry,
    };
  });
}

// ─────────────────────────────────────────────
// MCP SERVER
// ─────────────────────────────────────────────

function createCreditsServer(userId: string): McpServer {
  const server = new McpServer({
    name: "credits-mcp-server",
    version: "1.0.0",
  });

  server.registerTool(
    "credits_get_balance",
    {
      title: "Get Credit Balance",
      description: "Return the user's current live credit balance.",
      inputSchema: {},
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: false,
      },
    },
    async () => {
      const balance = await getBalance(userId);
      return jsonText({
        userId,
        balance,
      });
    },
  );

  server.registerTool(
    "credits_get_packs",
    {
      title: "Get Credit Packs",
      description: "List all available credit packs.",
      inputSchema: {},
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: false,
      },
    },
    async () => {
      return jsonText({
        packs: CREDIT_PACKS,
      });
    },
  );

  server.registerTool(
    "credits_get_ledger",
    {
      title: "Get Credit Ledger",
      description: "Return recent ledger entries for this user.",
      inputSchema: {
        cursor: z.string().default("0").describe("Pagination cursor"),
        limit: z
          .number()
          .int()
          .min(1)
          .max(100)
          .default(25)
          .describe("Entries per page"),
      },
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: false,
      },
    },
    async ({ cursor, limit }: { cursor: string; limit: number }) => {
      const parsedCursor = safeParseNumber(cursor, 0);
      const ledger = await getLedgerPage(userId, parsedCursor, limit);
      const balance = await getBalance(userId);

      return jsonText({
        userId,
        balance,
        ...ledger,
      });
    },
  );

  server.registerTool(
    "credits_purchase_pack",
    {
      title: "Purchase Credit Pack",
      description: "Add credits to the wallet from a pack purchase.",
      inputSchema: {
        pack_code: z
          .enum(["basic", "standard", "plus", "ultimate"])
          .describe("Pack code"),
        reference_id: z
          .string()
          .min(1)
          .optional()
          .describe("Shopify order ID or any purchase reference"),
        idempotency_key: z
          .string()
          .min(1)
          .optional()
          .describe("Use this to prevent duplicate credit adds"),
      },
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: false,
      },
    },
    async ({
      pack_code,
      reference_id,
      idempotency_key,
    }: {
      pack_code: PackCode;
      reference_id?: string;
      idempotency_key?: string;
    }) => {
      const pack = PACK_BY_CODE.get(pack_code);

      if (!pack) {
        throw new Error(`Unknown pack code: ${pack_code}`);
      }

      const result = await applyCreditChange({
        userId,
        amount: pack.credits,
        entryType: "purchase",
        referenceType: "pack",
        referenceId: reference_id || `pack:${pack.code}:${Date.now()}`,
        idempotencyKey: idempotency_key,
        description: `Purchased ${pack.name}`,
      });

      return jsonText({
        userId,
        pack,
        ...result,
      });
    },
  );

  server.registerTool(
    "credits_add_credits",
    {
      title: "Add Credits",
      description:
        "Add credits manually for bonus, support, or admin adjustments.",
      inputSchema: {
        credits: z.number().int().min(1).describe("Credits to add"),
        reference_id: z.string().min(1).describe("Reference ID"),
        idempotency_key: z
          .string()
          .min(1)
          .optional()
          .describe("Use this to prevent duplicate adds"),
        description: z
          .string()
          .min(1)
          .optional()
          .describe("Short reason for this credit add"),
      },
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: false,
      },
    },
    async ({
      credits,
      reference_id,
      idempotency_key,
      description,
    }: {
      credits: number;
      reference_id: string;
      idempotency_key?: string;
      description?: string;
    }) => {
      const result = await applyCreditChange({
        userId,
        amount: credits,
        entryType: "adjustment",
        referenceType: "manual",
        referenceId: reference_id,
        idempotencyKey: idempotency_key,
        description: description || "Manual credit add",
      });

      return jsonText({
        userId,
        ...result,
      });
    },
  );

  server.registerTool(
    "credits_deduct_credits",
    {
      title: "Deduct Credits",
      description:
        "Deduct credits when a user triggers generation or another paid action.",
      inputSchema: {
        credits: z.number().int().min(1).describe("Credits to deduct"),
        reference_id: z.string().min(1).describe("Generation job or action ID"),
        idempotency_key: z
          .string()
          .min(1)
          .optional()
          .describe("Use this to prevent duplicate charges"),
        description: z
          .string()
          .min(1)
          .optional()
          .describe("Short reason for the deduction"),
      },
      annotations: {
        readOnlyHint: false,
        destructiveHint: true,
        idempotentHint: true,
        openWorldHint: false,
      },
    },
    async ({
      credits,
      reference_id,
      idempotency_key,
      description,
    }: {
      credits: number;
      reference_id: string;
      idempotency_key?: string;
      description?: string;
    }) => {
      const result = await applyCreditChange({
        userId,
        amount: -credits,
        entryType: "usage",
        referenceType: "generation",
        referenceId: reference_id,
        idempotencyKey: idempotency_key,
        description: description || "Credit deduction",
      });

      return jsonText({
        userId,
        ...result,
      });
    },
  );

  server.registerTool(
    "credits_refund_credits",
    {
      title: "Refund Credits",
      description: "Refund credits instantly if something fails on your side.",
      inputSchema: {
        credits: z.number().int().min(1).describe("Credits to refund"),
        reference_id: z
          .string()
          .min(1)
          .describe("Failed job or refund reference"),
        idempotency_key: z
          .string()
          .min(1)
          .optional()
          .describe("Use this to prevent duplicate refunds"),
        description: z
          .string()
          .min(1)
          .optional()
          .describe("Short reason for the refund"),
      },
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: false,
      },
    },
    async ({
      credits,
      reference_id,
      idempotency_key,
      description,
    }: {
      credits: number;
      reference_id: string;
      idempotency_key?: string;
      description?: string;
    }) => {
      const result = await applyCreditChange({
        userId,
        amount: credits,
        entryType: "refund",
        referenceType: "refund",
        referenceId: reference_id,
        idempotencyKey: idempotency_key,
        description: description || "Instant credit refund",
      });

      return jsonText({
        userId,
        ...result,
      });
    },
  );

  return server;
}

// ─────────────────────────────────────────────
// USER RESOLUTION
// ─────────────────────────────────────────────

function resolveUserIdFromSharedRoute(req: Request): string | null {
  const userId = req.header("x-user-id");
  if (!userId) return null;
  return normalizeUserId(userId);
}

// ─────────────────────────────────────────────
// EXPRESS / MCP TRANSPORT
// ─────────────────────────────────────────────

const app = express();
app.use(express.json({ limit: "1mb" }));

app.get("/favicon.ico", (_req, res) => res.status(204).end());

app.get("/health", (_req, res) => {
  res.json({
    status: "ok",
    service: "credits-mcp-server",
    routes: ["/mcp", "/u/:userId"],
    auth: "disabled",
  });
});

async function handleMcpRequest(req: Request, res: Response, userId: string) {
  try {
    const server = createCreditsServer(userId);
    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: undefined,
      enableJsonResponse: true,
    });

    res.on("close", () => transport.close());

    await server.connect(transport);
    await transport.handleRequest(req, res, req.body);
  } catch (error) {
    console.error(`MCP error for ${userId}:`, error);
    if (!res.headersSent) {
      res.status(500).json({
        error: error instanceof Error ? error.message : "Internal server error",
      });
    }
  }
}

app.post("/mcp", async (req, res) => {
  const userId = resolveUserIdFromSharedRoute(req);

  if (!userId || userId.length < 2) {
    res.status(400).json({
      error: "Missing or invalid x-user-id header",
    });
    return;
  }

  await handleMcpRequest(req, res, userId);
});

app.post("/u/:userId", async (req, res) => {
  const userId = normalizeUserId(req.params.userId || "");

  if (!userId || userId.length < 2) {
    res.status(400).json({ error: "Invalid user ID" });
    return;
  }

  await handleMcpRequest(req, res, userId);
});

app.listen(PORT, () => {
  console.log(`Credits MCP running on :${PORT}`);
  console.log(`Routes: POST /mcp and POST /u/:userId`);
  console.log("Auth: disabled");
});
