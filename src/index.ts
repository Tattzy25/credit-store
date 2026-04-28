import { randomUUID } from "crypto";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import express, { Request, Response } from "express";
import { z } from "zod";

// ---------------------------------------------------------------------------
// CONFIG
// ---------------------------------------------------------------------------

const REDIS_URL = process.env.UPSTASH_REDIS_REST_URL!;
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN!;
const PORT = Number(process.env.PORT) || 3000;

type BillingUnit = "credit" | "minute";
type FeatureCode =
  | "image_generate"
  | "image_edit"
  | "photo_booth"
  | "model_train"
  | "live_minute";

type PackCode = "basic" | "standard" | "plus" | "ultimate";

type LedgerEntryType =
  | "purchase"
  | "signup_grant"
  | "monthly_grant"
  | "usage"
  | "auto_refund"
  | "adjustment";

type LedgerReferenceType =
  | "pack_purchase"
  | "signup_bonus"
  | "monthly_bonus"
  | "feature_usage"
  | "auto_refund"
  | "admin"
  | "system";

type UsageStatus = "pending" | "completed" | "failed" | "refunded";
type LiveSessionStatus = "active" | "ended" | "failed";

interface FeatureConfig {
  code: FeatureCode;
  name: string;
  billingUnit: BillingUnit;
  creditsPerUnit: number;
  active: boolean;
  description?: string;
}

interface CreditPack {
  code: PackCode;
  name: string;
  priceCents: number;
  credits: number;
  description?: string;
  shopifyProductId?: string;
  shopifyVariantId?: string;
  active: boolean;
}

interface Wallet {
  userId: string;
  balance: number;
  updatedAt: string;
}

interface CreditLedgerEntry {
  id: string;
  userId: string;
  entryType: LedgerEntryType;
  amount: number;
  balanceAfter: number;
  referenceType: LedgerReferenceType;
  referenceId: string;
  idempotencyKey?: string;
  description?: string;
  createdAt: string;
}

interface UsageEvent {
  id: string;
  userId: string;
  featureCode: FeatureCode;
  billingUnit: BillingUnit;
  quantity: number;
  creditsCharged: number;
  referenceId: string;
  idempotencyKey?: string;
  sessionId?: string;
  platform?: string;
  domain?: string;
  status: UsageStatus;
  errorMessage?: string;
  ledgerEntryId?: string;
  autoRefundLedgerEntryId?: string;
  createdAt: string;
  completedAt?: string;
}

interface LiveSession {
  id: string;
  userId: string;
  featureCode: "live_minute";
  startedAt: string;
  endedAt?: string;
  status: LiveSessionStatus;
  totalMinutesCharged: number;
  totalCreditsCharged: number;
  lastMinuteChargedAt?: string;
  platform?: string;
  domain?: string;
}

interface GrantMarker {
  userId: string;
  grantType: "signup_grant" | "monthly_grant";
  period: string;
  ledgerEntryId: string;
  createdAt: string;
}

const SIGNUP_FREE_CREDITS = 25;
const MONTHLY_FREE_CREDITS = 25;
const MAX_LEDGER_ITEMS = 5000;
const MAX_USAGE_ITEMS = 5000;
const MAX_LIVE_SESSION_ITEMS = 500;
const IDEM_TTL_SECONDS = 60 * 60 * 24 * 90;

const FEATURE_CONFIGS: Record<FeatureCode, FeatureConfig> = {
  image_generate: {
    code: "image_generate",
    name: "Image Generate",
    billingUnit: "credit",
    creditsPerUnit: 1,
    active: true,
    description: "Single image generation",
  },
  image_edit: {
    code: "image_edit",
    name: "Image Edit",
    billingUnit: "credit",
    creditsPerUnit: 1,
    active: true,
    description: "Single image edit",
  },
  photo_booth: {
    code: "photo_booth",
    name: "Photo Booth",
    billingUnit: "credit",
    creditsPerUnit: 4,
    active: true,
    description: "Photo booth session / batch",
  },
  model_train: {
    code: "model_train",
    name: "Model Train",
    billingUnit: "credit",
    creditsPerUnit: 100,
    active: true,
    description: "Model training",
  },
  live_minute: {
    code: "live_minute",
    name: "Live Minute",
    billingUnit: "minute",
    creditsPerUnit: 20,
    active: true,
    description: "FaceTime / live session per minute",
  },
};

const CREDIT_PACKS: CreditPack[] = [
  {
    code: "basic",
    name: "Basic Pack",
    priceCents: 599,
    credits: 20,
    description: 'The "Quick Hit" for curious users.',
    active: true,
  },
  {
    code: "standard",
    name: "Standard Pack",
    priceCents: 999,
    credits: 40,
    description: 'The "Mid-Tier" casualty.',
    active: true,
  },
  {
    code: "plus",
    name: "Plus Pack",
    priceCents: 1999,
    credits: 115,
    description: "Forced choice between Training or FaceTime.",
    active: true,
  },
  {
    code: "ultimate",
    name: "Ultimate Pack",
    priceCents: 9999,
    credits: 750,
    description: "The whale pack.",
    active: true,
  },
];

const PACK_BY_CODE = new Map<PackCode, CreditPack>(
  CREDIT_PACKS.map((pack) => [pack.code, pack]),
);

const walletKey = (userId: string) => `credits:${userId}:wallet`;
const ledgerIndexKey = (userId: string) => `credits:${userId}:ledger:index`;
const ledgerItemKey = (userId: string, id: string) =>
  `credits:${userId}:ledger:item:${id}`;
const usageIndexKey = (userId: string) => `credits:${userId}:usage:index`;
const usageItemKey = (userId: string, id: string) =>
  `credits:${userId}:usage:item:${id}`;
const liveSessionIndexKey = (userId: string) => `credits:${userId}:live:index`;
const liveSessionKey = (userId: string, id: string) =>
  `credits:${userId}:live:item:${id}`;
const signupGrantKey = (userId: string) => `credits:${userId}:grant:signup`;
const monthlyGrantKey = (userId: string, month: string) =>
  `credits:${userId}:grant:monthly:${month}`;
const idempotencyKeyFor = (userId: string, key: string) =>
  `credits:${userId}:idem:${key}`;
const lockKey = (userId: string) => `credits:${userId}:lock`;

function nowIso() {
  return new Date().toISOString();
}

function normalizeUserId(raw: string): string {
  return raw.trim().toLowerCase();
}
function requireFiniteNumber(value: unknown, label: string): number {
  if (typeof value === "number" && Number.isFinite(value)) return value;

  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }

  throw new Error(`Expected finite number for ${label}, got: ${String(value)}`);
}

function requirePositiveInt(value: unknown, label: string): number {
  const parsed = requireFiniteNumber(value, label);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(
      `Expected non-negative integer for ${label}, got: ${String(value)}`,
    );
  }
  return parsed;
}

function requireFeature(code: FeatureCode): FeatureConfig {
  const feature = FEATURE_CONFIGS[code];
  if (!feature || !feature.active) {
    throw new Error(`Unknown or inactive feature: ${code}`);
  }
  return feature;
}
function currentMonthKey(date: Date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    throw new Error("currentMonthKey requires a valid Date");
  }
  return date.toISOString().slice(0, 7);
}
// ---------------------------------------------------------------------------
// REDIS HELPERS
// ---------------------------------------------------------------------------

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

  const rawText = await res.text();

  if (!res.ok) {
    throw new Error(`Redis command failed: ${parts[0]} ${res.status} ${rawText}`);
  }

  let data: unknown;

  try {
    data = JSON.parse(rawText);
  } catch {
    throw new Error(`Redis returned non-JSON for ${parts[0]}: ${rawText.slice(0, 300)}`);
  }

  if (!data || typeof data !== "object" || !("result" in data)) {
    throw new Error(`Redis response missing result for ${parts[0]}: ${rawText.slice(0, 300)}`);
  }

  return (data as { result: T | null }).result;
}

async function getString(key: string): Promise<string | null> {
  const result = await redisCommand<string>("GET", key);
  if (typeof result !== "string") return null;
  return result;
}

async function setString(key: string, value: string, ttlSeconds?: number) {
  if (ttlSeconds && ttlSeconds > 0) {
    await redisCommand("SET", key, value, "EX", ttlSeconds);
    return;
  }
  await redisCommand("SET", key, value);
}

async function getJson<T>(key: string): Promise<T | null> {
  const raw = await getString(key);
  if (raw === null) return null;
  try {
    return JSON.parse(raw) as T;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`Invalid JSON at Redis key ${key}: ${message}; raw=${raw.slice(0, 300)}`);
  }
}

async function setJson(key: string, value: unknown, ttlSeconds?: number) {
  await setString(key, JSON.stringify(value), ttlSeconds);
}

async function deleteKey(key: string) {
  await redisCommand("DEL", key);
}

async function pushIndex(indexKey: string, id: string, maxItems: number) {
  await redisCommand("LPUSH", indexKey, id);
  await redisCommand("LTRIM", indexKey, 0, maxItems - 1);
}

async function appendIndexedJson(
  indexKey: string,
  itemKey: string,
  value: unknown,
  maxItems: number,
) {
  await setJson(itemKey, value);
  await pushIndex(indexKey, itemKey, maxItems);
}

async function getIndexedJsonPage<T>(
  indexKey: string,
  cursor = 0,
  limit = 25,
): Promise<{
  items: T[];
  nextCursor: string | null;
  hasMore: boolean;
  total: number;
}> {
  const totalRaw = await redisCommand<unknown>("LLEN", indexKey);
  const total = requirePositiveInt(totalRaw ?? 0, `LLEN ${indexKey}`);

  if (total === 0) {
    return {
      items: [],
      nextCursor: null,
      hasMore: false,
      total,
    };
  }

  const offset = Math.max(0, cursor);
  const end = Math.min(offset + limit - 1, total - 1);

  if (offset > end) {
    return {
      items: [],
      nextCursor: null,
      hasMore: false,
      total,
    };
  }

  const itemKeysRaw = await redisCommand<unknown[]>("LRANGE", indexKey, offset, end);

  if (!Array.isArray(itemKeysRaw)) {
    throw new Error(`LRANGE ${indexKey} did not return an array`);
  }

  const itemKeys = itemKeysRaw.map((value, index) => {
    if (typeof value !== "string" || value.length === 0) {
      throw new Error(`Invalid item key at ${indexKey}[${offset + index}]`);
    }
    return value;
  });

  const rawItems = await Promise.all(itemKeys.map((key) => getJson<T>(key)));
  const items = rawItems.filter((v): v is T => v !== null);

  const nextOffset = offset + items.length;
  const hasMore = nextOffset < total;

  return {
    items,
    nextCursor: hasMore ? String(nextOffset) : null,
    hasMore,
    total,
  };
}

// ---------------------------------------------------------------------------
// MCP SERVER FACTORY
// ---------------------------------------------------------------------------

function createCreditStoreServer(userId: string): McpServer {
  const server = new McpServer({
    name: "credit-store",
    version: "1.0.0",
  });

  // --- Wallet ---

  server.tool(
    "get_balance",
    "Get the current credit balance for the user",
    {},
    async () => {
      const wallet = await getJson<Wallet>(walletKey(userId));
      if (!wallet) {
        return jsonText({ userId, balance: 0, updatedAt: null });
      }
      return jsonText(wallet);
    },
  );

  server.tool(
    "get_wallet",
    "Get full wallet details for the user",
    {},
    async () => {
      const wallet = await getJson<Wallet>(walletKey(userId));
      return jsonText(wallet ?? { userId, balance: 0, updatedAt: null });
    },
  );

  // --- Ledger ---

  server.tool(
    "get_ledger",
    "Get paginated credit ledger entries for the user",
    {
      cursor: z.string().optional().describe("Pagination cursor (offset)"),
      limit: z.number().int().min(1).max(100).optional().describe("Items per page (default 25)"),
    },
    async ({ cursor, limit }) => {
      const page = await getIndexedJsonPage<CreditLedgerEntry>(
        ledgerIndexKey(userId),
        cursor ? Number(cursor) : 0,
        limit ?? 25,
      );
      return jsonText(page);
    },
  );

  // --- Usage ---

  server.tool(
    "get_usage",
    "Get paginated usage events for the user",
    {
      cursor: z.string().optional().describe("Pagination cursor (offset)"),
      limit: z.number().int().min(1).max(100).optional().describe("Items per page (default 25)"),
    },
    async ({ cursor, limit }) => {
      const page = await getIndexedJsonPage<UsageEvent>(
        usageIndexKey(userId),
        cursor ? Number(cursor) : 0,
        limit ?? 25,
      );
      return jsonText(page);
    },
  );

  server.tool(
    "record_usage",
    "Record a feature usage event and deduct credits from the user's wallet",
    {
      featureCode: z.enum(["image_generate", "image_edit", "photo_booth", "model_train", "live_minute"]).describe("Feature being used"),
      quantity: z.number().int().min(1).describe("Number of units consumed"),
      referenceId: z.string().min(1).describe("External reference ID (e.g. job ID)"),
      idempotencyKey: z.string().optional().describe("Idempotency key to prevent duplicate charges"),
      sessionId: z.string().optional().describe("Session ID if applicable"),
      platform: z.string().optional().describe("Platform identifier"),
      domain: z.string().optional().describe("Domain identifier"),
    },
    async ({ featureCode, quantity, referenceId, idempotencyKey, sessionId, platform, domain }) => {
      const feature = requireFeature(featureCode as FeatureCode);
      const creditsCharged = feature.creditsPerUnit * quantity;

      // Idempotency check
      if (idempotencyKey) {
        const idemKey = idempotencyKeyFor(userId, idempotencyKey);
        const existing = await getString(idemKey);
        if (existing !== null) {
          return jsonText({ duplicate: true, idempotencyKey });
        }
      }

      // Acquire lock
      const lock = lockKey(userId);
      const lockAcquired = await redisCommand("SET", lock, "1", "NX", "EX", 10);
      if (!lockAcquired) {
        throw new Error("Could not acquire wallet lock — try again");
      }

      try {
        const wallet = await getJson<Wallet>(walletKey(userId));
        const currentBalance = wallet?.balance ?? 0;

        if (currentBalance < creditsCharged) {
          throw new Error(`Insufficient credits: need ${creditsCharged}, have ${currentBalance}`);
        }

        const newBalance = currentBalance - creditsCharged;
        const now = nowIso();
        const entryId = randomUUID();

        const ledgerEntry: CreditLedgerEntry = {
          id: entryId,
          userId,
          entryType: "usage",
          amount: -creditsCharged,
          balanceAfter: newBalance,
          referenceType: "feature_usage",
          referenceId,
          idempotencyKey,
          description: `${feature.name} x${quantity}`,
          createdAt: now,
        };

        const usageId = randomUUID();
        const usageEvent: UsageEvent = {
          id: usageId,
          userId,
          featureCode: featureCode as FeatureCode,
          billingUnit: feature.billingUnit,
          quantity,
          creditsCharged,
          referenceId,
          idempotencyKey,
          sessionId,
          platform,
          domain,
          status: "completed",
          ledgerEntryId: entryId,
          createdAt: now,
          completedAt: now,
        };

        const updatedWallet: Wallet = { userId, balance: newBalance, updatedAt: now };

        await setJson(walletKey(userId), updatedWallet);
        await appendIndexedJson(ledgerIndexKey(userId), ledgerItemKey(userId, entryId), ledgerEntry, MAX_LEDGER_ITEMS);
        await appendIndexedJson(usageIndexKey(userId), usageItemKey(userId, usageId), usageEvent, MAX_USAGE_ITEMS);

        if (idempotencyKey) {
          await setString(idempotencyKeyFor(userId, idempotencyKey), entryId, IDEM_TTL_SECONDS);
        }

        return jsonText({ success: true, creditsCharged, newBalance, ledgerEntryId: entryId, usageEventId: usageId });
      } finally {
        await deleteKey(lock);
      }
    },
  );

  server.tool(
    "refund_usage",
    "Refund credits for a previously recorded usage event",
    {
      usageEventId: z.string().min(1).describe("ID of the usage event to refund"),
    },
    async ({ usageEventId }) => {
      const lock = lockKey(userId);
      const lockAcquired = await redisCommand("SET", lock, "1", "NX", "EX", 10);
      if (!lockAcquired) {
        throw new Error("Could not acquire wallet lock — try again");
      }

      try {
        const usageEvent = await getJson<UsageEvent>(usageItemKey(userId, usageEventId));
        if (!usageEvent) {
          throw new Error(`Usage event not found: ${usageEventId}`);
        }
        if (usageEvent.status === "refunded") {
          return jsonText({ duplicate: true, message: "Already refunded" });
        }

        const wallet = await getJson<Wallet>(walletKey(userId));
        const currentBalance = wallet?.balance ?? 0;
        const newBalance = currentBalance + usageEvent.creditsCharged;
        const now = nowIso();
        const refundEntryId = randomUUID();

        const refundEntry: CreditLedgerEntry = {
          id: refundEntryId,
          userId,
          entryType: "auto_refund",
          amount: usageEvent.creditsCharged,
          balanceAfter: newBalance,
          referenceType: "auto_refund",
          referenceId: usageEventId,
          description: `Refund for usage ${usageEventId}`,
          createdAt: now,
        };

        const updatedUsage: UsageEvent = { ...usageEvent, status: "refunded", autoRefundLedgerEntryId: refundEntryId };
        const updatedWallet: Wallet = { userId, balance: newBalance, updatedAt: now };

        await setJson(walletKey(userId), updatedWallet);
        await setJson(usageItemKey(userId, usageEventId), updatedUsage);
        await appendIndexedJson(ledgerIndexKey(userId), ledgerItemKey(userId, refundEntryId), refundEntry, MAX_LEDGER_ITEMS);

        return jsonText({ success: true, creditsRefunded: usageEvent.creditsCharged, newBalance, refundLedgerEntryId: refundEntryId });
      } finally {
        await deleteKey(lock);
      }
    },
  );

  // --- Credit packs ---

  server.tool(
    "list_credit_packs",
    "List all available credit packs",
    {},
    async () => {
      return jsonText(CREDIT_PACKS.filter((p) => p.active));
    },
  );

  server.tool(
    "purchase_credits",
    "Record a credit pack purchase and add credits to the user's wallet",
    {
      packCode: z.enum(["basic", "standard", "plus", "ultimate"]).describe("Credit pack to purchase"),
      referenceId: z.string().min(1).describe("External reference ID (e.g. Shopify order ID)"),
      idempotencyKey: z.string().optional().describe("Idempotency key to prevent duplicate purchases"),
    },
    async ({ packCode, referenceId, idempotencyKey }) => {
      const pack = PACK_BY_CODE.get(packCode as PackCode);
      if (!pack || !pack.active) {
        throw new Error(`Unknown or inactive pack: ${packCode}`);
      }

      if (idempotencyKey) {
        const idemKey = idempotencyKeyFor(userId, idempotencyKey);
        const existing = await getString(idemKey);
        if (existing !== null) {
          return jsonText({ duplicate: true, idempotencyKey });
        }
      }

      const lock = lockKey(userId);
      const lockAcquired = await redisCommand("SET", lock, "1", "NX", "EX", 10);
      if (!lockAcquired) {
        throw new Error("Could not acquire wallet lock — try again");
      }

      try {
        const wallet = await getJson<Wallet>(walletKey(userId));
        const currentBalance = wallet?.balance ?? 0;
        const newBalance = currentBalance + pack.credits;
        const now = nowIso();
        const entryId = randomUUID();

        const ledgerEntry: CreditLedgerEntry = {
          id: entryId,
          userId,
          entryType: "purchase",
          amount: pack.credits,
          balanceAfter: newBalance,
          referenceType: "pack_purchase",
          referenceId,
          idempotencyKey,
          description: `Purchased ${pack.name} (${pack.credits} credits)`,
          createdAt: now,
        };

        const updatedWallet: Wallet = { userId, balance: newBalance, updatedAt: now };

        await setJson(walletKey(userId), updatedWallet);
        await appendIndexedJson(ledgerIndexKey(userId), ledgerItemKey(userId, entryId), ledgerEntry, MAX_LEDGER_ITEMS);

        if (idempotencyKey) {
          await setString(idempotencyKeyFor(userId, idempotencyKey), entryId, IDEM_TTL_SECONDS);
        }

        return jsonText({ success: true, creditsAdded: pack.credits, newBalance, ledgerEntryId: entryId });
      } finally {
        await deleteKey(lock);
      }
    },
  );

  // --- Grants ---

  server.tool(
    "grant_signup_credits",
    "Grant signup bonus credits to a new user (idempotent)",
    {},
    async () => {
      const existing = await getString(signupGrantKey(userId));
      if (existing !== null) {
        return jsonText({ duplicate: true, message: "Signup grant already issued" });
      }

      const lock = lockKey(userId);
      const lockAcquired = await redisCommand("SET", lock, "1", "NX", "EX", 10);
      if (!lockAcquired) {
        throw new Error("Could not acquire wallet lock — try again");
      }

      try {
        const wallet = await getJson<Wallet>(walletKey(userId));
        const currentBalance = wallet?.balance ?? 0;
        const newBalance = currentBalance + SIGNUP_FREE_CREDITS;
        const now = nowIso();
        const entryId = randomUUID();

        const ledgerEntry: CreditLedgerEntry = {
          id: entryId,
          userId,
          entryType: "signup_grant",
          amount: SIGNUP_FREE_CREDITS,
          balanceAfter: newBalance,
          referenceType: "signup_bonus",
          referenceId: userId,
          description: `Signup bonus: ${SIGNUP_FREE_CREDITS} credits`,
          createdAt: now,
        };

        const grantMarker: GrantMarker = {
          userId,
          grantType: "signup_grant",
          period: "once",
          ledgerEntryId: entryId,
          createdAt: now,
        };

        const updatedWallet: Wallet = { userId, balance: newBalance, updatedAt: now };

        await setJson(walletKey(userId), updatedWallet);
        await appendIndexedJson(ledgerIndexKey(userId), ledgerItemKey(userId, entryId), ledgerEntry, MAX_LEDGER_ITEMS);
        await setJson(signupGrantKey(userId), grantMarker);

        return jsonText({ success: true, creditsGranted: SIGNUP_FREE_CREDITS, newBalance, ledgerEntryId: entryId });
      } finally {
        await deleteKey(lock);
      }
    },
  );

  server.tool(
    "grant_monthly_credits",
    "Grant monthly bonus credits to a user (idempotent per calendar month)",
    {
      month: z.string().optional().describe("Month in YYYY-MM format (defaults to current month)"),
    },
    async ({ month }) => {
      const monthKey = month ?? currentMonthKey(new Date());
      const existing = await getString(monthlyGrantKey(userId, monthKey));
      if (existing !== null) {
        return jsonText({ duplicate: true, message: `Monthly grant already issued for ${monthKey}` });
      }

      const lock = lockKey(userId);
      const lockAcquired = await redisCommand("SET", lock, "1", "NX", "EX", 10);
      if (!lockAcquired) {
        throw new Error("Could not acquire wallet lock — try again");
      }

      try {
        const wallet = await getJson<Wallet>(walletKey(userId));
        const currentBalance = wallet?.balance ?? 0;
        const newBalance = currentBalance + MONTHLY_FREE_CREDITS;
        const now = nowIso();
        const entryId = randomUUID();

        const ledgerEntry: CreditLedgerEntry = {
          id: entryId,
          userId,
          entryType: "monthly_grant",
          amount: MONTHLY_FREE_CREDITS,
          balanceAfter: newBalance,
          referenceType: "monthly_bonus",
          referenceId: monthKey,
          description: `Monthly bonus: ${MONTHLY_FREE_CREDITS} credits for ${monthKey}`,
          createdAt: now,
        };

        const grantMarker: GrantMarker = {
          userId,
          grantType: "monthly_grant",
          period: monthKey,
          ledgerEntryId: entryId,
          createdAt: now,
        };

        const updatedWallet: Wallet = { userId, balance: newBalance, updatedAt: now };

        await setJson(walletKey(userId), updatedWallet);
        await appendIndexedJson(ledgerIndexKey(userId), ledgerItemKey(userId, entryId), ledgerEntry, MAX_LEDGER_ITEMS);
        await setJson(monthlyGrantKey(userId, monthKey), grantMarker);

        return jsonText({ success: true, creditsGranted: MONTHLY_FREE_CREDITS, newBalance, ledgerEntryId: entryId, month: monthKey });
      } finally {
        await deleteKey(lock);
      }
    },
  );

  // --- Live sessions ---

  server.tool(
    "start_live_session",
    "Start a live session for the user",
    {
      platform: z.string().optional().describe("Platform identifier"),
      domain: z.string().optional().describe("Domain identifier"),
    },
    async ({ platform, domain }) => {
      const sessionId = randomUUID();
      const now = nowIso();

      const session: LiveSession = {
        id: sessionId,
        userId,
        featureCode: "live_minute",
        startedAt: now,
        status: "active",
        totalMinutesCharged: 0,
        totalCreditsCharged: 0,
        platform,
        domain,
      };

      await appendIndexedJson(liveSessionIndexKey(userId), liveSessionKey(userId, sessionId), session, MAX_LIVE_SESSION_ITEMS);

      return jsonText({ success: true, sessionId, startedAt: now });
    },
  );

  server.tool(
    "charge_live_minute",
    "Charge one minute of live session credits",
    {
      sessionId: z.string().min(1).describe("Live session ID"),
    },
    async ({ sessionId }) => {
      const feature = requireFeature("live_minute");
      const creditsPerMinute = feature.creditsPerUnit;

      const lock = lockKey(userId);
      const lockAcquired = await redisCommand("SET", lock, "1", "NX", "EX", 10);
      if (!lockAcquired) {
        throw new Error("Could not acquire wallet lock — try again");
      }

      try {
        const session = await getJson<LiveSession>(liveSessionKey(userId, sessionId));
        if (!session) {
          throw new Error(`Live session not found: ${sessionId}`);
        }
        if (session.status !== "active") {
          throw new Error(`Live session is not active: ${session.status}`);
        }

        const wallet = await getJson<Wallet>(walletKey(userId));
        const currentBalance = wallet?.balance ?? 0;

        if (currentBalance < creditsPerMinute) {
          throw new Error(`Insufficient credits: need ${creditsPerMinute}, have ${currentBalance}`);
        }

        const newBalance = currentBalance - creditsPerMinute;
        const now = nowIso();
        const entryId = randomUUID();

        const ledgerEntry: CreditLedgerEntry = {
          id: entryId,
          userId,
          entryType: "usage",
          amount: -creditsPerMinute,
          balanceAfter: newBalance,
          referenceType: "feature_usage",
          referenceId: sessionId,
          description: `Live minute charge for session ${sessionId}`,
          createdAt: now,
        };

        const updatedSession: LiveSession = {
          ...session,
          totalMinutesCharged: session.totalMinutesCharged + 1,
          totalCreditsCharged: session.totalCreditsCharged + creditsPerMinute,
          lastMinuteChargedAt: now,
        };

        const updatedWallet: Wallet = { userId, balance: newBalance, updatedAt: now };

        await setJson(walletKey(userId), updatedWallet);
        await setJson(liveSessionKey(userId, sessionId), updatedSession);
        await appendIndexedJson(ledgerIndexKey(userId), ledgerItemKey(userId, entryId), ledgerEntry, MAX_LEDGER_ITEMS);

        return jsonText({ success: true, creditsCharged: creditsPerMinute, newBalance, totalMinutesCharged: updatedSession.totalMinutesCharged });
      } finally {
        await deleteKey(lock);
      }
    },
  );

  server.tool(
    "end_live_session",
    "End an active live session",
    {
      sessionId: z.string().min(1).describe("Live session ID"),
    },
    async ({ sessionId }) => {
      const session = await getJson<LiveSession>(liveSessionKey(userId, sessionId));
      if (!session) {
        throw new Error(`Live session not found: ${sessionId}`);
      }
      if (session.status !== "active") {
        return jsonText({ duplicate: true, message: `Session already ${session.status}` });
      }

      const now = nowIso();
      const updatedSession: LiveSession = { ...session, status: "ended", endedAt: now };
      await setJson(liveSessionKey(userId, sessionId), updatedSession);

      return jsonText({ success: true, sessionId, endedAt: now, totalMinutesCharged: updatedSession.totalMinutesCharged, totalCreditsCharged: updatedSession.totalCreditsCharged });
    },
  );

  server.tool(
    "get_live_sessions",
    "Get paginated live sessions for the user",
    {
      cursor: z.string().optional().describe("Pagination cursor (offset)"),
      limit: z.number().int().min(1).max(100).optional().describe("Items per page (default 25)"),
    },
    async ({ cursor, limit }) => {
      const page = await getIndexedJsonPage<LiveSession>(
        liveSessionIndexKey(userId),
        cursor ? Number(cursor) : 0,
        limit ?? 25,
      );
      return jsonText(page);
    },
  );

  // --- Feature configs ---

  server.tool(
    "list_features",
    "List all feature configurations",
    {},
    async () => {
      return jsonText(Object.values(FEATURE_CONFIGS));
    },
  );

  return server;
}

// ---------------------------------------------------------------------------
// EXPRESS
// ---------------------------------------------------------------------------

const app = express();

app.use(express.json());

app.get("/favicon.ico", (_req: Request, res: Response) => {
  res.status(204).end();
});

app.get("/health", (_req: Request, res: Response) => {
  res.json({ status: "ok", service: "credit-store" });
});

app.post("/u/:userId", async (req: Request, res: Response) => {
  const { userId } = req.params;
  try {
    const server = createCreditStoreServer(normalizeUserId(userId));
    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: () => randomUUID() });
    await server.connect(transport);
    await transport.handleRequest(req, res, req.body);
  } catch (error) {
    console.error("[credit-store] error handling request", error);
    if (!res.headersSent) {
      res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
  }
});

app.listen(PORT, () => {
  console.log(`[credit-store] listening on port ${PORT}`);
});