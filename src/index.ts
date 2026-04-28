import { randomUUID } from "crypto";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import express, { Request, Response } from "express";
import { z } from "zod";

const REDIS_URL = process.env.UPSTASH_REDIS_REST_URL!;
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN!;
const PORT = parseInt(process.env.PORT || "3001", 10);

if (!REDIS_URL || !REDIS_TOKEN) {
  throw new Error("Missing UPSTASH_REDIS_REST_URL or UPSTASH_REDIS_REST_TOKEN");
}

type AccessMode = "read" | "write";
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

interface SessionRecord {
  sessionId: string;
  userId: string;
  access: AccessMode;
  server: McpServer;
  transport: StreamableHTTPServerTransport;
  createdAt: number;
  lastSeenAt: number;
}

const SIGNUP_FREE_CREDITS = 25;
const MONTHLY_FREE_CREDITS = 25;
const MAX_LEDGER_ITEMS = 5000;
const MAX_USAGE_ITEMS = 5000;
const MAX_LIVE_SESSION_ITEMS = 500;
const IDEM_TTL_SECONDS = 60 * 60 * 24 * 90;
const SESSION_TTL_MS = 1000 * 60 * 60 * 24;

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

const sessions = new Map<string, SessionRecord>();

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

function requireFeature(code: FeatureCode): FeatureConfig {
  const feature = FEATURE_CONFIGS[code];
  if (!feature || !feature.active) {
    throw new Error(`Unknown or inactive feature: ${code}`);
  }
  return feature;
}

function currentMonthKey(date = new Date()) {
  return date.toISOString().slice(0, 7);
}

function getSessionHeader(req: Request): string | undefined {
  return (
    req.header("mcp-session-id") ||
    req.header("Mcp-Session-Id") ||
    req.header("MCP-Session-Id") ||
    undefined
  );
}

function setCommonHeaders(res: Response) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader(
    "Access-Control-Allow-Headers",
    [
      "Content-Type",
      "Accept",
      "Mcp-Session-Id",
      "mcp-session-id",
      "x-user-id",
    ].join(", "),
  );
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
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

async function getString(key: string): Promise<string | null> {
  const result = await redisCommand<string>("GET", key);
  return result ?? null;
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
  if (!raw) return null;
  try {
    return JSON.parse(raw) as T;
  } catch {
    return null;
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
  const total = safeParseNumber(
    await redisCommand<number>("LLEN", indexKey),
    0,
  );
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

  const itemKeys =
    (await redisCommand<string[]>("LRANGE", indexKey, offset, end)) || [];
  const items = (
    await Promise.all(
      itemKeys.map(async (key) => {
        const value = await getJson<T>(key);
        return value;
      }),
    )
  ).filter(Boolean) as T[];

  const nextOffset = offset + items.length;
  const hasMore = nextOffset < total;

  return {
    items,
    nextCursor: hasMore ? String(nextOffset) : null,
    hasMore,
    total,
  };
}

async function getBalance(userId: string): Promise<number> {
  const result = await getString(walletKey(userId));
  return safeParseNumber(result, 0);
}

async function setBalance(userId: string, balance: number): Promise<Wallet> {
  const wallet: Wallet = {
    userId,
    balance,
    updatedAt: nowIso(),
  };
  await setJson(walletKey(userId), wallet);
  return wallet;
}

async function readWallet(userId: string): Promise<Wallet> {
  const wallet = await getJson<Wallet>(walletKey(userId));
  if (wallet) return wallet;
  return {
    userId,
    balance: 0,
    updatedAt: nowIso(),
  };
}

async function saveLedgerEntry(entry: CreditLedgerEntry) {
  await appendIndexedJson(
    ledgerIndexKey(entry.userId),
    ledgerItemKey(entry.userId, entry.id),
    entry,
    MAX_LEDGER_ITEMS,
  );
}

async function saveUsageEvent(event: UsageEvent) {
  const exists = await getJson<UsageEvent>(
    usageItemKey(event.userId, event.id),
  );
  await setJson(usageItemKey(event.userId, event.id), event);
  if (!exists) {
    await pushIndex(
      usageIndexKey(event.userId),
      usageItemKey(event.userId, event.id),
      MAX_USAGE_ITEMS,
    );
  }
}

async function saveLiveSession(session: LiveSession) {
  const exists = await getJson<LiveSession>(
    liveSessionKey(session.userId, session.id),
  );
  await setJson(liveSessionKey(session.userId, session.id), session);
  if (!exists) {
    await pushIndex(
      liveSessionIndexKey(session.userId),
      liveSessionKey(session.userId, session.id),
      MAX_LIVE_SESSION_ITEMS,
    );
  }
}

async function withUserLock<T>(
  userId: string,
  fn: () => Promise<T>,
): Promise<T> {
  const key = lockKey(userId);
  const token = randomUUID();
  const timeoutMs = 10000;
  const started = Date.now();

  while (Date.now() - started < timeoutMs) {
    const acquired = await redisCommand<string>(
      "SET",
      key,
      token,
      "NX",
      "EX",
      10,
    );

    if (acquired === "OK") {
      try {
        return await fn();
      } finally {
        const owner = await getString(key);
        if (owner === token) {
          await deleteKey(key);
        }
      }
    }

    await sleep(100);
  }

  throw new Error("Wallet is busy. Try again in a second.");
}

async function createLedgerEntry(args: {
  userId: string;
  amount: number;
  entryType: LedgerEntryType;
  referenceType: LedgerReferenceType;
  referenceId: string;
  idempotencyKey?: string;
  description?: string;
}) {
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
      const existingLedgerId = await getString(
        idempotencyKeyFor(userId, idempotencyKey),
      );
      if (existingLedgerId) {
        const existing = await getJson<CreditLedgerEntry>(
          ledgerItemKey(userId, existingLedgerId),
        );
        if (existing) {
          return {
            duplicated: true,
            balance: existing.balanceAfter,
            entry: existing,
          };
        }
      }
    }

    const currentBalance = await getBalance(userId);
    const nextBalance = currentBalance + amount;

    if (nextBalance < 0) {
      throw new Error(
        `Insufficient credits. Current balance: ${currentBalance}, requested: ${Math.abs(amount)}.`,
      );
    }

    const entry: CreditLedgerEntry = {
      id: randomUUID(),
      userId,
      entryType,
      amount,
      balanceAfter: nextBalance,
      referenceType,
      referenceId,
      idempotencyKey,
      description,
      createdAt: nowIso(),
    };

    await setBalance(userId, nextBalance);
    await saveLedgerEntry(entry);

    if (idempotencyKey) {
      await setString(
        idempotencyKeyFor(userId, idempotencyKey),
        entry.id,
        IDEM_TTL_SECONDS,
      );
    }

    return {
      duplicated: false,
      balance: nextBalance,
      entry,
    };
  });
}

async function createUsageEvent(args: {
  userId: string;
  featureCode: FeatureCode;
  quantity: number;
  creditsCharged: number;
  referenceId: string;
  idempotencyKey?: string;
  sessionId?: string;
  platform?: string;
  domain?: string;
  status?: UsageStatus;
  errorMessage?: string;
  ledgerEntryId?: string;
  autoRefundLedgerEntryId?: string;
}) {
  const feature = requireFeature(args.featureCode);
  const event: UsageEvent = {
    id: randomUUID(),
    userId: args.userId,
    featureCode: feature.code,
    billingUnit: feature.billingUnit,
    quantity: args.quantity,
    creditsCharged: args.creditsCharged,
    referenceId: args.referenceId,
    idempotencyKey: args.idempotencyKey,
    sessionId: args.sessionId,
    platform: args.platform,
    domain: args.domain,
    status: args.status || "pending",
    errorMessage: args.errorMessage,
    ledgerEntryId: args.ledgerEntryId,
    autoRefundLedgerEntryId: args.autoRefundLedgerEntryId,
    createdAt: nowIso(),
    completedAt:
      args.status === "completed" ||
      args.status === "failed" ||
      args.status === "refunded"
        ? nowIso()
        : undefined,
  };

  await saveUsageEvent(event);
  return event;
}

async function updateUsageEvent(
  userId: string,
  usageEventId: string,
  patch: Partial<UsageEvent>,
) {
  const existing = await getJson<UsageEvent>(
    usageItemKey(userId, usageEventId),
  );
  if (!existing) throw new Error("Usage event not found");

  const updated: UsageEvent = {
    ...existing,
    ...patch,
    completedAt:
      patch.status && patch.status !== "pending"
        ? patch.completedAt || nowIso()
        : existing.completedAt,
  };

  await saveUsageEvent(updated);
  return updated;
}

async function grantSignupBonus(
  userId: string,
  referenceId?: string,
  idempotencyKey?: string,
) {
  const existing = await getJson<GrantMarker>(signupGrantKey(userId));
  if (existing) {
    const ledgerEntry = await getJson<CreditLedgerEntry>(
      ledgerItemKey(userId, existing.ledgerEntryId),
    );
    return {
      duplicated: true,
      marker: existing,
      ledgerEntry,
      balance: await getBalance(userId),
    };
  }

  const grantIdem = idempotencyKey || `signup-grant:${userId}`;
  const result = await createLedgerEntry({
    userId,
    amount: SIGNUP_FREE_CREDITS,
    entryType: "signup_grant",
    referenceType: "signup_bonus",
    referenceId: referenceId || `signup:${userId}`,
    idempotencyKey: grantIdem,
    description: "Signup bonus credits",
  });

  const marker: GrantMarker = {
    userId,
    grantType: "signup_grant",
    period: "one_time",
    ledgerEntryId: result.entry.id,
    createdAt: nowIso(),
  };

  await setJson(signupGrantKey(userId), marker);
  return {
    duplicated: false,
    marker,
    ledgerEntry: result.entry,
    balance: result.balance,
  };
}

async function grantMonthlyBonus(
  userId: string,
  month?: string,
  idempotencyKey?: string,
) {
  const period = month || currentMonthKey();
  const key = monthlyGrantKey(userId, period);
  const existing = await getJson<GrantMarker>(key);
  if (existing) {
    const ledgerEntry = await getJson<CreditLedgerEntry>(
      ledgerItemKey(userId, existing.ledgerEntryId),
    );
    return {
      duplicated: true,
      marker: existing,
      ledgerEntry,
      balance: await getBalance(userId),
    };
  }

  const grantIdem = idempotencyKey || `monthly-free:${userId}:${period}`;
  const result = await createLedgerEntry({
    userId,
    amount: MONTHLY_FREE_CREDITS,
    entryType: "monthly_grant",
    referenceType: "monthly_bonus",
    referenceId: period,
    idempotencyKey: grantIdem,
    description: `Monthly free credits for ${period}`,
  });

  const marker: GrantMarker = {
    userId,
    grantType: "monthly_grant",
    period,
    ledgerEntryId: result.entry.id,
    createdAt: nowIso(),
  };

  await setJson(key, marker);
  return {
    duplicated: false,
    marker,
    ledgerEntry: result.entry,
    balance: result.balance,
  };
}

async function chargeFeatureUsage(args: {
  userId: string;
  featureCode: FeatureCode;
  quantity: number;
  referenceId: string;
  idempotencyKey?: string;
  sessionId?: string;
  platform?: string;
  domain?: string;
  description?: string;
}) {
  const feature = requireFeature(args.featureCode);
  const quantity = Math.max(1, args.quantity);
  const creditsCharged = feature.creditsPerUnit * quantity;

  const usageEvent = await createUsageEvent({
    userId: args.userId,
    featureCode: feature.code,
    quantity,
    creditsCharged,
    referenceId: args.referenceId,
    idempotencyKey: args.idempotencyKey,
    sessionId: args.sessionId,
    platform: args.platform,
    domain: args.domain,
    status: "pending",
  });

  try {
    const ledger = await createLedgerEntry({
      userId: args.userId,
      amount: -creditsCharged,
      entryType: "usage",
      referenceType: "feature_usage",
      referenceId: usageEvent.id,
      idempotencyKey: args.idempotencyKey,
      description: args.description || `${feature.name} usage`,
    });

    const updatedUsage = await updateUsageEvent(args.userId, usageEvent.id, {
      status: "completed",
      ledgerEntryId: ledger.entry.id,
    });

    return {
      usageEvent: updatedUsage,
      ledgerEntry: ledger.entry,
      balance: ledger.balance,
      duplicated: ledger.duplicated,
      feature,
    };
  } catch (error) {
    await updateUsageEvent(args.userId, usageEvent.id, {
      status: "failed",
      errorMessage: error instanceof Error ? error.message : "Unknown error",
    });
    throw error;
  }
}

async function autoRefundUsage(args: {
  userId: string;
  usageEventId: string;
  referenceId?: string;
  idempotencyKey?: string;
  description?: string;
}) {
  const usageEvent = await getJson<UsageEvent>(
    usageItemKey(args.userId, args.usageEventId),
  );
  if (!usageEvent) {
    throw new Error("Usage event not found");
  }

  if (usageEvent.status === "refunded") {
    const refundId = usageEvent.autoRefundLedgerEntryId;
    const refundEntry = refundId
      ? await getJson<CreditLedgerEntry>(ledgerItemKey(args.userId, refundId))
      : null;
    return {
      duplicated: true,
      usageEvent,
      refundEntry,
      balance: await getBalance(args.userId),
    };
  }

  if (usageEvent.creditsCharged <= 0) {
    throw new Error("Usage event has no refundable credits");
  }

  const refundIdem =
    args.idempotencyKey || `auto-refund:${args.userId}:${args.usageEventId}`;

  const refund = await createLedgerEntry({
    userId: args.userId,
    amount: usageEvent.creditsCharged,
    entryType: "auto_refund",
    referenceType: "auto_refund",
    referenceId: args.referenceId || args.usageEventId,
    idempotencyKey: refundIdem,
    description:
      args.description ||
      `Automatic refund for failed ${usageEvent.featureCode}`,
  });

  const updatedUsage = await updateUsageEvent(args.userId, args.usageEventId, {
    status: "refunded",
    autoRefundLedgerEntryId: refund.entry.id,
  });

  return {
    duplicated: refund.duplicated,
    usageEvent: updatedUsage,
    refundEntry: refund.entry,
    balance: refund.balance,
  };
}

async function startLiveSession(args: {
  userId: string;
  sessionId?: string;
  platform?: string;
  domain?: string;
}) {
  const id = args.sessionId || randomUUID();
  const existing = await getJson<LiveSession>(liveSessionKey(args.userId, id));
  if (existing) {
    return {
      duplicated: true,
      session: existing,
    };
  }

  const session: LiveSession = {
    id,
    userId: args.userId,
    featureCode: "live_minute",
    startedAt: nowIso(),
    status: "active",
    totalMinutesCharged: 0,
    totalCreditsCharged: 0,
    platform: args.platform,
    domain: args.domain,
  };

  await saveLiveSession(session);

  return {
    duplicated: false,
    session,
  };
}

async function chargeLiveMinute(args: {
  userId: string;
  sessionId: string;
  minutes?: number;
  referenceId?: string;
  idempotencyKey?: string;
  description?: string;
}) {
  const session = await getJson<LiveSession>(
    liveSessionKey(args.userId, args.sessionId),
  );
  if (!session) {
    throw new Error("Live session not found");
  }

  if (session.status !== "active") {
    throw new Error("Live session is not active");
  }

  const minutes = Math.max(1, args.minutes || 1);

  const charge = await chargeFeatureUsage({
    userId: args.userId,
    featureCode: "live_minute",
    quantity: minutes,
    referenceId:
      args.referenceId || `live-minute:${args.sessionId}:${Date.now()}`,
    idempotencyKey: args.idempotencyKey,
    sessionId: args.sessionId,
    platform: session.platform,
    domain: session.domain,
    description: args.description || `Live session minute charge (${minutes})`,
  });

  const updatedSession: LiveSession = {
    ...session,
    totalMinutesCharged: session.totalMinutesCharged + minutes,
    totalCreditsCharged:
      session.totalCreditsCharged + charge.usageEvent.creditsCharged,
    lastMinuteChargedAt: nowIso(),
  };

  await saveLiveSession(updatedSession);

  return {
    session: updatedSession,
    usageEvent: charge.usageEvent,
    ledgerEntry: charge.ledgerEntry,
    balance: charge.balance,
    duplicated: charge.duplicated,
  };
}

async function endLiveSession(args: {
  userId: string;
  sessionId: string;
  status?: LiveSessionStatus;
}) {
  const session = await getJson<LiveSession>(
    liveSessionKey(args.userId, args.sessionId),
  );
  if (!session) {
    throw new Error("Live session not found");
  }

  const updated: LiveSession = {
    ...session,
    endedAt: nowIso(),
    status: args.status || "ended",
  };

  await saveLiveSession(updated);
  return updated;
}

async function getLedgerPage(userId: string, cursor = 0, limit = 25) {
  const page = await getIndexedJsonPage<CreditLedgerEntry>(
    ledgerIndexKey(userId),
    cursor,
    limit,
  );

  return {
    entries: page.items,
    nextCursor: page.nextCursor,
    hasMore: page.hasMore,
    total: page.total,
  };
}

async function getUsagePage(userId: string, cursor = 0, limit = 25) {
  const page = await getIndexedJsonPage<UsageEvent>(
    usageIndexKey(userId),
    cursor,
    limit,
  );

  return {
    entries: page.items,
    nextCursor: page.nextCursor,
    hasMore: page.hasMore,
    total: page.total,
  };
}

async function getLiveSessionsPage(userId: string, cursor = 0, limit = 25) {
  const page = await getIndexedJsonPage<LiveSession>(
    liveSessionIndexKey(userId),
    cursor,
    limit,
  );

  return {
    entries: page.items,
    nextCursor: page.nextCursor,
    hasMore: page.hasMore,
    total: page.total,
  };
}

function registerReadTools(server: McpServer, userId: string) {
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
      const wallet = await readWallet(userId);
      return jsonText(wallet);
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
        packs: CREDIT_PACKS.filter((pack) => pack.active),
      });
    },
  );

  server.registerTool(
    "credits_get_features",
    {
      title: "Get Credit Features",
      description: "List all configured feature billing rules.",
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
        features: Object.values(FEATURE_CONFIGS).filter(
          (feature) => feature.active,
        ),
      });
    },
  );

  server.registerTool(
    "credits_get_ledger",
    {
      title: "Get Credit Ledger",
      description: "Return recent credit ledger entries for this user.",
      inputSchema: {
        cursor: z.string().default("0"),
        limit: z.number().int().min(1).max(100).default(25),
      },
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: false,
      },
    },
    async ({ cursor, limit }: { cursor: string; limit: number }) => {
      const page = await getLedgerPage(
        userId,
        safeParseNumber(cursor, 0),
        limit,
      );
      const wallet = await readWallet(userId);
      return jsonText({
        userId,
        balance: wallet.balance,
        ...page,
      });
    },
  );

  server.registerTool(
    "credits_get_usage_history",
    {
      title: "Get Usage History",
      description: "Return recent feature usage events for this user.",
      inputSchema: {
        cursor: z.string().default("0"),
        limit: z.number().int().min(1).max(100).default(25),
      },
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: false,
      },
    },
    async ({ cursor, limit }: { cursor: string; limit: number }) => {
      const page = await getUsagePage(
        userId,
        safeParseNumber(cursor, 0),
        limit,
      );
      return jsonText({
        userId,
        ...page,
      });
    },
  );

  server.registerTool(
    "credits_get_live_sessions",
    {
      title: "Get Live Sessions",
      description: "Return recent live sessions for this user.",
      inputSchema: {
        cursor: z.string().default("0"),
        limit: z.number().int().min(1).max(100).default(25),
      },
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: false,
      },
    },
    async ({ cursor, limit }: { cursor: string; limit: number }) => {
      const page = await getLiveSessionsPage(
        userId,
        safeParseNumber(cursor, 0),
        limit,
      );
      return jsonText({
        userId,
        ...page,
      });
    },
  );
}

function registerWriteTools(server: McpServer, userId: string) {
  server.registerTool(
    "credits_purchase_pack",
    {
      title: "Purchase Credit Pack",
      description: "Add credits to the wallet from a pack purchase.",
      inputSchema: {
        pack_code: z.enum(["basic", "standard", "plus", "ultimate"]),
        reference_id: z.string().min(1),
        idempotency_key: z.string().min(1).optional(),
        platform: z.string().min(1).optional(),
        domain: z.string().min(1).optional(),
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
      reference_id: string;
      idempotency_key?: string;
      platform?: string;
      domain?: string;
    }) => {
      const pack = PACK_BY_CODE.get(pack_code);
      if (!pack || !pack.active) {
        throw new Error(`Unknown or inactive pack code: ${pack_code}`);
      }

      const result = await createLedgerEntry({
        userId,
        amount: pack.credits,
        entryType: "purchase",
        referenceType: "pack_purchase",
        referenceId: reference_id,
        idempotencyKey: idempotency_key || reference_id,
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
    "credits_grant_signup_bonus",
    {
      title: "Grant Signup Bonus",
      description: "Grant the one-time signup credit bonus.",
      inputSchema: {
        reference_id: z.string().min(1).optional(),
        idempotency_key: z.string().min(1).optional(),
      },
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: false,
      },
    },
    async ({
      reference_id,
      idempotency_key,
    }: {
      reference_id?: string;
      idempotency_key?: string;
    }) => {
      const result = await grantSignupBonus(
        userId,
        reference_id,
        idempotency_key,
      );
      return jsonText({
        userId,
        freeCredits: SIGNUP_FREE_CREDITS,
        ...result,
      });
    },
  );

  server.registerTool(
    "credits_grant_monthly_bonus",
    {
      title: "Grant Monthly Bonus",
      description: "Grant the monthly free credits once per month.",
      inputSchema: {
        month: z
          .string()
          .regex(/^\d{4}-\d{2}$/)
          .optional(),
        idempotency_key: z.string().min(1).optional(),
      },
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: false,
      },
    },
    async ({
      month,
      idempotency_key,
    }: {
      month?: string;
      idempotency_key?: string;
    }) => {
      const result = await grantMonthlyBonus(userId, month, idempotency_key);
      return jsonText({
        userId,
        freeCredits: MONTHLY_FREE_CREDITS,
        ...result,
      });
    },
  );

  server.registerTool(
    "credits_charge_feature_usage",
    {
      title: "Charge Feature Usage",
      description: "Charge credits for a feature and log the usage event.",
      inputSchema: {
        feature_code: z.enum([
          "image_generate",
          "image_edit",
          "photo_booth",
          "model_train",
          "live_minute",
        ]),
        quantity: z.number().int().min(1).default(1),
        reference_id: z.string().min(1),
        idempotency_key: z.string().min(1).optional(),
        session_id: z.string().min(1).optional(),
        platform: z.string().min(1).optional(),
        domain: z.string().min(1).optional(),
        description: z.string().min(1).optional(),
      },
      annotations: {
        readOnlyHint: false,
        destructiveHint: true,
        idempotentHint: true,
        openWorldHint: false,
      },
    },
    async ({
      feature_code,
      quantity,
      reference_id,
      idempotency_key,
      session_id,
      platform,
      domain,
      description,
    }: {
      feature_code: FeatureCode;
      quantity: number;
      reference_id: string;
      idempotency_key?: string;
      session_id?: string;
      platform?: string;
      domain?: string;
      description?: string;
    }) => {
      const result = await chargeFeatureUsage({
        userId,
        featureCode: feature_code,
        quantity,
        referenceId: reference_id,
        idempotencyKey: idempotency_key || reference_id,
        sessionId: session_id,
        platform,
        domain,
        description,
      });

      return jsonText({
        userId,
        feature: result.feature,
        usageEvent: result.usageEvent,
        ledgerEntry: result.ledgerEntry,
        balance: result.balance,
        duplicated: result.duplicated,
      });
    },
  );

  server.registerTool(
    "credits_auto_refund_usage",
    {
      title: "Auto Refund Usage",
      description:
        "Automatically refund a previously charged usage event after a failed operation.",
      inputSchema: {
        usage_event_id: z.string().min(1),
        reference_id: z.string().min(1).optional(),
        idempotency_key: z.string().min(1).optional(),
        description: z.string().min(1).optional(),
      },
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: false,
      },
    },
    async ({
      usage_event_id,
      reference_id,
      idempotency_key,
      description,
    }: {
      usage_event_id: string;
      reference_id?: string;
      idempotency_key?: string;
      description?: string;
    }) => {
      const result = await autoRefundUsage({
        userId,
        usageEventId: usage_event_id,
        referenceId: reference_id,
        idempotencyKey: idempotency_key,
        description,
      });

      return jsonText({
        userId,
        ...result,
      });
    },
  );

  server.registerTool(
    "credits_start_live_session",
    {
      title: "Start Live Session",
      description: "Create a live session record.",
      inputSchema: {
        session_id: z.string().min(1).optional(),
        platform: z.string().min(1).optional(),
        domain: z.string().min(1).optional(),
      },
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: false,
      },
    },
    async ({
      session_id,
      platform,
      domain,
    }: {
      session_id?: string;
      platform?: string;
      domain?: string;
    }) => {
      const result = await startLiveSession({
        userId,
        sessionId: session_id,
        platform,
        domain,
      });

      return jsonText({
        userId,
        ...result,
      });
    },
  );

  server.registerTool(
    "credits_charge_live_minute",
    {
      title: "Charge Live Minute",
      description: "Charge one or more live session minutes.",
      inputSchema: {
        session_id: z.string().min(1),
        minutes: z.number().int().min(1).default(1),
        reference_id: z.string().min(1).optional(),
        idempotency_key: z.string().min(1).optional(),
        description: z.string().min(1).optional(),
      },
      annotations: {
        readOnlyHint: false,
        destructiveHint: true,
        idempotentHint: true,
        openWorldHint: false,
      },
    },
    async ({
      session_id,
      minutes,
      reference_id,
      idempotency_key,
      description,
    }: {
      session_id: string;
      minutes: number;
      reference_id?: string;
      idempotency_key?: string;
      description?: string;
    }) => {
      const result = await chargeLiveMinute({
        userId,
        sessionId: session_id,
        minutes,
        referenceId: reference_id,
        idempotencyKey: idempotency_key,
        description,
      });

      return jsonText({
        userId,
        ...result,
      });
    },
  );

  server.registerTool(
    "credits_end_live_session",
    {
      title: "End Live Session",
      description: "End a live session.",
      inputSchema: {
        session_id: z.string().min(1),
        status: z.enum(["active", "ended", "failed"]).optional(),
      },
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: false,
      },
    },
    async ({
      session_id,
      status,
    }: {
      session_id: string;
      status?: LiveSessionStatus;
    }) => {
      const session = await endLiveSession({
        userId,
        sessionId: session_id,
        status,
      });

      return jsonText({
        userId,
        session,
      });
    },
  );
}

function createCreditsServer(userId: string, access: AccessMode): McpServer {
  const server = new McpServer({
    name: access === "read" ? "credits-mcp-public" : "credits-mcp-internal",
    version: "2.0.0",
  });

  registerReadTools(server, userId);

  if (access === "write") {
    registerWriteTools(server, userId);
  }

  return server;
}

function resolvePublicUserId(req: Request): string | null {
  const userId = req.header("x-user-id") || String(req.query.userId || "");
  if (!userId) return null;
  return normalizeUserId(userId);
}

function resolveWriteUserId(req: Request): string | null {
  const userId =
    req.params.userId ||
    req.header("x-user-id") ||
    String(req.query.userId || "");
  if (!userId) return null;
  return normalizeUserId(userId);
}

async function createSession(userId: string, access: AccessMode) {
  const server = createCreditsServer(userId, access);
  const transport = new StreamableHTTPServerTransport({
    sessionIdGenerator: () => randomUUID(),
    enableJsonResponse: true,
  });

  await server.connect(transport);

  return {
    server,
    transport,
  };
}

async function getOrCreateRecord(
  req: Request,
  userId: string,
  access: AccessMode,
) {
  const sessionId = getSessionHeader(req);

  if (sessionId) {
    const record = sessions.get(sessionId);
    if (!record) {
      throw new Error("Session not found");
    }
    if (record.userId !== userId) {
      throw new Error("Session user mismatch");
    }
    if (record.access !== access) {
      throw new Error("Session access mismatch");
    }
    record.lastSeenAt = Date.now();
    return record;
  }

  const created = await createSession(userId, access);
  const tempSessionId = (created.transport as any).sessionId || randomUUID();

  const record: SessionRecord = {
    sessionId: tempSessionId,
    userId,
    access,
    server: created.server,
    transport: created.transport,
    createdAt: Date.now(),
    lastSeenAt: Date.now(),
  };

  return record;
}

async function persistSessionIfAvailable(record: SessionRecord) {
  const realSessionId = (record.transport as any).sessionId;
  if (!realSessionId) return record;

  if (record.sessionId !== realSessionId) {
    record.sessionId = realSessionId;
  }

  sessions.set(record.sessionId, record);
  return record;
}

async function cleanupExpiredSessions() {
  const now = Date.now();

  for (const [sessionId, record] of sessions.entries()) {
    if (now - record.lastSeenAt > SESSION_TTL_MS) {
      try {
        await record.transport.close?.();
      } catch {}
      sessions.delete(sessionId);
    }
  }
}

setInterval(() => {
  void cleanupExpiredSessions();
}, 60 * 1000);

const app = express();

app.use((req, res, next) => {
  setCommonHeaders(res);
  next();
});

app.use(express.json({ limit: "2mb" }));

app.get("/favicon.ico", (_req, res) => res.status(204).end());

app.get("/health", async (_req, res) => {
  res.json({
    status: "ok",
    service: "credits-mcp-server",
    transport: "streamable-http",
    routes: {
      publicRead: "/mcp",
      internalWrite: "/u/:userId",
    },
    methods: ["GET", "POST", "DELETE", "OPTIONS"],
    auth: "disabled",
    freeCredits: {
      signup: SIGNUP_FREE_CREDITS,
      monthly: MONTHLY_FREE_CREDITS,
    },
    features: Object.values(FEATURE_CONFIGS),
    packs: CREDIT_PACKS.filter((pack) => pack.active),
    activeSessions: sessions.size,
  });
});

app.options("/mcp", (_req, res) => res.status(204).end());
app.options("/u/:userId", (_req, res) => res.status(204).end());

async function handleStreamableHttpRequest(
  req: Request,
  res: Response,
  access: AccessMode,
  resolveUserId: (req: Request) => string | null,
) {
  const userId = resolveUserId(req);
  if (!userId || userId.length < 2) {
    res.status(400).json({ error: "Missing or invalid user id" });
    return;
  }

  try {
    if (req.method === "POST") {
      const record = await getOrCreateRecord(req, userId, access);
      await record.transport.handleRequest(req, res, req.body);
      await persistSessionIfAvailable(record);
      return;
    }

    if (req.method === "GET") {
      const sessionId = getSessionHeader(req);
      if (!sessionId) {
        res.status(400).json({ error: "Missing Mcp-Session-Id header" });
        return;
      }

      const record = sessions.get(sessionId);
      if (!record) {
        res.status(400).json({ error: "Session not found" });
        return;
      }

      if (record.userId !== userId || record.access !== access) {
        res.status(400).json({ error: "Session mismatch" });
        return;
      }

      record.lastSeenAt = Date.now();
      await record.transport.handleRequest(req, res);
      return;
    }

    if (req.method === "DELETE") {
      const sessionId = getSessionHeader(req);
      if (!sessionId) {
        res.status(400).json({ error: "Missing Mcp-Session-Id header" });
        return;
      }

      const record = sessions.get(sessionId);
      if (!record) {
        res.status(400).json({ error: "Session not found" });
        return;
      }

      if (record.userId !== userId || record.access !== access) {
        res.status(400).json({ error: "Session mismatch" });
        return;
      }

      record.lastSeenAt = Date.now();
      await record.transport.handleRequest(req, res);
      try {
        await record.transport.close?.();
      } catch {}
      sessions.delete(sessionId);
      return;
    }

    res.status(405).json({ error: "Method not allowed" });
  } catch (error) {
    console.error(`[${access}] MCP error for ${userId}:`, error);
    if (!res.headersSent) {
      res.status(500).json({
        error: error instanceof Error ? error.message : "Internal server error",
      });
    }
  }
}

app.all("/mcp", async (req, res) => {
  await handleStreamableHttpRequest(req, res, "read", resolvePublicUserId);
});

app.all("/u/:userId", async (req, res) => {
  await handleStreamableHttpRequest(req, res, "write", resolveWriteUserId);
});

app.listen(PORT, () => {
  console.log(`Credits MCP running on :${PORT}`);
  console.log(`Public read lane: /mcp`);
  console.log(`Internal write lane: /u/:userId`);
  console.log(`Methods: GET POST DELETE OPTIONS`);
  console.log(`Auth: disabled`);
});
