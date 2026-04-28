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