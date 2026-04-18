import { createHmac, timingSafeEqual } from "node:crypto";
import type { IncomingHttpHeaders } from "node:http";
import * as appConfig from "../config.js";

function constTimeEqualStr(a: string, b: string): boolean {
  const tag = (s: string) =>
    createHmac("sha256", "docuseal-webhook-compare").update(s, "utf8").digest();
  return timingSafeEqual(tag(a), tag(b));
}

function headerValue(headers: IncomingHttpHeaders, name: string): string | undefined {
  const key = name.toLowerCase();
  const raw = headers[key] ?? headers[name];
  if (raw === undefined) return undefined;
  const v = Array.isArray(raw) ? raw[0] : raw;
  return typeof v === "string" ? v : undefined;
}

export type DocusealWebhookVerifyResult =
  | { ok: true }
  | { ok: false; status: number; error: string };

/**
 * DocuSeal “Add Secret” sends a header name + value you choose in the console.
 * Set DOCUSEAL_WEBHOOK_HEADER_NAME and DOCUSEAL_WEBHOOK_SECRET to match.
 */
export function verifyDocusealWebhookHeaders(headers: IncomingHttpHeaders): DocusealWebhookVerifyResult {
  const insecureOk =
    appConfig.DOCUSEAL_WEBHOOK_INSECURE &&
    (process.env.NODE_ENV === undefined || process.env.NODE_ENV !== "production");
  if (insecureOk) {
    return { ok: true };
  }

  const headerName = appConfig.DOCUSEAL_WEBHOOK_HEADER_NAME.trim();
  const secret = appConfig.DOCUSEAL_WEBHOOK_SECRET.trim();
  if (!headerName || !secret) {
    return {
      ok: false,
      status: 503,
      error:
        "DocuSeal webhook not configured: set DOCUSEAL_WEBHOOK_HEADER_NAME and DOCUSEAL_WEBHOOK_SECRET (or DOCUSEAL_WEBHOOK_INSECURE=1 outside production only)",
    };
  }

  const received = headerValue(headers, headerName);
  if (!received || !constTimeEqualStr(received.trim(), secret)) {
    return { ok: false, status: 401, error: "Invalid webhook credentials" };
  }
  return { ok: true };
}

/** In-process dedupe for DocuSeal retries (48h window per their docs); scale-out needs Redis/ERP idempotency. */
const seenKeys = new Map<string, number>();
const DEDUPE_TTL_MS = 72 * 60 * 60 * 1000;
const DEDUPE_MAX = 15_000;

function pruneDedupe(now: number): void {
  if (seenKeys.size < DEDUPE_MAX) return;
  for (const [k, t] of seenKeys) {
    if (now - t > DEDUPE_TTL_MS) seenKeys.delete(k);
    if (seenKeys.size < DEDUPE_MAX * 0.7) break;
  }
}

/** @returns true if this delivery should be skipped (already processed recently). */
export function takeDocusealDuplicate(key: string): boolean {
  const now = Date.now();
  pruneDedupe(now);
  const prev = seenKeys.get(key);
  if (prev !== undefined && now - prev < DEDUPE_TTL_MS) {
    return true;
  }
  seenKeys.set(key, now);
  return false;
}

export function docusealWebhookDedupeKey(payload: unknown): string {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    return `invalid:${String(payload).slice(0, 80)}`;
  }
  const p = payload as Record<string, unknown>;
  const eventType = String(p.event_type ?? "unknown");
  const ts = String(p.timestamp ?? "");
  const data = p.data;
  if (!data || typeof data !== "object" || Array.isArray(data)) {
    return `${eventType}:${ts}`;
  }
  const d = data as Record<string, unknown>;
  const submission = d.submission;
  const submissionId =
    submission && typeof submission === "object" && !Array.isArray(submission)
      ? (submission as Record<string, unknown>).id
      : undefined;
  const partyId = d.id;

  if (partyId != null && submissionId != null) {
    return `${eventType}:submission:${submissionId}:party:${partyId}`;
  }
  if (submissionId != null) {
    return `${eventType}:submission:${submissionId}`;
  }
  if (partyId != null) {
    return `${eventType}:id:${partyId}`;
  }
  return `${eventType}:${ts}`;
}
