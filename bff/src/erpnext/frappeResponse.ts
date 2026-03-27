import type { ErpError } from "./client.js";

/** Best-effort parse of upstream HR API error JSON for logs or dev hints. */
export function parseFrappeErrorBody(body: unknown): string | null {
  if (body == null) return null;
  if (typeof body === "string") {
    // Strip HTML error pages
    if (/<html[\s>]/i.test(body)) return null;
    return body.slice(0, 500);
  }
  if (typeof body !== "object") return null;
  const o = body as Record<string, unknown>;

  // Frappe wraps traceback in exc (JSON-encoded array of strings)
  if (typeof o.exc === "string") {
    try {
      const exc = JSON.parse(o.exc) as unknown;
      if (Array.isArray(exc) && exc[0]) {
        // Extract the last line which is the exception message
        const lines = String(exc[0]).split("\n").filter(Boolean);
        const last = lines[lines.length - 1]?.trim();
        if (last) return last.slice(0, 500);
      }
    } catch { /* ignore */ }
  }

  // exc_type + _server_messages is the most common Frappe 417 format
  if (typeof o._server_messages === "string") {
    try {
      const msgs = JSON.parse(o._server_messages) as unknown[];
      if (Array.isArray(msgs)) {
        for (const raw of msgs) {
          try {
            const inner = JSON.parse(String(raw)) as { message?: string };
            if (inner?.message) return String(inner.message).replace(/<[^>]*>/g, "").trim().slice(0, 500);
          } catch { /* try next */ }
        }
      }
    } catch { /* ignore */ }
  }

  if (typeof o.exception === "string") return o.exception.slice(0, 500);
  if (typeof o.message === "string") return o.message.replace(/<[^>]*>/g, "").trim().slice(0, 500);

  // Frappe sometimes nests under data
  const data = (o as Record<string, unknown>).data;
  if (data && typeof data === "object") return parseFrappeErrorBody(data);

  return null;
}

function bodyIndicatesRecordStale(body: unknown): boolean {
  if (body == null || typeof body !== "object") return false;
  const o = body as Record<string, unknown>;
  const blob = [
    typeof o.exception === "string" ? o.exception : "",
    typeof o.exc_type === "string" ? o.exc_type : "",
    typeof o.message === "string" ? o.message : "",
    typeof o.exc === "string" ? o.exc : "",
  ]
    .join(" ")
    .toLowerCase();
  return (
    blob.includes("timestampmismatch") ||
    blob.includes("document has been modified") ||
    blob.includes("refresh to get the latest")
  );
}

function hintLooksLikeVendorNoise(hint: string): boolean {
  const h = hint.toLowerCase();
  return (
    h.includes("traceback") ||
    h.includes("apps/frappe") ||
    h.includes("frappe.") ||
    h.includes("erpnext") ||
    h.includes("/frappe/")
  );
}

/** Map upstream HR system failures to safe JSON for clients (no backend product names). */
export function publicErpFailure(e: ErpError, dev = process.env.NODE_ENV === "development"): Record<string, unknown> {
  const hint = parseFrappeErrorBody(e.body);

  // Upstream sometimes returns full tracebacks in 4xx/5xx bodies — never show those to users.
  const looksLikeTrace =
    typeof hint === "string" && (/Traceback \(most recent call last\)/.test(hint) || hint.includes("apps/frappe/"));
  let safeHint: string | null = looksLikeTrace ? null : hint;
  if (typeof safeHint === "string" && hintLooksLikeVendorNoise(safeHint)) safeHint = null;
  // Log the extracted message so server logs surface the real ERPNext error without full tracebacks
  console.error(`[erp] HTTP ${e.status} — ${safeHint ?? hint ?? JSON.stringify(e.body ?? null).slice(0, 300)}`);

  const recordStale =
    bodyIndicatesRecordStale(e.body) ||
    (typeof safeHint === "string" && (/document has been modified/i.test(safeHint) || /timestamp\s*mismatch/i.test(safeHint)));
  if (e.status >= 500) {
    return {
      error: "HR is temporarily unavailable. Please try again in a moment.",
      code: "HR_UPSTREAM",
      ...(safeHint ? { hint: safeHint } : {}),
    };
  }

  if (recordStale) {
    return {
      error: "This record was updated elsewhere. Refresh the page and try again.",
      code: "HR_RECORD_STALE",
      ...(dev && e.body ? { detail: e.body } : {}),
    };
  }

  return {
    error: safeHint || "We couldn't complete this HR request. Check your account or try again.",
    code: "HR_REQUEST_FAILED",
  };
}
