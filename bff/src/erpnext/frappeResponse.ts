import type { ErpError } from "./client.js";

/** Best-effort parse of upstream HR API error JSON for logs or dev hints. */
export function parseFrappeErrorBody(body: unknown): string | null {
  if (body == null) return null;
  if (typeof body === "string") return body.slice(0, 500);
  if (typeof body !== "object") return null;
  const o = body as Record<string, unknown>;

  if (typeof o.exc === "string") {
    try {
      const exc = JSON.parse(o.exc) as unknown;
      if (Array.isArray(exc) && exc[0]) return String(exc[0]).slice(0, 500);
    } catch {
      /* ignore */
    }
  }
  if (typeof o.exception === "string") return o.exception.slice(0, 500);

  if (typeof o._server_messages === "string") {
    try {
      const msgs = JSON.parse(o._server_messages) as unknown[];
      if (Array.isArray(msgs) && msgs[0]) {
        const inner = JSON.parse(String(msgs[0])) as { message?: string };
        if (inner?.message) return String(inner.message).slice(0, 500);
      }
    } catch {
      /* ignore */
    }
  }

  if (typeof o.message === "string") return o.message.slice(0, 500);
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
    typeof hint === "string" &&
    (/Traceback \(most recent call last\)/.test(hint) || hint.includes("apps/frappe/"));
  let safeHint = looksLikeTrace ? null : hint;
  if (typeof safeHint === "string" && hintLooksLikeVendorNoise(safeHint)) safeHint = null;

  const recordStale =
    e.status === 417 ||
    bodyIndicatesRecordStale(e.body) ||
    (typeof safeHint === "string" &&
      (/document has been modified/i.test(safeHint) || /timestamp\s*mismatch/i.test(safeHint)));
  if (e.status >= 500) {
    return {
      error: "HR is temporarily unavailable. Please try again in a moment.",
      code: "HR_UPSTREAM",
      ...(dev && safeHint ? { hint: safeHint } : {}),
    };
  }

  if (recordStale) {
    return {
      error: "This record was updated elsewhere. Refresh the page and try again.",
      code: "HR_RECORD_STALE",
      ...(dev && e.body ? { detail: e.body } : {}),
    };
  }

  const generic = "Something went wrong with this request. Please try again, or refresh if the problem continues.";

  return {
    error: safeHint || generic,
    code: "HR_REQUEST_FAILED",
    ...(dev && e.body ? { detail: e.body } : {}),
  };
}
