import type { ErpError } from "./client.js";

/** Best-effort parse of Frappe /api error JSON for logs or dev hints. */
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

/** Map upstream ERP failures to safe JSON for the Pay Hub UI (no vendor names). */
export function publicErpFailure(e: ErpError, dev = process.env.NODE_ENV === "development"): Record<string, unknown> {
  const hint = parseFrappeErrorBody(e.body);
  if (e.status >= 500) {
    return {
      error: "Your company HR directory is temporarily unavailable. Please try again shortly.",
      code: "HR_UPSTREAM",
      ...(dev && hint ? { hint } : {}),
    };
  }
  return {
    error: hint || "We couldn't complete this HR request. Check your account or try again.",
    code: "HR_REQUEST_FAILED",
    ...(dev && e.body ? { detail: e.body } : {}),
  };
}
