/**
 * Employee lifecycle event tracking.
 *
 * Events (promotion, transfer, offboarding_checklist, exit_interview) are stored
 * as ERPNext Comments on the Employee doc — no custom doctypes required.
 * The Comment's `content` field holds a JSON payload with the event data and status.
 *
 * State machine:  draft → submitted → approved | rejected → completed
 *
 * Routes:
 *   GET    /v1/employees/:id/lifecycle                     — list events
 *   POST   /v1/employees/:id/lifecycle/:eventType          — log new event
 *   PATCH  /v1/employees/:id/lifecycle/:eventId/status     — transition state
 */
import type { FastifyPluginAsync } from "fastify";
import { defaultClient, ErpError } from "../erpnext/client.js";
import { resolveHrContext, HttpError } from "../context/resolveHrContext.js";

const erp = defaultClient();

const LIFECYCLE_PREFIX = "[lifecycle:";
const VALID_EVENT_TYPES = ["promotion", "transfer", "offboarding_checklist", "exit_interview"] as const;
const VALID_STATUSES = ["draft", "submitted", "approved", "rejected", "completed"] as const;
type LifecycleStatus = (typeof VALID_STATUSES)[number];

interface LifecyclePayload {
  eventType: string;
  effective_date?: string | null;
  notes?: string | null;
  status: LifecycleStatus;
  created_at: string;
  updated_at?: string;
}

function parseEvent(row: Record<string, unknown>) {
  const subject = String(row.subject ?? "");
  const eventType = subject.startsWith(LIFECYCLE_PREFIX)
    ? subject.slice(LIFECYCLE_PREFIX.length).replace(/\]$/, "")
    : "unknown";
  let payload: Partial<LifecyclePayload> = {};
  try {
    payload = JSON.parse(String(row.content ?? "{}")) as Partial<LifecyclePayload>;
  } catch { /* ignore */ }
  return {
    id: String(row.name ?? ""),
    eventType: payload.eventType ?? eventType,
    effectiveDate: payload.effective_date ?? null,
    status: payload.status ?? "draft",
    notes: payload.notes ?? null,
    createdAt: payload.created_at ?? String(row.creation ?? ""),
  };
}

export const lifecycleRoutes: FastifyPluginAsync = async (app) => {

  // ── GET /v1/employees/:id/lifecycle ─────────────────────────────────────
  app.get("/v1/employees/:id/lifecycle", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    const employeeId = (req.params as { id: string }).id;

    // Verify employee belongs to this company
    try {
      const emp = await erp.getDoc(ctx.creds, "Employee", employeeId) as Record<string, unknown>;
      if (String(emp.company) !== ctx.company) return reply.status(403).send({ error: "Employee not in your company" });
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : 404).send({ error: "Employee not found" });
      throw e;
    }

    try {
      const rows = await erp.getList(ctx.creds, "Comment", {
        filters: [
          ["reference_doctype", "=", "Employee"],
          ["reference_name", "=", employeeId],
          ["subject", "like", `${LIFECYCLE_PREFIX}%`],
        ],
        fields: ["name", "subject", "content", "creation", "owner"],
        order_by: "creation desc",
        limit_page_length: 200,
      });
      const data = (rows as Record<string, unknown>[]).map(parseEvent);
      return { data };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });

  // ── POST /v1/employees/:id/lifecycle/:eventType ───────────────────────────
  app.post("/v1/employees/:id/lifecycle/:eventType", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    const { id: employeeId, eventType } = req.params as { id: string; eventType: string };
    if (!VALID_EVENT_TYPES.includes(eventType as (typeof VALID_EVENT_TYPES)[number])) {
      return reply.status(400).send({ error: `Invalid event type. Must be one of: ${VALID_EVENT_TYPES.join(", ")}` });
    }

    const body = req.body as Record<string, unknown>;
    const effective_date = String(body?.effective_date ?? "").trim() || null;
    const notes = String(body?.notes ?? "").trim() || null;

    // Verify employee
    try {
      const emp = await erp.getDoc(ctx.creds, "Employee", employeeId) as Record<string, unknown>;
      if (String(emp.company) !== ctx.company) return reply.status(403).send({ error: "Employee not in your company" });
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : 404).send({ error: "Employee not found" });
      throw e;
    }

    const payload: LifecyclePayload = {
      eventType,
      effective_date,
      notes,
      status: "draft",
      created_at: new Date().toISOString(),
    };

    try {
      const created = await erp.createDoc(ctx.creds, "Comment", {
        reference_doctype: "Employee",
        reference_name: employeeId,
        comment_type: "Info",
        subject: `${LIFECYCLE_PREFIX}${eventType}]`,
        content: JSON.stringify(payload),
      });
      return { data: parseEvent({ ...payload, name: (created as Record<string, unknown>).name ?? "", creation: payload.created_at }) };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });

  // ── PATCH /v1/employees/:id/lifecycle/:eventId/status ────────────────────
  app.patch("/v1/employees/:id/lifecycle/:eventId/status", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    const { eventId } = req.params as { id: string; eventId: string };
    const newStatus = String((req.body as Record<string, unknown>)?.status ?? "") as LifecycleStatus;

    if (!VALID_STATUSES.includes(newStatus)) {
      return reply.status(400).send({ error: `Invalid status. Must be one of: ${VALID_STATUSES.join(", ")}` });
    }

    try {
      const comment = await erp.getDoc(ctx.creds, "Comment", eventId) as Record<string, unknown>;
      let payload: Partial<LifecyclePayload> = {};
      try { payload = JSON.parse(String(comment.content ?? "{}")); } catch { /* use empty */ }

      // Validate state machine transitions
      const current = payload.status ?? "draft";
      const allowed: Record<string, string[]> = {
        draft:     ["submitted"],
        submitted: ["approved", "rejected"],
        approved:  ["completed"],
        rejected:  ["draft"],        // allow re-open
        completed: [],
      };
      if (!(allowed[current] ?? []).includes(newStatus)) {
        return reply.status(400).send({ error: `Cannot transition from "${current}" to "${newStatus}"` });
      }

      payload.status = newStatus;
      payload.updated_at = new Date().toISOString();

      await erp.updateDoc(ctx.creds, "Comment", eventId, { content: JSON.stringify(payload) });
      return { data: { id: eventId, status: newStatus } };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });
};
