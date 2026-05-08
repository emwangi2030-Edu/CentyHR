/**
 * Employee lifecycle event tracking.
 *
 * Most events are ERPNext Comments on the Employee doc (JSON payload + workflow status).
 *
 * performance_review is stored primarily as an HRMS **Goal** (Human Resources → Performance → Goals),
 * linked to **Employee**, so it appears in ERPNext performance screens while staying tied to the employee record.
 * If Goal DocType is unavailable, falls back to the Comment-based format like other lifecycle types.
 *
 * Routes:
 *   GET    /v1/employees/:id/lifecycle                     — list events (comments + Pay Hub Goals merged)
 *   POST   /v1/employees/:id/lifecycle/:eventType          — log new event
 *   PATCH  /v1/employees/:id/lifecycle/:eventId/status     — transition state (Comments only)
 */
import type { FastifyPluginAsync } from "fastify";
import { defaultClient, ErpError } from "../erpnext/client.js";
import { resolveHrContext, HttpError } from "../context/resolveHrContext.js";

const erp = defaultClient();

const LIFECYCLE_PREFIX = "[lifecycle:";
const VALID_EVENT_TYPES = ["promotion", "transfer", "offboarding_checklist", "exit_interview", "performance_review"] as const;
const VALID_STATUSES = ["draft", "submitted", "approved", "rejected", "completed"] as const;
type LifecycleStatus = (typeof VALID_STATUSES)[number];

/** Prefix for Goal.goal_name — GET filters Goals starting with this string; keep stable across releases. */
const PAY_HUB_PERF_GOAL_PREFIX = "Pay Hub · Performance ·";

interface LifecyclePayload {
  eventType: string;
  effective_date?: string | null;
  notes?: string | null;
  status: LifecycleStatus;
  created_at: string;
  updated_at?: string;
  /** Present when Comment fallback stores structured rating */
  rating?: number;
}

type LifecycleRow = {
  id: string;
  eventType: string;
  effectiveDate?: string | null;
  status: string;
  notes?: string | null;
  createdAt: string;
  /** When false, PATCH workflow steps do not apply (ERP Goal stored outside Comment workflow). */
  lifecycleWorkflow?: boolean;
  /** ERP Goal doc name when stored_as Goal */
  erpGoalName?: string | null;
};

function clampRating(n: unknown): number {
  const x = Math.round(Number(n));
  if (!Number.isFinite(x)) return 3;
  return Math.min(5, Math.max(1, x));
}

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function stripHtml(html: string): string {
  return html.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
}

function endOfYearYmd(startYmd: string): string {
  const y = Number(startYmd.slice(0, 4));
  const yr = Number.isFinite(y) && y >= 1900 ? y : new Date().getFullYear();
  return `${yr}-12-31`;
}

function parseRatingFromBody(body: Record<string, unknown>): number {
  const r = Number(body?.rating);
  if (Number.isFinite(r) && r >= 1 && r <= 5) return clampRating(r);
  const notes = String(body?.notes ?? "");
  const m = notes.match(/Rating\s+(\d)\s*\/\s*5/i);
  if (m) return clampRating(Number(m[1]));
  return 3;
}

function extractRemark(body: Record<string, unknown>): string {
  let remark = String(body?.notes ?? "").trim();
  remark = remark.replace(/^\s*Rating\s+\d\s*\/\s*5\s*(·|\.|,|\s)·?\s*/i, "").trim();
  return remark;
}

function parseEvent(row: Record<string, unknown>): LifecycleRow {
  const subject = String(row.subject ?? "");
  const eventType = subject.startsWith(LIFECYCLE_PREFIX)
    ? subject.slice(LIFECYCLE_PREFIX.length).replace(/\]$/, "")
    : "unknown";
  let payload: Partial<LifecyclePayload> = {};
  try {
    payload = JSON.parse(String(row.content ?? "{}")) as Partial<LifecyclePayload>;
  } catch {
    /* ignore */
  }
  return {
    id: String(row.name ?? ""),
    eventType: payload.eventType ?? eventType,
    effectiveDate: payload.effective_date ?? null,
    status: payload.status ?? "draft",
    notes: payload.notes ?? null,
    createdAt: payload.created_at ?? String(row.creation ?? ""),
    lifecycleWorkflow: true,
  };
}

function parseGoalPerformanceRow(row: Record<string, unknown>): LifecycleRow {
  const progress = Number(row.progress ?? 0);
  const rating = clampRating(Math.round((progress / 100) * 5));
  const descRaw = stripHtml(String(row.description ?? ""));
  const cleaned = descRaw.replace(/\s*Recorded via Pay Hub\.?\s*$/i, "").trim();
  const notesParts = [`Rating ${rating}/5`, cleaned].filter(Boolean);
  const notes = notesParts.length > 1 ? `${notesParts[0]}\n\n${notesParts.slice(1).join("\n")}` : notesParts[0];
  return {
    id: String(row.name ?? ""),
    eventType: "performance_review",
    effectiveDate: row.start_date ? String(row.start_date) : null,
    status: "completed",
    notes,
    createdAt: String(row.creation ?? ""),
    lifecycleWorkflow: false,
    erpGoalName: String(row.name ?? ""),
  };
}

function sortLifecycleMerged(rows: LifecycleRow[]): LifecycleRow[] {
  return [...rows].sort((a, b) => {
    const ta = Date.parse(String(a.createdAt ?? ""));
    const tb = Date.parse(String(b.createdAt ?? ""));
    return (Number.isFinite(tb) ? tb : 0) - (Number.isFinite(ta) ? ta : 0);
  });
}

/** ERP Employee `name` for the signed-in user in this company (same idea as `/v1/self-service/me`). */
async function resolveOwnEmployeeDocName(ctx: ReturnType<typeof resolveHrContext>): Promise<string | null> {
  for (const field of ["user_id", "personal_email"] as const) {
    try {
      const rows = await erp.getList(ctx.creds, "Employee", {
        filters: [[field, "=", ctx.userEmail], ["company", "=", ctx.company]],
        fields: ["name"],
        limit_page_length: 1,
      });
      const row = rows?.[0] as { name?: string } | undefined;
      if (row?.name) return String(row.name).trim();
    } catch {
      /* try next field */
    }
  }
  return null;
}

export const lifecycleRoutes: FastifyPluginAsync = async (app) => {
  // ── GET /v1/employees/:id/lifecycle ─────────────────────────────────────
  app.get("/v1/employees/:id/lifecycle", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const employeeId = String((req.params as { id: string }).id ?? "").trim();
    if (!employeeId) return reply.status(400).send({ error: "Employee id is required" });

    let mayAccess = ctx.canSubmitOnBehalf;
    if (!mayAccess) {
      const ownId = await resolveOwnEmployeeDocName(ctx);
      mayAccess = !!ownId && ownId === employeeId;
    }
    if (!mayAccess) return reply.status(403).send({ error: "HR admin privileges required" });


    try {
      const emp = await erp.getDoc(ctx.creds, "Employee", employeeId) as Record<string, unknown>;
      if (String(emp.company) !== ctx.company) return reply.status(403).send({ error: "Employee not in your company" });
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : 404).send({ error: "Employee not found" });
      throw e;
    }

    let commentRows: Record<string, unknown>[] = [];
    try {
      commentRows = (await erp.getList(ctx.creds, "Comment", {
        filters: [
          ["reference_doctype", "=", "Employee"],
          ["reference_name", "=", employeeId],
          ["subject", "like", `${LIFECYCLE_PREFIX}%`],
        ],
        fields: ["name", "subject", "content", "creation", "owner"],
        order_by: "creation desc",
        limit_page_length: 200,
      })) as Record<string, unknown>[];
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }

    const fromComments = commentRows.map(parseEvent);

    let goalRows: Record<string, unknown>[] = [];
    try {
      goalRows = (await erp.getList(ctx.creds, "Goal", {
        filters: [
          ["employee", "=", employeeId],
          ["goal_name", "like", `${PAY_HUB_PERF_GOAL_PREFIX}%`],
        ],
        fields: ["name", "goal_name", "creation", "start_date", "progress", "description"],
        order_by: "creation desc",
        limit_page_length: 100,
      })) as Record<string, unknown>[];
    } catch {
      goalRows = [];
    }

    const fromGoals = goalRows.map(parseGoalPerformanceRow);

    /** Dedupe: legacy Comment-based performance_review rows may coexist until migrated — prefer Goal row same calendar day & similar rating when duplicated is rare; merge list keeps both if unsure. */
    const merged = sortLifecycleMerged([...fromComments, ...fromGoals]);

    return { data: merged };
  });

  // ── POST /v1/employees/:id/lifecycle/:eventType ───────────────────────────
  app.post("/v1/employees/:id/lifecycle/:eventType", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    const { id: employeeId, eventType } = req.params as { id: string; eventType: string };
    if (!VALID_EVENT_TYPES.includes(eventType as (typeof VALID_EVENT_TYPES)[number])) {
      return reply.status(400).send({ error: `Invalid event type. Must be one of: ${VALID_EVENT_TYPES.join(", ")}` });
    }

    const body = (req.body ?? {}) as Record<string, unknown>;
    let effective_date = String(body?.effective_date ?? "").trim() || null;
    let notes = String(body?.notes ?? "").trim() || null;

    try {
      const emp = await erp.getDoc(ctx.creds, "Employee", employeeId) as Record<string, unknown>;
      if (String(emp.company) !== ctx.company) return reply.status(403).send({ error: "Employee not in your company" });
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : 404).send({ error: "Employee not found" });
      throw e;
    }

    // ── Performance review → ERPNext Goal (Performance module), Employee-linked ──
    if (eventType === "performance_review") {
      const rating = parseRatingFromBody(body);
      const remarkPlain = extractRemark(body);
      const eff = effective_date || new Date().toISOString().slice(0, 10);
      effective_date = eff;
      const progress = Math.round((rating / 5) * 100);
      const tsLabel = new Date().toISOString().slice(11, 19);
      const goalName = `${PAY_HUB_PERF_GOAL_PREFIX}${eff} · ${tsLabel}`;
      const htmlDesc =
        `<p><strong>Rating:</strong> ${rating}/5</p>` +
        (remarkPlain ? `<p>${escapeHtml(remarkPlain).replace(/\n/g, "<br/>")}</p>` : "") +
        `<p><em>Recorded via Pay Hub.</em></p>`;

      try {
        const created = (await erp.createDoc(ctx.creds, "Goal", {
          employee: employeeId,
          goal_name: goalName,
          start_date: eff,
          end_date: endOfYearYmd(eff),
          progress,
          description: htmlDesc,
        })) as Record<string, unknown>;

        const row = parseGoalPerformanceRow({
          ...created,
          creation: created.creation ?? new Date().toISOString(),
          start_date: eff,
          progress,
          description: htmlDesc,
        });
        return { data: row, meta: { stored_as: "erp_goal" as const } };
      } catch (e) {
        console.warn("[lifecycle] Goal DocType unavailable or validation failed — falling back to Employee Comment:", String(e));
        notes = remarkPlain ? `Rating ${rating}/5 · ${remarkPlain}` : `Rating ${rating}/5`;
        const payload: LifecyclePayload = {
          eventType,
          effective_date: eff,
          notes,
          status: "draft",
          created_at: new Date().toISOString(),
          rating,
        };
        try {
          const created = await erp.createDoc(ctx.creds, "Comment", {
            reference_doctype: "Employee",
            reference_name: employeeId,
            comment_type: "Info",
            subject: `${LIFECYCLE_PREFIX}${eventType}]`,
            content: JSON.stringify(payload),
          });
          const parsed = parseEvent({
            ...(created as Record<string, unknown>),
            subject: `${LIFECYCLE_PREFIX}${eventType}]`,
            content: JSON.stringify(payload),
            creation: payload.created_at,
          });
          return { data: parsed, meta: { stored_as: "comment_fallback" as const } };
        } catch (inner) {
          if (inner instanceof ErpError) return reply.status(inner.status >= 500 ? 502 : inner.status).send({ error: String(inner.message) });
          throw inner;
        }
      }
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
      const parsed = parseEvent({
        ...(created as Record<string, unknown>),
        subject: `${LIFECYCLE_PREFIX}${eventType}]`,
        content: JSON.stringify(payload),
        creation: payload.created_at,
      });
      return { data: parsed };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });

  // ── PATCH /v1/employees/:id/lifecycle/:eventId/status ────────────────────
  app.patch("/v1/employees/:id/lifecycle/:eventId/status", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    const { eventId } = req.params as { id: string; eventId: string };
    const newStatus = String((req.body as Record<string, unknown>)?.status ?? "") as LifecycleStatus;

    if (!VALID_STATUSES.includes(newStatus)) {
      return reply.status(400).send({ error: `Invalid status. Must be one of: ${VALID_STATUSES.join(", ")}` });
    }

    try {
      let comment: Record<string, unknown>;
      try {
        comment = (await erp.getDoc(ctx.creds, "Comment", eventId)) as Record<string, unknown>;
      } catch (ce) {
        let isGoal = false;
        try {
          await erp.getDoc(ctx.creds, "Goal", eventId);
          isGoal = true;
        } catch {
          isGoal = false;
        }
        if (isGoal) {
          return reply.status(400).send({
            error: "This performance note is stored as an ERP Goal — update its status in ERPNext Performance.",
          });
        }
        if (ce instanceof ErpError) {
          return reply.status(ce.status >= 500 ? 502 : ce.status).send({ error: String(ce.message) });
        }
        throw ce;
      }

      let payload: Partial<LifecyclePayload> = {};
      try {
        payload = JSON.parse(String(comment.content ?? "{}"));
      } catch {
        /* use empty */
      }

      const current = payload.status ?? "draft";
      const allowed: Record<string, string[]> = {
        draft: ["submitted"],
        submitted: ["approved", "rejected"],
        approved: ["completed"],
        rejected: ["draft"],
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
