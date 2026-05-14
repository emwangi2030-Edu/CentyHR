import type { FastifyPluginAsync } from "fastify";
import { defaultClient, ErpError } from "../erpnext/client.js";
import { resolveHrContext, HttpError } from "../context/resolveHrContext.js";

const erp = defaultClient();

const CYCLE_PREFIX = "[perf-cycle:]";
const GOAL_PREFIX = "[perf-goal:]";
const CHECKIN_PREFIX = "[perf-checkin:]";

type GoalStatus = "draft" | "active" | "at_risk" | "completed" | "cancelled";

type GoalCycle = {
  id: string;
  name: string;
  start_date: string;
  end_date: string;
  status: "planned" | "active" | "closed";
  created_at: string;
};

type Goal = {
  id: string;
  cycle_id: string;
  employee_id: string;
  title: string;
  description?: string;
  metric_name?: string;
  target_value?: number;
  current_value?: number;
  status: GoalStatus;
  created_at: string;
  updated_at: string;
};

type GoalCheckin = {
  id: string;
  goal_id: string;
  note: string;
  progress_value?: number;
  confidence?: "low" | "medium" | "high";
  created_at: string;
  created_by: string;
};

function ymd(input: unknown): string {
  const s = String(input ?? "").trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : "";
}

function nowIso(): string {
  return new Date().toISOString();
}

function uid(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function parseJson<T>(raw: unknown): T | null {
  try {
    return JSON.parse(String(raw ?? "")) as T;
  } catch {
    return null;
  }
}

async function resolveSelfEmployee(ctx: {
  creds: { apiKey: string; apiSecret: string };
  userEmail: string;
  company: string;
}): Promise<string | null> {
  const mine = await erp.listDocs(ctx.creds, "Employee", {
    filters: [
      ["user_id", "=", ctx.userEmail],
      ["company", "=", ctx.company],
    ],
    fields: ["name"],
    limit_page_length: 1,
  });
  const first = (mine.data?.[0] ?? {}) as Record<string, unknown>;
  const emp = String(first.name ?? "").trim();
  return emp || null;
}

async function listByPrefix<T>(
  creds: { apiKey: string; apiSecret: string },
  company: string,
  prefix: string
): Promise<T[]> {
  const rows = await erp.getList(creds, "Comment", {
    filters: [
      ["reference_doctype", "=", "Company"],
      ["reference_name", "=", company],
      ["subject", "like", `${prefix}%`],
    ],
    fields: ["content"],
    order_by: "creation desc",
    limit_page_length: 500,
  });
  return (rows as Record<string, unknown>[])
    .map((r) => parseJson<T>(r.content))
    .filter((r): r is T => r != null);
}

async function appendRecord(
  creds: { apiKey: string; apiSecret: string },
  company: string,
  subjectPrefix: string,
  id: string,
  payload: Record<string, unknown>
): Promise<void> {
  await erp.createDoc(creds, "Comment", {
    reference_doctype: "Company",
    reference_name: company,
    comment_type: "Info",
    subject: `${subjectPrefix}${id}]`,
    content: JSON.stringify(payload),
  });
}

export const performanceRoutes: FastifyPluginAsync = async (app) => {
  app.get("/v1/performance/goal-cycles", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    try {
      const cycles = await listByPrefix<GoalCycle>(ctx.creds, ctx.company, CYCLE_PREFIX);
      return { data: cycles };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });

  app.post("/v1/performance/goal-cycles", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });
    const body = (req.body ?? {}) as Record<string, unknown>;
    const name = String(body.name ?? "").trim();
    const start_date = ymd(body.start_date);
    const end_date = ymd(body.end_date);
    const status = String(body.status ?? "planned").trim() as GoalCycle["status"];
    if (!name || !start_date || !end_date) {
      return reply.status(400).send({ error: "name, start_date, end_date are required (YYYY-MM-DD)" });
    }
    const id = uid("cycle");
    const payload: GoalCycle = {
      id,
      name,
      start_date,
      end_date,
      status: status === "active" || status === "closed" ? status : "planned",
      created_at: nowIso(),
    };
    try {
      await appendRecord(ctx.creds, ctx.company, CYCLE_PREFIX, id, payload);
      return { data: payload };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });

  app.get("/v1/performance/goals", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const q = (req.query ?? {}) as Record<string, unknown>;
    const cycle_id = String(q.cycle_id ?? "").trim();
    const employeeFilter = String(q.employee ?? "").trim();
    try {
      let goals = await listByPrefix<Goal>(ctx.creds, ctx.company, GOAL_PREFIX);
      if (!ctx.canSubmitOnBehalf) {
        const selfEmp = await resolveSelfEmployee(ctx);
        if (!selfEmp) return reply.status(403).send({ error: "No Employee linked to this user for this Company" });
        goals = goals.filter((g) => g.employee_id === selfEmp);
      } else if (employeeFilter) {
        goals = goals.filter((g) => g.employee_id === employeeFilter);
      }
      if (cycle_id) goals = goals.filter((g) => g.cycle_id === cycle_id);
      return { data: goals };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });

  app.post("/v1/performance/goals", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const body = (req.body ?? {}) as Record<string, unknown>;
    const cycle_id = String(body.cycle_id ?? "").trim();
    const title = String(body.title ?? "").trim();
    if (!cycle_id || !title) return reply.status(400).send({ error: "cycle_id and title are required" });

    let employee_id = String(body.employee_id ?? "").trim();
    if (!employee_id) {
      const selfEmp = await resolveSelfEmployee(ctx);
      if (!selfEmp) return reply.status(403).send({ error: "No Employee linked to this user for this Company" });
      employee_id = selfEmp;
    }
    if (!ctx.canSubmitOnBehalf) {
      const selfEmp = await resolveSelfEmployee(ctx);
      if (!selfEmp || employee_id !== selfEmp) {
        return reply.status(403).send({ error: "You may only create your own goals" });
      }
    }

    const id = uid("goal");
    const current = Number(body.current_value ?? 0);
    const target = Number(body.target_value ?? 0);
    const payload: Goal = {
      id,
      cycle_id,
      employee_id,
      title,
      description: String(body.description ?? "").trim() || undefined,
      metric_name: String(body.metric_name ?? "").trim() || undefined,
      target_value: Number.isFinite(target) ? target : undefined,
      current_value: Number.isFinite(current) ? current : undefined,
      status: "draft",
      created_at: nowIso(),
      updated_at: nowIso(),
    };
    try {
      await appendRecord(ctx.creds, ctx.company, GOAL_PREFIX, id, payload);
      return { data: payload };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });

  app.patch("/v1/performance/goals/:id/status", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const id = String((req.params as { id?: string }).id ?? "").trim();
    const body = (req.body ?? {}) as Record<string, unknown>;
    const status = String(body.status ?? "").trim() as GoalStatus;
    const allowed: GoalStatus[] = ["draft", "active", "at_risk", "completed", "cancelled"];
    if (!id || !allowed.includes(status)) return reply.status(400).send({ error: "valid goal id and status are required" });
    try {
      const goals = await listByPrefix<Goal>(ctx.creds, ctx.company, GOAL_PREFIX);
      const current = goals.find((g) => g.id === id);
      if (!current) return reply.status(404).send({ error: "Goal not found" });
      if (!ctx.canSubmitOnBehalf) {
        const selfEmp = await resolveSelfEmployee(ctx);
        if (!selfEmp || current.employee_id !== selfEmp) {
          return reply.status(403).send({ error: "You may only update your own goals" });
        }
      }
      const next: Goal = { ...current, status, updated_at: nowIso() };
      await appendRecord(ctx.creds, ctx.company, GOAL_PREFIX, id, next);
      return { data: next };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });

  app.post("/v1/performance/goals/:id/checkins", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const goal_id = String((req.params as { id?: string }).id ?? "").trim();
    const body = (req.body ?? {}) as Record<string, unknown>;
    const note = String(body.note ?? "").trim();
    if (!goal_id || !note) return reply.status(400).send({ error: "goal id and note are required" });
    try {
      const goals = await listByPrefix<Goal>(ctx.creds, ctx.company, GOAL_PREFIX);
      const current = goals.find((g) => g.id === goal_id);
      if (!current) return reply.status(404).send({ error: "Goal not found" });
      if (!ctx.canSubmitOnBehalf) {
        const selfEmp = await resolveSelfEmployee(ctx);
        if (!selfEmp || current.employee_id !== selfEmp) {
          return reply.status(403).send({ error: "You may only check in on your own goals" });
        }
      }

      const checkin: GoalCheckin = {
        id: uid("checkin"),
        goal_id,
        note,
        progress_value: Number.isFinite(Number(body.progress_value)) ? Number(body.progress_value) : undefined,
        confidence: ["low", "medium", "high"].includes(String(body.confidence))
          ? (String(body.confidence) as GoalCheckin["confidence"])
          : undefined,
        created_at: nowIso(),
        created_by: ctx.userEmail,
      };

      await appendRecord(ctx.creds, ctx.company, CHECKIN_PREFIX, checkin.id, checkin);

      if (checkin.progress_value != null) {
        const updated: Goal = {
          ...current,
          current_value: checkin.progress_value,
          updated_at: nowIso(),
        };
        await appendRecord(ctx.creds, ctx.company, GOAL_PREFIX, current.id, updated);
      }

      return { data: checkin };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });
};

