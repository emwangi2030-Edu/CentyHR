/**
 * Employee onboarding — templates and per-employee task tracking.
 *
 * Uses ERPNext's built-in doctypes:
 *   - "Employee Onboarding Template"  (template with activities child table)
 *   - "Employee Onboarding"           (per-employee instance linked to a template)
 *
 * Falls back to Comment-based storage if the doctypes are not installed.
 *
 * Routes:
 *   GET  /v1/onboarding/templates                         — list templates (optionally filter by dept/designation)
 *   POST /v1/onboarding/templates                         — create a new template
 *   GET  /v1/employees/:id/onboarding/tasks               — get active onboarding tasks for an employee
 *   POST /v1/employees/:id/onboarding/start               — assign a template and start onboarding
 *   PATCH /v1/employees/:id/onboarding/tasks/:taskId      — update activity/task status
 */
import type { FastifyPluginAsync } from "fastify";
import { defaultClient, ErpError } from "../erpnext/client.js";
import { resolveHrContext, HttpError } from "../context/resolveHrContext.js";

const erp = defaultClient();

// ── Helper: check if Employee Onboarding Template doctype is available ───────
let _erpOnboardingAvailable: boolean | null = null;
async function erpOnboardingAvailable(creds: Parameters<typeof erp.getList>[0]): Promise<boolean> {
  if (_erpOnboardingAvailable !== null) return _erpOnboardingAvailable;
  try {
    await erp.getList(creds, "Employee Onboarding Template", { fields: ["name"], limit_page_length: 1 });
    _erpOnboardingAvailable = true;
  } catch {
    _erpOnboardingAvailable = false;
  }
  return _erpOnboardingAvailable;
}

// ── Fallback: Comment-based template storage ──────────────────────────────────
const TEMPLATE_COMMENT_DOCTYPE = "HR Settings";
const TEMPLATE_SUBJECT_PREFIX = "[onboarding-template:]";

interface TemplatePayload {
  id: string;
  name: string;
  department?: string;
  designation?: string;
  tasks: Array<{ title: string; description?: string; dueDays?: number }>;
  created_at: string;
}

export const onboardingRoutes: FastifyPluginAsync = async (app) => {

  // ── GET /v1/onboarding/templates ──────────────────────────────────────────
  app.get("/v1/onboarding/templates", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    const q = req.query as Record<string, string>;
    const deptFilter = q.department ?? "";
    const desigFilter = q.designation ?? "";

    const hasErp = await erpOnboardingAvailable(ctx.creds);

    if (hasErp) {
      try {
        const filters: unknown[] = [];
        if (deptFilter) filters.push(["department", "=", deptFilter]);
        if (desigFilter) filters.push(["designation", "=", desigFilter]);

        const rows = await erp.getList(ctx.creds, "Employee Onboarding Template", {
          filters,
          fields: ["name", "template_name", "department", "designation"],
          order_by: "template_name asc",
          limit_page_length: 100,
        });

        const templates = await Promise.all(
          (rows as { name: string; template_name?: string; department?: string; designation?: string }[]).map(async (r) => {
            try {
              const doc = await erp.getDoc(ctx!.creds, "Employee Onboarding Template", r.name) as Record<string, unknown>;
              const activities = Array.isArray(doc.activities)
                ? (doc.activities as Record<string, unknown>[]).map((a) => ({
                    title: String(a.activity_name ?? a.title ?? ""),
                    description: String(a.description ?? ""),
                    dueDays: Number(a.due_days ?? 0) || undefined,
                  }))
                : [];
              return {
                id: r.name,
                name: String(r.template_name ?? r.name),
                department: r.department ?? "",
                designation: r.designation ?? "",
                tasks: activities,
              };
            } catch {
              return { id: r.name, name: String(r.template_name ?? r.name), department: r.department ?? "", designation: r.designation ?? "", tasks: [] };
            }
          })
        );
        return { data: templates };
      } catch (e) {
        if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
        throw e;
      }
    }

    // Fallback: Comment-backed templates
    try {
      const rows = await erp.getList(ctx.creds, "Comment", {
        filters: [
          ["reference_doctype", "=", "HR Settings"],
          ["subject", "like", `${TEMPLATE_SUBJECT_PREFIX}%`],
        ],
        fields: ["name", "content", "creation"],
        order_by: "creation desc",
        limit_page_length: 100,
      });
      const templates = (rows as Record<string, unknown>[]).flatMap((r) => {
        try {
          const p = JSON.parse(String(r.content ?? "{}")) as TemplatePayload;
          if (deptFilter && p.department && p.department !== deptFilter) return [];
          if (desigFilter && p.designation && p.designation !== desigFilter) return [];
          return [{ id: p.id ?? String(r.name), name: p.name ?? "", department: p.department ?? "", designation: p.designation ?? "", tasks: p.tasks ?? [] }];
        } catch { return []; }
      });
      return { data: templates };
    } catch (e) {
      if (e instanceof ErpError) return { data: [] };
      throw e;
    }
  });

  // ── POST /v1/onboarding/templates ─────────────────────────────────────────
  app.post("/v1/onboarding/templates", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    const body = req.body as Record<string, unknown>;
    const templateName = String(body?.name ?? "").trim();
    const department = String(body?.department ?? "").trim();
    const designation = String(body?.designation ?? "").trim();
    const rawTasks = Array.isArray(body?.tasks) ? body.tasks as { title: string; description?: string; dueDays?: number }[] : [];
    const tasks = rawTasks.filter((t) => String(t.title ?? "").trim());

    if (!templateName) return reply.status(400).send({ error: "Template name is required" });
    if (tasks.length === 0) return reply.status(400).send({ error: "At least one task is required" });

    const hasErp = await erpOnboardingAvailable(ctx.creds);

    if (hasErp) {
      try {
        const doc: Record<string, unknown> = {
          template_name: templateName,
          ...(department && { department }),
          ...(designation && { designation }),
          activities: tasks.map((t, i) => ({
            activity_name: t.title,
            description: t.description ?? "",
            due_days: t.dueDays ?? 0,
            idx: i + 1,
          })),
        };
        const created = await erp.createDoc(ctx.creds, "Employee Onboarding Template", doc) as Record<string, unknown>;
        return {
          data: {
            id: String(created.name),
            name: templateName,
            department,
            designation,
            tasks,
          },
        };
      } catch (e) {
        if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
        throw e;
      }
    }

    // Fallback: store as Comment
    const id = `onb-tpl-${Date.now()}`;
    const payload: TemplatePayload = { id, name: templateName, department, designation, tasks, created_at: new Date().toISOString() };
    try {
      await erp.createDoc(ctx.creds, "Comment", {
        reference_doctype: TEMPLATE_COMMENT_DOCTYPE,
        reference_name: TEMPLATE_COMMENT_DOCTYPE,
        comment_type: "Info",
        subject: `${TEMPLATE_SUBJECT_PREFIX}${id}]`,
        content: JSON.stringify(payload),
      });
      return { data: { id, name: templateName, department, designation, tasks } };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });

  // ── GET /v1/employees/:id/onboarding/tasks ────────────────────────────────
  app.get("/v1/employees/:id/onboarding/tasks", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    const employeeId = (req.params as { id: string }).id;
    const hasErp = await erpOnboardingAvailable(ctx.creds);

    if (hasErp) {
      try {
        const onboardings = await erp.getList(ctx.creds, "Employee Onboarding", {
          filters: [["employee", "=", employeeId]],
          fields: ["name", "employee_name", "date_of_joining", "department", "designation", "docstatus"],
          order_by: "creation desc",
          limit_page_length: 10,
        });

        if ((onboardings as unknown[]).length === 0) return { data: [] };

        const latest = (onboardings as { name: string }[])[0];
        const doc = await erp.getDoc(ctx.creds, "Employee Onboarding", latest.name) as Record<string, unknown>;
        const activities = Array.isArray(doc.activities) ? doc.activities as Record<string, unknown>[] : [];

        const tasks = activities.map((a) => ({
          id: String(a.name ?? a.idx ?? ""),
          title: String(a.activity_name ?? ""),
          description: String(a.description ?? "") || null,
          status: String(a.status ?? "Pending").toLowerCase().replace(/ /g, "_"),
          dueDate: a.required_for_employee_creation ? null : null,
          onboardingDoc: latest.name,
          rowName: String(a.name ?? ""),
        }));
        return { data: tasks };
      } catch (e) {
        if (e instanceof ErpError) return { data: [] };
        throw e;
      }
    }

    // Fallback: Comment-backed tasks
    try {
      const rows = await erp.getList(ctx.creds, "Comment", {
        filters: [
          ["reference_doctype", "=", "Employee"],
          ["reference_name", "=", employeeId],
          ["subject", "like", "[onboarding-task:%]"],
        ],
        fields: ["name", "content", "creation"],
        order_by: "creation asc",
        limit_page_length: 200,
      });
      const tasks = (rows as Record<string, unknown>[]).flatMap((r) => {
        try {
          const p = JSON.parse(String(r.content ?? "{}")) as { title: string; description?: string; status: string; dueDate?: string };
          return [{ id: String(r.name), title: p.title ?? "", description: p.description ?? null, status: p.status ?? "pending", dueDate: p.dueDate ?? null, rowName: String(r.name) }];
        } catch { return []; }
      });
      return { data: tasks };
    } catch (e) {
      if (e instanceof ErpError) return { data: [] };
      throw e;
    }
  });

  // ── POST /v1/employees/:id/onboarding/start ───────────────────────────────
  app.post("/v1/employees/:id/onboarding/start", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    const employeeId = (req.params as { id: string }).id;
    const body = req.body as Record<string, unknown>;
    const templateId = String(body?.templateId ?? "").trim();
    if (!templateId) return reply.status(400).send({ error: "templateId is required" });

    let emp: Record<string, unknown>;
    try {
      emp = await erp.getDoc(ctx.creds, "Employee", employeeId) as Record<string, unknown>;
      if (String(emp.company) !== ctx.company) return reply.status(403).send({ error: "Employee not in your company" });
    } catch (e) {
      if (e instanceof ErpError) return reply.status(404).send({ error: "Employee not found" });
      throw e;
    }

    const hasErp = await erpOnboardingAvailable(ctx.creds);

    if (hasErp) {
      try {
        // Fetch template to get activities
        const tmpl = await erp.getDoc(ctx.creds, "Employee Onboarding Template", templateId) as Record<string, unknown>;
        const activities = Array.isArray(tmpl.activities) ? tmpl.activities as Record<string, unknown>[] : [];

        const onboardingDoc = await erp.createDoc(ctx.creds, "Employee Onboarding", {
          employee: employeeId,
          employee_name: emp.employee_name ?? emp.first_name,
          date_of_joining: emp.date_of_joining,
          department: emp.department,
          designation: emp.designation,
          boarding_begins_on: new Date().toISOString().slice(0, 10),
          activities: activities.map((a, i) => ({
            activity_name: a.activity_name,
            description: a.description ?? "",
            due_days: a.due_days ?? 0,
            status: "Pending",
            idx: i + 1,
          })),
        }) as Record<string, unknown>;
        return { data: { onboardingId: String(onboardingDoc.name), status: "started" } };
      } catch (e) {
        if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
        throw e;
      }
    }

    // Fallback: fetch template from Comments and create task Comments on Employee
    try {
      const tmplRows = await erp.getList(ctx.creds, "Comment", {
        filters: [
          ["reference_doctype", "=", TEMPLATE_COMMENT_DOCTYPE],
          ["subject", "like", `${TEMPLATE_SUBJECT_PREFIX}${templateId}]`],
        ],
        fields: ["content"],
        limit_page_length: 1,
      });
      if ((tmplRows as unknown[]).length === 0) return reply.status(404).send({ error: "Template not found" });

      let tmpl: TemplatePayload;
      try { tmpl = JSON.parse(String((tmplRows as Record<string, unknown>[])[0].content ?? "{}")); }
      catch { return reply.status(500).send({ error: "Template data is corrupt" }); }

      await Promise.all((tmpl.tasks ?? []).map((task) =>
        erp.createDoc(ctx!.creds, "Comment", {
          reference_doctype: "Employee",
          reference_name: employeeId,
          comment_type: "Info",
          subject: `[onboarding-task:${templateId}]`,
          content: JSON.stringify({ title: task.title, description: task.description ?? "", status: "pending", dueDate: null }),
        })
      ));
      return { data: { onboardingId: `fallback-${Date.now()}`, status: "started" } };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });

  // ── PATCH /v1/employees/:id/onboarding/tasks/:taskId ─────────────────────
  app.patch("/v1/employees/:id/onboarding/tasks/:taskId", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    const { id: employeeId, taskId } = req.params as { id: string; taskId: string };
    const body = req.body as Record<string, unknown>;
    const newStatus = String(body?.status ?? "").trim();
    const onboardingDoc = String(body?.onboardingDoc ?? "").trim();

    const VALID_TASK_STATUSES = ["pending", "in_progress", "completed", "blocked", "Pending", "In Progress", "Completed"];
    if (!VALID_TASK_STATUSES.some(s => s.toLowerCase() === newStatus.toLowerCase())) {
      return reply.status(400).send({ error: "Invalid status" });
    }

    const hasErp = await erpOnboardingAvailable(ctx.creds);

    if (hasErp && onboardingDoc) {
      try {
        // Update the child table row in the Employee Onboarding doc
        const doc = await erp.getDoc(ctx.creds, "Employee Onboarding", onboardingDoc) as Record<string, unknown>;
        const activities = Array.isArray(doc.activities) ? [...doc.activities as Record<string, unknown>[]] : [];
        const idx = activities.findIndex((a) => String(a.name) === taskId || String(a.idx) === taskId);
        if (idx === -1) return reply.status(404).send({ error: "Task not found" });

        // Map our lowercase status to ERPNext title case
        const erpStatus = newStatus.charAt(0).toUpperCase() + newStatus.slice(1).replace(/_/g, " ");
        activities[idx] = { ...activities[idx], status: erpStatus };
        await erp.updateDoc(ctx.creds, "Employee Onboarding", onboardingDoc, { activities });
        return { data: { taskId, status: newStatus } };
      } catch (e) {
        if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
        throw e;
      }
    }

    // Fallback: update Comment content
    try {
      const comment = await erp.getDoc(ctx.creds, "Comment", taskId) as Record<string, unknown>;
      let payload: Record<string, unknown> = {};
      try { payload = JSON.parse(String(comment.content ?? "{}")); } catch { /* use empty */ }
      payload.status = newStatus;
      await erp.updateDoc(ctx.creds, "Comment", taskId, { content: JSON.stringify(payload) });
      return { data: { taskId, status: newStatus } };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });
};
