/**
 * Kenya / Frappe HRMS payroll — read-model layer for Pay Hub hybrid flow.
 *
 * - All statutory math (PAYE, NSSF, SHIF, AHL, …) lives in ERP salary structures + CSF KE — never here.
 * - Salary Slip / Payroll Entry are loaded from Frappe; field sets vary by site (v15 HRMS).
 */
import type { FastifyPluginAsync, FastifyReply } from "fastify";
import type { HrContext } from "../types.js";
import { defaultClient } from "../erpnext/client.js";
import { ErpError } from "../erpnext/client.js";
import { publicErpFailure } from "../erpnext/frappeResponse.js";
import { resolveHrContext, HttpError } from "../context/resolveHrContext.js";

const erp = defaultClient();

function replyErp(reply: FastifyReply, e: ErpError): FastifyReply {
  const status = e.status >= 500 ? 502 : e.status;
  return reply.status(status).send(publicErpFailure(e));
}

function parseDate(v: unknown): string {
  const s = String(v ?? "").trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : "";
}

export const payrollRoutes: FastifyPluginAsync = async (app) => {
  async function resolveSelfEmployee(ctx: HrContext): Promise<string | null> {
    for (const field of ["user_id", "personal_email", "prefered_email"] as const) {
      const mine = await erp.listDocs(ctx.creds, "Employee", {
        filters: [[field, "=", ctx.userEmail], ["company", "=", ctx.company]],
        fields: ["name"],
        limit_page_length: 1,
      });
      const row = mine.data?.[0];
      const name = row && typeof (row as { name?: unknown }).name === "string"
        ? String((row as { name: string }).name)
        : null;
      if (name) return name;
    }
    return null;
  }

  async function resolveEmployeeIdForRequest(ctx: HrContext, qEmp: string): Promise<string | null> {
    if (ctx.canSubmitOnBehalf) {
      if (!qEmp) return null;
      const empDoc = await erp.getDoc(ctx.creds, "Employee", qEmp);
      if (String(empDoc.company) !== ctx.company) return null;
      return qEmp;
    }
    return resolveSelfEmployee(ctx);
  }

  /**
   * HR: salary slips whose pay period overlaps [from_date, to_date].
   * Query: from_date, to_date (YYYY-MM-DD), optional employee (Employee name).
   */
  app.get("/v1/payroll/salary-slips", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "Only HR can list salary slips for the company." });
    }

    const q = (req.query ?? {}) as Record<string, unknown>;
    const qEmp = String(q.employee ?? "").trim();
    const from = parseDate(q.from_date ?? q.from);
    const to = parseDate(q.to_date ?? q.to);
    if (!from || !to) return reply.status(400).send({ error: "from_date and to_date are required (YYYY-MM-DD)" });

    try {
      const employeeId = qEmp ? await resolveEmployeeIdForRequest(ctx, qEmp) : null;
      if (qEmp && !employeeId) return reply.status(403).send({ error: "Employee not in your Company" });

      const filters: unknown[] = [
        ["company", "=", ctx.company],
        ["docstatus", "!=", 2],
        ["start_date", "<=", to],
        ["end_date", ">=", from],
      ];
      if (employeeId) filters.push(["employee", "=", employeeId]);

      const rows = (await erp.getList(ctx.creds, "Salary Slip", {
        fields: [
          "name",
          "employee",
          "employee_name",
          "company",
          "posting_date",
          "start_date",
          "end_date",
          "currency",
          "status",
          "docstatus",
        ],
        filters,
        order_by: "start_date desc, employee asc",
        limit_page_length: 200,
      })) as Record<string, unknown>[];

      return { data: rows };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** HR: single salary slip (full doc as ERP returns it). */
  app.get("/v1/payroll/salary-slips/:name", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "Only HR can view salary slip detail." });
    }

    const params = (req.params ?? {}) as Record<string, unknown>;
    const name = String(params.name ?? "").trim();
    if (!name) return reply.status(400).send({ error: "Salary Slip name is required" });

    try {
      const doc = await erp.getDoc(ctx.creds, "Salary Slip", name);
      if (String(doc.company ?? "") !== ctx.company) {
        return reply.status(403).send({ error: "Salary Slip not in your Company" });
      }
      return { data: doc };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * HR: payroll entry documents (runs) overlapping the date range.
   */
  app.get("/v1/payroll/payroll-entries", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "Only HR can list payroll entries." });
    }

    const q = (req.query ?? {}) as Record<string, unknown>;
    const from = parseDate(q.from_date ?? q.from);
    const to = parseDate(q.to_date ?? q.to);
    if (!from || !to) return reply.status(400).send({ error: "from_date and to_date are required (YYYY-MM-DD)" });

    try {
      const filters: unknown[] = [
        ["company", "=", ctx.company],
        ["docstatus", "!=", 2],
        ["start_date", "<=", to],
        ["end_date", ">=", from],
      ];

      const rows = (await erp.getList(ctx.creds, "Payroll Entry", {
        fields: [
          "name",
          "company",
          "posting_date",
          "start_date",
          "end_date",
          "currency",
          "exchange_rate",
          "payroll_frequency",
          "status",
          "docstatus",
        ],
        filters,
        order_by: "start_date desc",
        limit_page_length: 100,
      })) as Record<string, unknown>[];

      return { data: rows };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });
};
