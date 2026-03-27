import type { FastifyPluginAsync, FastifyReply } from "fastify";
import { defaultClient, ErpError } from "../erpnext/client.js";
import { publicErpFailure } from "../erpnext/frappeResponse.js";
import { resolveHrContext, HttpError } from "../context/resolveHrContext.js";

const erp = defaultClient();

function replyErp(reply: FastifyReply, e: ErpError) {
  const status = e.status >= 500 ? 502 : e.status;
  return reply.status(status).send(publicErpFailure(e));
}

function asRecord(v: unknown): Record<string, unknown> | null {
  return v && typeof v === "object" && !Array.isArray(v) ? (v as Record<string, unknown>) : null;
}

async function resolveEmployeeIdForUser(ctx: ReturnType<typeof resolveHrContext>): Promise<string | null> {
  const rows = await erp.getList(ctx.creds, "Employee", {
    filters: [
      ["user_id", "=", ctx.userEmail],
      ["company", "=", ctx.company],
    ],
    fields: ["name"],
    limit_page_length: 1,
  });
  const first = asRecord(rows[0]);
  const id = String(first?.name ?? "").trim();
  return id || null;
}

export const loanRoutes: FastifyPluginAsync = async (app) => {
  app.get("/v1/loans/products", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    try {
      const rows = await erp.getList(ctx.creds, "Loan Product", {
        filters: [["company", "=", ctx.company]],
        fields: ["name", "product_name", "rate_of_interest", "is_term_loan", "disabled"],
        order_by: "modified desc",
        limit_page_length: 100,
      });
      return { data: rows };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  app.get("/v1/loans/applications/mine", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    try {
      const employeeId = await resolveEmployeeIdForUser(ctx);
      if (!employeeId) {
        return reply.status(404).send({ error: "No employee profile linked to your account in this company." });
      }
      const rows = await erp.getList(ctx.creds, "Loan Application", {
        filters: [
          ["company", "=", ctx.company],
          ["applicant_type", "=", "Employee"],
          ["applicant", "=", employeeId],
        ],
        fields: ["name", "posting_date", "loan_product", "loan_amount", "status", "repayment_method", "repayment_periods"],
        order_by: "modified desc",
        limit_page_length: 100,
      });
      return { data: rows, meta: { applicant_type: "Employee", applicant: employeeId } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  app.post("/v1/loans/applications", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    try {
      const employeeId = await resolveEmployeeIdForUser(ctx);
      if (!employeeId) {
        return reply.status(404).send({ error: "No employee profile linked to your account in this company." });
      }
      const body = ((req.body ?? {}) as Record<string, unknown>) || {};
      const loanProduct = String(body.loan_product ?? "").trim();
      if (!loanProduct) return reply.status(400).send({ error: "loan_product is required" });
      const loanAmount = Number(body.loan_amount ?? 0);
      if (!Number.isFinite(loanAmount) || loanAmount <= 0) {
        return reply.status(400).send({ error: "loan_amount must be a positive number" });
      }

      const doc: Record<string, unknown> = {
        applicant_type: "Employee",
        applicant: employeeId,
        company: ctx.company,
        loan_product: loanProduct,
        loan_amount: loanAmount,
        posting_date:
          /^\d{4}-\d{2}-\d{2}$/.test(String(body.posting_date ?? "").trim())
            ? String(body.posting_date).trim()
            : new Date().toISOString().slice(0, 10),
      };

      const repaymentMethod = String(body.repayment_method ?? "").trim();
      if (repaymentMethod) doc.repayment_method = repaymentMethod;
      if (body.repayment_periods != null) doc.repayment_periods = Number(body.repayment_periods);
      if (body.repayment_amount != null) doc.repayment_amount = Number(body.repayment_amount);

      const created = await erp.createDoc(ctx.creds, "Loan Application", doc);
      return { data: created, meta: { applicant_type: "Employee", applicant: employeeId } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });
};
