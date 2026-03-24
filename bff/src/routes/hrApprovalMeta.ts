/**
 * Read-only HR approval model metadata for UIs (no ERP round-trip for config).
 */
import type { FastifyPluginAsync } from "fastify";
import * as config from "../config.js";
import { resolveHrContext, HttpError } from "../context/resolveHrContext.js";

export const hrApprovalMetaRoutes: FastifyPluginAsync = async (app) => {
  app.get("/v1/meta/hr-approval", async (req, reply) => {
    try {
      resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    return {
      data: {
        model: "two_stage_line_manager_then_hr",
        stages: [
          {
            key: "line_manager",
            label: "Line manager",
            erp: {
              leave_field: "leave_approver",
              expense_field: "expense_approver",
            },
          },
          {
            key: "hr_finance",
            label: "HR / Finance",
            note: "Pay Hub roles forwarded as bridge canHr / appRole",
          },
        ],
        leave: {
          manager_approve_max_days: config.LEAVE_MANAGER_APPROVE_MAX_DAYS,
          two_stage_custom_field: config.LEAVE_TWO_STAGE_APPROVAL,
          first_approver_field: config.LEAVE_TWO_STAGE_APPROVAL ? config.LEAVE_FIRST_APPROVER_FIELD : null,
          hr_bypass_first_approver: config.LEAVE_HR_BYPASS_FIRST_APPROVER,
        },
        expense: {
          approve_ceiling_for_non_finance_note:
            "Per-company in Supabase expense_hub_company_rules.workflow.approve_ceiling_for_non_finance",
          two_stage_custom_field: config.EXPENSE_TWO_STAGE_APPROVAL,
          first_approver_field: config.EXPENSE_TWO_STAGE_APPROVAL ? config.EXPENSE_FIRST_APPROVER_FIELD : null,
          hr_bypass_first_approver: config.EXPENSE_HR_BYPASS_FIRST_APPROVER,
        },
      },
    };
  });
};
