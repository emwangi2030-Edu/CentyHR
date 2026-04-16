import type { FastifyPluginAsync } from "fastify";
import { resolveHrContext, HttpError } from "../context/resolveHrContext.js";
import { HR_CAPABILITIES_JSON } from "../config.js";
import { buildCapabilitiesForResponse } from "../lib/hrCapabilitiesResponse.js";
import { readPerformanceMethodology } from "../lib/companyPerformanceMethodology.js";

/**
 * Feature flags for Pay Hub HR modules. Auth required (same as other /v1 routes).
 * Override per deployment with HR_CAPABILITIES_JSON (partial `{ "data": { ... } }` or bare object).
 */
export const capabilitiesRoutes: FastifyPluginAsync = async (app) => {
  app.get("/v1/capabilities", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const base = buildCapabilitiesForResponse(HR_CAPABILITIES_JSON);
    let methodology: "bsc" | "okr" = "bsc";
    try {
      methodology = await readPerformanceMethodology(ctx.creds, ctx.company);
    } catch {
      /* keep default */
    }
    const data = {
      ...base,
      performance: {
        enabled: true,
        goals: true,
        selfAppraisal: true,
        methodology,
      },
    };
    // sendInvite stays true even when Supabase isn't configured — the button is always shown
    // and the error is surfaced at invite-creation time with a clear message instead of hiding the feature.
    return reply.send({
      data,
      meta: {
        bff: "centy-hr-bff",
        routes: ["employees", "employee-invites", "assets", "leaves", "attendance", "payroll", "expenses", "meta", "health"],
      },
    });
  });
};

