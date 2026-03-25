import type { FastifyPluginAsync } from "fastify";
import { resolveHrContext, HttpError } from "../context/resolveHrContext.js";
import { HR_CAPABILITIES_JSON } from "../config.js";
import { buildCapabilitiesForResponse } from "../lib/hrCapabilitiesResponse.js";

/**
 * Feature flags for Pay Hub HR modules. Auth required (same as other /v1 routes).
 * Override per deployment with HR_CAPABILITIES_JSON (partial `{ "data": { ... } }` or bare object).
 */
export const capabilitiesRoutes: FastifyPluginAsync = async (app) => {
  app.get("/v1/capabilities", async (req, reply) => {
    try {
      resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const data = buildCapabilitiesForResponse(HR_CAPABILITIES_JSON);
    return reply.send({
      data,
      meta: {
        bff: "centy-hr-bff",
        routes: ["employees", "employee-invites", "assets", "leaves", "attendance", "expenses", "meta", "health"],
      },
    });
  });
};

