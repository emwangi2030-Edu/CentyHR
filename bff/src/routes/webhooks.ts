import type { FastifyPluginAsync } from "fastify";
import * as appConfig from "../config.js";
import {
  docusealWebhookDedupeKey,
  takeDocusealDuplicate,
  verifyDocusealWebhookHeaders,
} from "../lib/docusealWebhook.js";
import { defaultClient, ErpError } from "../erpnext/client.js";
import { publicErpFailure } from "../erpnext/frappeResponse.js";

const erp = defaultClient();

/**
 * Inbound DocuSeal webhooks (no Pay Hub bridge — verified via shared secret header).
 *
 * Point DocuSeal at: `POST {public-bff}/v1/webhooks/docuseal`
 * Configure the same header name + value in DocuSeal “Add Secret” as env vars here.
 *
 * Forwards the **same JSON** DocuSeal posted to ERP `handle_docuseal_webhook` / `DOCUSEAL_WEBHOOK_ERP_METHOD`
 * (default `centypay_dms.api.documents.handle_docuseal_webhook` on erp.tarakilishicloud.com).
 */
export const webhooksRoutes: FastifyPluginAsync = async (app) => {
  app.post("/v1/webhooks/docuseal", async (req, reply) => {
    const v = verifyDocusealWebhookHeaders(req.headers);
    if (!v.ok) {
      return reply.status(v.status).send({ error: v.error });
    }

    const body = req.body;
    if (body === null || body === undefined || typeof body !== "object" || Array.isArray(body)) {
      return reply.status(400).send({ error: "JSON object body required" });
    }

    const dedupeKey = docusealWebhookDedupeKey(body);
    if (takeDocusealDuplicate(dedupeKey)) {
      req.log.info({ dedupeKey }, "docuseal webhook duplicate delivery skipped");
      return { ok: true, duplicate: true };
    }

    const { ERP_API_KEY, ERP_API_SECRET, DOCUSEAL_WEBHOOK_ERP_METHOD } = appConfig;
    if (!ERP_API_KEY || !ERP_API_SECRET) {
      return reply.status(503).send({ error: "ERP_API_KEY / ERP_API_SECRET not configured on BFF" });
    }

    try {
      const result = await erp.callMethod(
        { apiKey: ERP_API_KEY, apiSecret: ERP_API_SECRET },
        DOCUSEAL_WEBHOOK_ERP_METHOD,
        body as Record<string, unknown>
      );
      return { ok: true, result };
    } catch (e) {
      if (e instanceof ErpError) {
        const status = e.status >= 500 ? 502 : e.status;
        return reply.status(status).send(publicErpFailure(e));
      }
      throw e;
    }
  });
};
