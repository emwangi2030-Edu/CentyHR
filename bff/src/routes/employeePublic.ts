import type { FastifyPluginAsync, FastifyReply } from "fastify";
import { defaultClient } from "../erpnext/client.js";
import { ErpError } from "../erpnext/client.js";
import { publicErpFailure } from "../erpnext/frappeResponse.js";
import * as config from "../config.js";
import { getInviteByToken, invitesAvailable, markInviteCompleted } from "../lib/employeeInvites.js";

const erp = defaultClient();

function replyErp(reply: FastifyReply, e: ErpError) {
  const status = e.status >= 500 ? 502 : e.status;
  return reply.status(status).send(publicErpFailure(e));
}

function integrationCreds(): { apiKey: string; apiSecret: string } {
  const k = config.ERP_API_KEY?.trim();
  const s = config.ERP_API_SECRET?.trim();
  if (!k || !s) {
    throw new Error("HR integration keys are not configured on this service");
  }
  return { apiKey: k, apiSecret: s };
}

/**
 * Token-based employee self-onboarding (no Pay Hub session).
 * ERP writes use integration API keys; the invite row in Supabase binds email + company.
 */
export const employeePublicRoutes: FastifyPluginAsync = async (app) => {
  app.get("/v1/public/employee-invite/:token", async (req, reply) => {
    if (!invitesAvailable()) {
      return reply.status(503).send({ error: "Employee invites are not configured on the HR service" });
    }
    const token = String((req.params as { token: string }).token ?? "").trim();
    if (!token) return reply.status(400).send({ error: "Missing token" });

    const invite = await getInviteByToken(token);
    if (!invite) return reply.status(404).send({ error: "Invite not found" });
    if (invite.status !== "pending") {
      return reply.status(410).send({ error: "This invite is no longer valid", code: "INVITE_USED" });
    }
    if (new Date(invite.expires_at) < new Date()) {
      return reply.status(410).send({ error: "This invite has expired", code: "INVITE_EXPIRED" });
    }

    return {
      data: {
        email: invite.email,
        company_key: invite.company_key,
        expires_at: invite.expires_at,
      },
    };
  });

  app.post("/v1/public/employee-invite/:token/complete", async (req, reply) => {
    if (!invitesAvailable()) {
      return reply.status(503).send({ error: "Employee invites are not configured on the HR service" });
    }
    let creds;
    try {
      creds = integrationCreds();
    } catch (e) {
      return reply.status(503).send({ error: String((e as Error).message) });
    }

    const token = String((req.params as { token: string }).token ?? "").trim();
    if (!token) return reply.status(400).send({ error: "Missing token" });

    const invite = await getInviteByToken(token);
    if (!invite) return reply.status(404).send({ error: "Invite not found" });
    if (invite.status !== "pending") {
      return reply.status(410).send({ error: "This invite is no longer valid" });
    }
    if (new Date(invite.expires_at) < new Date()) {
      return reply.status(410).send({ error: "This invite has expired" });
    }

    const body = (req.body ?? {}) as {
      first_name?: string;
      last_name?: string;
      gender?: string;
      date_of_joining?: string;
    };
    const first_name = String(body.first_name ?? "").trim();
    const last_name = String(body.last_name ?? "").trim();
    if (!first_name || !last_name) {
      return reply.status(400).send({ error: "first_name and last_name are required" });
    }

    const dateRaw = String(body.date_of_joining ?? "").trim();
    const date_of_joining = /^\d{4}-\d{2}-\d{2}$/.test(dateRaw)
      ? dateRaw
      : new Date().toISOString().slice(0, 10);
    const gender = String(body.gender ?? "").trim() || undefined;

    const doc: Record<string, unknown> = {
      company: invite.company_key,
      first_name,
      last_name,
      employee_name: `${first_name} ${last_name}`.trim(),
      date_of_joining,
      status: "Active",
      prefered_email: invite.email,
      company_email: invite.email,
    };
    if (gender) doc.gender = gender;

    try {
      const created = await erp.createDoc(creds, "Employee", doc);
      await markInviteCompleted(token);
      const { company: _c, ...data } = created as Record<string, unknown>;
      return { data };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });
};
