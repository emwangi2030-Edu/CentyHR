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
      salutation?: string;
      gender?: string;
      date_of_birth?: string;
      date_of_joining?: string;
      cell_number?: string;
      department?: string;
      designation?: string;
      branch?: string;
    };
    const first_name = String(body.first_name ?? "").trim();
    const last_name = String(body.last_name ?? "").trim();
    if (!first_name) {
      return reply.status(400).send({ error: "first_name is required" });
    }

    const dateRaw = String(body.date_of_joining ?? "").trim();
    const date_of_joining = /^\d{4}-\d{2}-\d{2}$/.test(dateRaw)
      ? dateRaw
      : new Date().toISOString().slice(0, 10);

    // Normalize gender to ERPNext expected values
    const rawGender = String(body.gender ?? "").trim().toLowerCase();
    const gender =
      rawGender === "male" || rawGender === "m" ? "Male" :
      rawGender === "female" || rawGender === "f" ? "Female" :
      rawGender === "other" ? "Other" : "Male"; // ERPNext requires gender; default Male

    // Default DOB to 25 years before DOJ (ERPNext mandatory field)
    const dobRaw = String(body.date_of_birth ?? "").trim();
    const dobDefault = new Date(date_of_joining);
    dobDefault.setFullYear(dobDefault.getFullYear() - 25);
    const date_of_birth = /^\d{4}-\d{2}-\d{2}$/.test(dobRaw)
      ? dobRaw
      : dobDefault.toISOString().slice(0, 10);

    const doc: Record<string, unknown> = {
      company: invite.company_key,
      first_name,
      last_name: last_name || "",
      employee_name: last_name ? `${first_name} ${last_name}`.trim() : first_name,
      date_of_joining,
      date_of_birth,
      gender,
      status: "Active",
      prefered_email: invite.email,
      personal_email: invite.email,
    };

    const str = (v: unknown) => String(v ?? "").trim();
    if (str(body.salutation)) doc.salutation = str(body.salutation);
    if (str(body.cell_number)) doc.cell_number = str(body.cell_number);
    if (str(body.department)) doc.department = str(body.department);
    if (str(body.designation)) doc.designation = str(body.designation);
    if (str(body.branch)) doc.branch = str(body.branch);

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
