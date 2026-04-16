/**
 * Recruitment writes (Phase 2): Job Requisition (HR), Job Applicant / internal interest (employee).
 *
 * Frappe HRMS DocTypes: Job Requisition, Job Applicant, Employee Referral (reserved for future).
 */
import type { FastifyPluginAsync, FastifyReply } from "fastify";
import { defaultClient, ErpError } from "../erpnext/client.js";
import { publicErpFailure } from "../erpnext/frappeResponse.js";
import { resolveHrContext, HttpError } from "../context/resolveHrContext.js";
import { resolveCompanyDocName } from "../lib/companyPerformanceMethodology.js";

const erp = defaultClient();

function replyErp(reply: FastifyReply, e: ErpError): FastifyReply {
  const status = e.status >= 500 ? 502 : e.status;
  return reply.status(status).send(publicErpFailure(e));
}

function asRecord(v: unknown): Record<string, unknown> | null {
  return v && typeof v === "object" && !Array.isArray(v) ? (v as Record<string, unknown>) : null;
}

function todayYmd(): string {
  return new Date().toISOString().slice(0, 10);
}

async function resolveSelfEmployee(ctx: {
  creds: { apiKey: string; apiSecret: string };
  userEmail: string;
  company: string;
}): Promise<{ name: string; employee_name?: string } | null> {
  const mine = await erp.listDocs(ctx.creds, "Employee", {
    filters: [
      ["user_id", "=", ctx.userEmail],
      ["company", "=", ctx.company],
    ],
    fields: ["name", "employee_name"],
    limit_page_length: 1,
  });
  const row = asRecord(mine.data?.[0]);
  const name = row?.name;
  if (typeof name !== "string" || !name.trim()) return null;
  return {
    name: name.trim(),
    employee_name: row && typeof row.employee_name === "string" ? row.employee_name : undefined,
  };
}

async function resolveEmployeeInCompany(
  ctx: { creds: { apiKey: string; apiSecret: string }; company: string },
  employeeId: string
): Promise<Record<string, unknown> | null> {
  try {
    const doc = await erp.getDoc(ctx.creds, "Employee", employeeId);
    if (String(doc.company) !== ctx.company) return null;
    return doc;
  } catch (e) {
    if (e instanceof ErpError && (e.status === 404 || e.status === 403)) return null;
    throw e;
  }
}

/** Best-effort: link a Job Opening on this company whose `job_title` matches free text. */
async function guessJobOpeningForRole(
  ctx: { creds: { apiKey: string; apiSecret: string }; company: string },
  companyDocName: string,
  roleTitle: string
): Promise<string | undefined> {
  const q = roleTitle.trim();
  if (!q) return undefined;
  const rows = await erp.getList(ctx.creds, "Job Opening", {
    filters: [
      ["company", "=", companyDocName],
      ["job_title", "=", q],
      ["status", "=", "Open"],
    ],
    fields: ["name"],
    limit_page_length: 2,
  });
  if (!Array.isArray(rows) || rows.length !== 1) return undefined;
  const n = asRecord(rows[0])?.name;
  return typeof n === "string" && n.trim() ? n.trim() : undefined;
}

export const recruitmentRoutes: FastifyPluginAsync = async (app) => {
  /**
   * HR / delegated users: create a Job Requisition (Frappe HRMS).
   * Body: designation (required), no_of_positions, expected_compensation, description,
   * department?, reason_for_requesting?, requested_by? (Employee name), posting_date? (YYYY-MM-DD).
   */
  app.post("/v1/recruitment/requisitions", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR admin privileges required to create a job requisition" });
    }

    const body = asRecord(req.body) ?? {};
    const designation = String(body.designation ?? "").trim();
    if (!designation) {
      return reply.status(400).send({ error: "designation is required", fieldErrors: { designation: "Required" } });
    }

    const noRaw = body.no_of_positions;
    const noOf = typeof noRaw === "number" && Number.isFinite(noRaw) ? Math.max(1, Math.floor(noRaw)) : 1;
    const compRaw = body.expected_compensation;
    const expectedCompensation =
      typeof compRaw === "number" && Number.isFinite(compRaw) ? compRaw : Number.parseFloat(String(compRaw ?? "0")) || 0;
    const description =
      String(body.description ?? "").trim() ||
      `<p>Job requisition for <strong>${designation}</strong> (${noOf} position(s)).</p>`;
    const department = String(body.department ?? "").trim() || undefined;
    const reason = String(body.reason_for_requesting ?? "").trim() || undefined;
    const postingDate = String(body.posting_date ?? "").trim() || todayYmd();

    let companyDocName: string;
    try {
      companyDocName = await resolveCompanyDocName(ctx.creds, ctx.company);
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }

    const requestedByOverride = String(body.requested_by ?? "").trim();
    let requestedBy: string;
    if (requestedByOverride) {
      const empDoc = await resolveEmployeeInCompany(ctx, requestedByOverride);
      if (!empDoc) {
        return reply.status(400).send({
          error: "requested_by must be an Employee in your company",
          fieldErrors: { requested_by: "Invalid employee" },
        });
      }
      requestedBy = requestedByOverride;
    } else {
      const self = await resolveSelfEmployee(ctx);
      if (!self) {
        return reply.status(400).send({
          error: "No Employee row linked to your user; pass requested_by explicitly.",
        });
      }
      requestedBy = self.name;
    }

    const doc: Record<string, unknown> = {
      designation,
      department,
      no_of_positions: noOf,
      expected_compensation: expectedCompensation,
      status: "Pending",
      company: companyDocName,
      requested_by: requestedBy,
      posting_date: postingDate,
      description,
      reason_for_requesting: reason,
    };

    try {
      const created = await erp.createDoc(ctx.creds, "Job Requisition", doc);
      return reply.status(201).send({ data: { name: created.name, doctype: "Job Requisition" } });
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * Employee: apply to a Job Opening or express interest (Job Applicant).
   * Body: job_opening? (name), role_title? (free text when no opening), notes?, applicant_name?, email_id?
   * At least one of job_opening or role_title is required.
   */
  app.post("/v1/recruitment/applications", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const body = asRecord(req.body) ?? {};
    const jobOpening = String(body.job_opening ?? "").trim();
    const roleTitle = String(body.role_title ?? "").trim();
    if (!jobOpening && !roleTitle) {
      return reply.status(400).send({
        error: "Provide job_opening (Job Opening name) and/or role_title",
        fieldErrors: { job_opening: "job_opening or role_title required", role_title: "job_opening or role_title required" },
      });
    }

    const self = await resolveSelfEmployee(ctx);
    if (!self) {
      return reply.status(403).send({ error: "No Employee record linked to your account for this company" });
    }

    let companyDocName: string;
    try {
      companyDocName = await resolveCompanyDocName(ctx.creds, ctx.company);
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }

    const applicantName =
      String(body.applicant_name ?? "").trim() ||
      self.employee_name ||
      self.name;
    const emailId = String(body.email_id ?? "").trim() || ctx.userEmail.trim();
    if (!emailId) {
      return reply.status(400).send({ error: "email_id is required", fieldErrors: { email_id: "Required" } });
    }

    const notes = String(body.notes ?? "").trim();
    let jobTitleLink: string | undefined = jobOpening || undefined;
    if (!jobTitleLink && roleTitle) {
      jobTitleLink = await guessJobOpeningForRole(ctx, companyDocName, roleTitle);
    }

    const coverParts = [
      roleTitle && !jobOpening ? `Expressed interest in role: ${roleTitle}` : "",
      notes,
    ].filter(Boolean);
    const coverLetter = coverParts.join("\n\n").trim() || undefined;

    const ja: Record<string, unknown> = {
      applicant_name: applicantName,
      email_id: emailId,
      phone_number: String(body.phone_number ?? "").trim() || undefined,
      status: "Open",
      job_title: jobTitleLink,
      cover_letter: coverLetter,
    };

    try {
      if (jobOpening) {
        const jo = await erp.getDoc(ctx.creds, "Job Opening", jobOpening);
        if (String(jo.company) !== companyDocName) {
          return reply.status(400).send({ error: "Job Opening is not in your company" });
        }
      }
      const created = await erp.createDoc(ctx.creds, "Job Applicant", ja);
      return reply.status(201).send({ data: { name: created.name, doctype: "Job Applicant" } });
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * Legacy path used by Pay Hub employee portal: internal mobility interest.
   * Creates a **Job Applicant** (same as POST /applications with mapped fields).
   */
  app.post("/v1/recruitment/referrals", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const body = asRecord(req.body) ?? {};
    const roleTitle = String(body.role_title ?? "").trim();
    if (!roleTitle) {
      return reply.status(400).send({ error: "role_title is required", fieldErrors: { role_title: "Required" } });
    }

    const self = await resolveSelfEmployee(ctx);
    if (!self) {
      return reply.status(403).send({ error: "No Employee record linked to your account for this company" });
    }

    let companyDocName: string;
    try {
      companyDocName = await resolveCompanyDocName(ctx.creds, ctx.company);
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }

    const candidateName =
      String(body.candidate_name ?? "").trim() ||
      self.employee_name ||
      self.name;
    const candidateEmail = String(body.candidate_email ?? "").trim() || ctx.userEmail.trim();
    const notes = String(body.notes ?? "").trim();
    const jobOpeningGuess = await guessJobOpeningForRole(ctx, companyDocName, roleTitle);

    const coverParts = [`Internal mobility / role interest: ${roleTitle}`, notes].filter(Boolean);
    const ja: Record<string, unknown> = {
      applicant_name: candidateName,
      email_id: candidateEmail,
      status: "Open",
      job_title: jobOpeningGuess,
      cover_letter: coverParts.join("\n\n"),
    };

    try {
      const created = await erp.createDoc(ctx.creds, "Job Applicant", ja);
      return reply.status(201).send({ data: { name: created.name, doctype: "Job Applicant" } });
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });
};
