/**
 * Recruitment reads + writes: Job Opening list, self-summary, my Job Applicants;
 * Job Requisition (HR); Job Applicant apply / internal interest.
 *
 * Frappe HRMS DocTypes: Job Opening, Job Applicant, Job Requisition, Interview, Job Offer,
 * Interview Type, Interview Feedback.
 */
import type { FastifyPluginAsync, FastifyReply } from "fastify";
import type { ErpCredentials } from "../erpnext/client.js";
import type { HrContext } from "../types.js";
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
function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

async function collectApplicantEmails(
  ctx: { creds: { apiKey: string; apiSecret: string }; userEmail: string },
  self: { name: string } | null
): Promise<string[]> {
  const emails = new Set<string>();
  const ue = ctx.userEmail.trim().toLowerCase();
  if (ue) emails.add(ue);
  if (self) {
    try {
      const doc = await erp.getDoc(ctx.creds, "Employee", self.name);
      for (const k of ["personal_email", "company_email", "prefered_email"] as const) {
        const v = String(doc[k] ?? "")
          .trim()
          .toLowerCase();
        if (v) emails.add(v);
      }
    } catch {
      /* ignore */
    }
  }
  return [...emails];
}

/** Job Applicant rows for the signed-in user (matches `email_id` to login / employee emails). */
async function listMyJobApplicants(ctx: {
  creds: { apiKey: string; apiSecret: string };
  userEmail: string;
  company: string;
}): Promise<Record<string, unknown>[]> {
  const self = await resolveSelfEmployee(ctx);
  const emails = await collectApplicantEmails(ctx, self);
  if (emails.length === 0) return [];
  const orFilters = emails.map((e) => ["email_id", "=", e] as [string, string, string]);
  const rows = await erp.getList(ctx.creds, "Job Applicant", {
    or_filters: orFilters,
    fields: [
      "name",
      "applicant_name",
      "email_id",
      "status",
      "job_title",
      "creation",
      "cover_letter",
      "designation",
    ],
    order_by: "creation desc",
    limit_page_length: 200,
  });
  return rows.map((r) => asRecord(r)).filter((r): r is Record<string, unknown> => r != null);
}

async function jobOpeningTitleMap(
  creds: { apiKey: string; apiSecret: string },
  openingNames: string[]
): Promise<Map<string, string>> {
  const map = new Map<string, string>();
  const uniq = [...new Set(openingNames.map((n) => n.trim()).filter(Boolean))];
  for (const part of chunk(uniq, 40)) {
    try {
      const rows = await erp.getList(creds, "Job Opening", {
        filters: [["name", "in", part]],
        fields: ["name", "job_title"],
        limit_page_length: part.length,
      });
      for (const raw of rows) {
        const rec = asRecord(raw);
        const n = rec?.name;
        if (typeof n === "string" && n) {
          map.set(n, String(rec.job_title ?? n));
        }
      }
    } catch {
      /* ignore */
    }
  }
  return map;
}

async function openingNamesForCompany(
  creds: ErpCredentials,
  companyDocName: string
): Promise<string[]> {
  const rows = await erp.getList(creds, "Job Opening", {
    filters: [["company", "=", companyDocName]],
    fields: ["name"],
    limit_page_length: 500,
  });
  return [...new Set(rows.map((r) => String(asRecord(r)?.name ?? "").trim()).filter(Boolean))];
}

async function listApplicantsForCompanyPipeline(
  ctx: HrContext,
  companyDocName: string
): Promise<Record<string, unknown>[]> {
  const openingIds = await openingNamesForCompany(ctx.creds, companyDocName);
  if (openingIds.length === 0) return [];
  const merged: Record<string, unknown>[] = [];
  const seen = new Set<string>();
  for (const part of chunk(openingIds, 80)) {
    const rows = await erp.getList(ctx.creds, "Job Applicant", {
      filters: [["job_title", "in", part]],
      fields: [
        "name",
        "applicant_name",
        "email_id",
        "status",
        "job_title",
        "creation",
        "designation",
        "phone_number",
      ],
      order_by: "creation desc",
      limit_page_length: 200,
    });
    for (const raw of rows) {
      const rec = asRecord(raw);
      if (!rec) continue;
      const n = String(rec.name ?? "");
      if (!n || seen.has(n)) continue;
      seen.add(n);
      merged.push(rec);
      if (merged.length >= 200) return merged;
    }
  }
  return merged;
}

async function listInterviewsForCompanyOpenings(
  ctx: HrContext,
  openingIds: string[]
): Promise<Record<string, unknown>[]> {
  if (openingIds.length === 0) return [];
  const merged: Record<string, unknown>[] = [];
  const seen = new Set<string>();
  for (const part of chunk(openingIds, 80)) {
    const rows = await erp.getList(ctx.creds, "Interview", {
      filters: [["job_opening", "in", part]],
      fields: [
        "name",
        "job_applicant",
        "job_opening",
        "status",
        "scheduled_on",
        "from_time",
        "to_time",
        "interview_type",
        "creation",
      ],
      order_by: "scheduled_on desc",
      limit_page_length: 200,
    });
    for (const raw of rows) {
      const rec = asRecord(raw);
      if (!rec) continue;
      const n = String(rec.name ?? "");
      if (!n || seen.has(n)) continue;
      seen.add(n);
      merged.push(rec);
      if (merged.length >= 200) return merged;
    }
  }
  return merged;
}

async function pickDefaultInterviewType(creds: ErpCredentials): Promise<string | null> {
  const rows = await erp.getList(creds, "Interview Type", {
    fields: ["name"],
    limit_page_length: 1,
    order_by: "creation asc",
  });
  const n = asRecord(rows[0])?.name;
  return typeof n === "string" && n.trim() ? n.trim() : null;
}

async function interviewAccess(ctx: HrContext, interviewDoc: Record<string, unknown>): Promise<"ok" | "deny"> {
  let companyDocName: string;
  try {
    companyDocName = await resolveCompanyDocName(ctx.creds, ctx.company);
  } catch {
    return "deny";
  }
  const jobApp = String(interviewDoc.job_applicant ?? "").trim();
  if (!jobApp) return "deny";
  let appl: Record<string, unknown>;
  try {
    appl = await erp.getDoc(ctx.creds, "Job Applicant", jobApp);
  } catch {
    return "deny";
  }
  const joLink = String(appl.job_title ?? "").trim();
  if (!joLink) {
    return ctx.canSubmitOnBehalf ? "ok" : "deny";
  }
  try {
    const jo = await erp.getDoc(ctx.creds, "Job Opening", joLink);
    if (String(jo.company) !== companyDocName) return "deny";
  } catch {
    return "deny";
  }
  if (ctx.canSubmitOnBehalf) return "ok";
  const mine = await listMyJobApplicants(ctx);
  if (mine.some((m) => String(m.name) === jobApp)) return "ok";
  return "deny";
}

async function skillAssessmentFromInterviewType(
  creds: ErpCredentials,
  interviewTypeName: string,
  defaultRating: number
): Promise<Array<{ skill: string; rating: number }>> {
  const doc = await erp.getDoc(creds, "Interview Type", interviewTypeName);
  const rawRows = Array.isArray(doc.expected_skill_set) ? doc.expected_skill_set : [];
  const out: Array<{ skill: string; rating: number }> = [];
  for (const raw of rawRows) {
    const row = asRecord(raw);
    const skill = String(row?.skill ?? "").trim();
    if (!skill) continue;
    const r = Number.isFinite(defaultRating) ? Math.min(5, Math.max(1, defaultRating)) : 3;
    out.push({ skill, rating: r });
  }
  return out;
}

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
  /** Aggregate counts for employee portal recruitment cards. */
  app.get("/v1/recruitment/self-summary", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    let companyDocName: string;
    try {
      companyDocName = await resolveCompanyDocName(ctx.creds, ctx.company);
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }

    try {
      const applicants = await listMyJobApplicants(ctx);
      const internalInterest = applicants.filter((a) => {
        const cl = String(a.cover_letter ?? "").toLowerCase();
        return cl.includes("internal mobility") || cl.includes("expressed interest in role");
      });
      const names = applicants.map((a) => String(a.name ?? "").trim()).filter(Boolean).slice(0, 150);

      let interviews = 0;
      let offers = 0;
      if (names.length > 0) {
        try {
          const intRows = await erp.getList(ctx.creds, "Interview", {
            filters: [["job_applicant", "in", names]],
            fields: ["name"],
            limit_page_length: 200,
          });
          interviews = intRows.length;
        } catch {
          interviews = 0;
        }
        try {
          const offRows = await erp.getList(ctx.creds, "Job Offer", {
            filters: [
              ["job_applicant", "in", names],
              ["company", "=", companyDocName],
            ],
            fields: ["name"],
            limit_page_length: 200,
          });
          offers = offRows.length;
        } catch {
          offers = 0;
        }
      }

      let openRoles = 0;
      try {
        const openRows = await erp.getList(ctx.creds, "Job Opening", {
          filters: [
            ["company", "=", companyDocName],
            ["status", "=", "Open"],
          ],
          fields: ["name"],
          limit_page_length: 200,
        });
        openRoles = openRows.length;
      } catch {
        openRoles = 0;
      }

      return {
        data: {
          openRoles,
          applications: applicants.length,
          interviews,
          offers,
          referrals: internalInterest.length,
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** Open Job Openings for the tenant company (Pay Hub may filter external-only for employees). */
  app.get("/v1/recruitment/openings", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    let companyDocName: string;
    try {
      companyDocName = await resolveCompanyDocName(ctx.creds, ctx.company);
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }

    try {
      const rows = await erp.getList(ctx.creds, "Job Opening", {
        filters: [
          ["company", "=", companyDocName],
          ["status", "=", "Open"],
        ],
        fields: ["name", "job_title", "department", "location", "status", "posted_on", "publish"],
        order_by: "posted_on desc",
        limit_page_length: 100,
      });

      const data = rows.map((raw) => {
        const r = asRecord(raw);
        const name = typeof r?.name === "string" ? r.name : "";
        const posted = r?.posted_on != null ? String(r.posted_on) : "";
        return {
          name,
          id: name,
          title: typeof r?.job_title === "string" ? r.job_title : name || "Role",
          department: typeof r?.department === "string" ? r.department : undefined,
          location: typeof r?.location === "string" ? r.location : undefined,
          status: typeof r?.status === "string" ? r.status : undefined,
          postedOn: posted.slice(0, 10) || undefined,
          scope: r?.publish === 1 ? "published" : "internal",
        };
      });

      return { data, meta: { source: "bff" as const } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** Job Applicant rows tied to the current user (for “My internal applications”). */
  app.get("/v1/recruitment/referrals/mine", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    try {
      const applicants = await listMyJobApplicants(ctx);
      const openingNames = applicants
        .map((a) => String(a.job_title ?? "").trim())
        .filter((n) => n.length > 0);
      const titleMap = await jobOpeningTitleMap(ctx.creds, openingNames);

      const data = applicants.map((a) => {
        const name = String(a.name ?? "");
        const jo = String(a.job_title ?? "").trim();
        const roleTitle =
          (jo && titleMap.get(jo)) || String(a.designation ?? "").trim() || jo || "—";
        const created = a.creation != null ? String(a.creation) : "";
        return {
          id: name,
          candidateName: String(a.applicant_name ?? "—"),
          roleTitle,
          status: String(a.status ?? "—"),
          submittedOn: created.slice(0, 10) || undefined,
        };
      });

      return { data, meta: { source: "bff" as const } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** HR: Job Applicants linked to this tenant’s Job Openings (pipeline). */
  app.get("/v1/recruitment/applicants", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR admin privileges required" });
    }
    let companyDocName: string;
    try {
      companyDocName = await resolveCompanyDocName(ctx.creds, ctx.company);
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
    try {
      const rows = await listApplicantsForCompanyPipeline(ctx, companyDocName);
      const openingNames = rows
        .map((a) => String(a.job_title ?? "").trim())
        .filter((n) => n.length > 0);
      const titleMap = await jobOpeningTitleMap(ctx.creds, openingNames);
      const data = rows.map((a) => {
        const jo = String(a.job_title ?? "").trim();
        return {
          name: String(a.name ?? ""),
          applicantName: String(a.applicant_name ?? "—"),
          email: String(a.email_id ?? ""),
          status: String(a.status ?? "—"),
          jobOpening: jo || undefined,
          roleTitle: (jo && titleMap.get(jo)) || String(a.designation ?? "").trim() || jo || "—",
          submittedOn:
            a.creation != null ? String(a.creation).slice(0, 10) : undefined,
        };
      });
      return { data, meta: { source: "bff" as const } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** HR: Interview types (for scheduling UI). */
  app.get("/v1/recruitment/interview-types", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR admin privileges required" });
    }
    try {
      const rows = await erp.getList(ctx.creds, "Interview Type", {
        fields: ["name", "interview_type_name", "designation"],
        limit_page_length: 100,
        order_by: "interview_type_name asc",
      });
      const data = rows.map((raw) => {
        const r = asRecord(raw);
        return {
          name: String(r?.name ?? ""),
          label: String(r?.interview_type_name ?? r?.name ?? ""),
          designation: typeof r?.designation === "string" ? r.designation : undefined,
        };
      });
      return { data, meta: { source: "bff" as const } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** Employee: interviews for Job Applicants tied to the current user. */
  app.get("/v1/recruitment/interviews/mine", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    try {
      const applicants = await listMyJobApplicants(ctx);
      const appNames = applicants.map((a) => String(a.name ?? "").trim()).filter(Boolean);
      if (appNames.length === 0) return { data: [], meta: { source: "bff" as const } };
      const rows: Record<string, unknown>[] = [];
      const seen = new Set<string>();
      for (const part of chunk(appNames, 80)) {
        const batch = await erp.getList(ctx.creds, "Interview", {
          filters: [["job_applicant", "in", part]],
          fields: [
            "name",
            "job_applicant",
            "job_opening",
            "status",
            "scheduled_on",
            "from_time",
            "to_time",
            "interview_type",
          ],
          order_by: "scheduled_on desc",
          limit_page_length: 100,
        });
        for (const raw of batch) {
          const rec = asRecord(raw);
          if (!rec) continue;
          const n = String(rec.name ?? "");
          if (!n || seen.has(n)) continue;
          seen.add(n);
          rows.push(rec);
        }
      }
      const data = rows.map((r) => ({
        id: String(r.name ?? ""),
        applicantName: String(r.job_applicant ?? ""),
        status: String(r.status ?? "—"),
        scheduledOn: r.scheduled_on != null ? String(r.scheduled_on).slice(0, 10) : undefined,
        fromTime: r.from_time != null ? String(r.from_time) : undefined,
        toTime: r.to_time != null ? String(r.to_time) : undefined,
        interviewType: typeof r.interview_type === "string" ? r.interview_type : undefined,
      }));
      return { data, meta: { source: "bff" as const } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** HR: interviews for this tenant’s Job Openings. */
  app.get("/v1/recruitment/interviews", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR admin privileges required" });
    }
    let companyDocName: string;
    try {
      companyDocName = await resolveCompanyDocName(ctx.creds, ctx.company);
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
    try {
      const openingIds = await openingNamesForCompany(ctx.creds, companyDocName);
      const rows = await listInterviewsForCompanyOpenings(ctx, openingIds);
      const data = rows.map((r) => ({
        id: String(r.name ?? ""),
        jobApplicant: String(r.job_applicant ?? ""),
        jobOpening: String(r.job_opening ?? ""),
        status: String(r.status ?? "—"),
        scheduledOn: r.scheduled_on != null ? String(r.scheduled_on).slice(0, 10) : undefined,
        fromTime: r.from_time != null ? String(r.from_time) : undefined,
        toTime: r.to_time != null ? String(r.to_time) : undefined,
        interviewType: typeof r.interview_type === "string" ? r.interview_type : undefined,
      }));
      return { data, meta: { source: "bff" as const } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** Single Interview (HR or owning applicant). */
  app.get("/v1/recruitment/interviews/:name", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const p = req.params as { name?: string };
    const intName = decodeURIComponent(String(p.name ?? "").trim());
    if (!intName) return reply.status(400).send({ error: "Interview name is required" });
    try {
      const doc = await erp.getDoc(ctx.creds, "Interview", intName);
      const gate = await interviewAccess(ctx, doc);
      if (gate !== "ok") return reply.status(403).send({ error: "Not allowed to view this interview" });
      return { data: doc, meta: { source: "bff" as const } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** Interview Feedback rows for an interview (read). */
  app.get("/v1/recruitment/interviews/:name/feedback", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const p = req.params as { name?: string };
    const intName = decodeURIComponent(String(p.name ?? "").trim());
    if (!intName) return reply.status(400).send({ error: "Interview name is required" });
    try {
      const doc = await erp.getDoc(ctx.creds, "Interview", intName);
      const gate = await interviewAccess(ctx, doc);
      if (gate !== "ok") return reply.status(403).send({ error: "Not allowed to view this interview" });
      const rows = await erp.getList(ctx.creds, "Interview Feedback", {
        filters: [["interview", "=", intName]],
        fields: ["name", "interviewer", "result", "feedback", "creation", "average_rating"],
        order_by: "creation desc",
        limit_page_length: 50,
      });
      const data = rows.map((raw) => {
        const r = asRecord(raw);
        return {
          name: String(r?.name ?? ""),
          interviewer: String(r?.interviewer ?? ""),
          result: String(r?.result ?? ""),
          feedback: String(r?.feedback ?? ""),
          submittedOn: r?.creation != null ? String(r.creation).slice(0, 10) : undefined,
          averageRating: r?.average_rating,
        };
      });
      return { data, meta: { source: "bff" as const } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * HR: schedule an Interview (Frappe HRMS).
   * Body: job_applicant (required), scheduled_on (YYYY-MM-DD), interview_type?, from_time?, to_time?
   */
  app.post("/v1/recruitment/interviews", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR admin privileges required" });
    }
    const body = asRecord(req.body) ?? {};
    const jobApplicant = String(body.job_applicant ?? "").trim();
    const scheduledOn = String(body.scheduled_on ?? "").trim();
    if (!jobApplicant || !scheduledOn) {
      return reply.status(400).send({ error: "job_applicant and scheduled_on are required" });
    }
    let companyDocName: string;
    try {
      companyDocName = await resolveCompanyDocName(ctx.creds, ctx.company);
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
    let appl: Record<string, unknown>;
    try {
      appl = await erp.getDoc(ctx.creds, "Job Applicant", jobApplicant);
    } catch (e) {
      if (e instanceof ErpError) return reply.status(404).send({ error: "Job Applicant not found" });
      throw e;
    }
    const joName = String(appl.job_title ?? "").trim();
    if (!joName) {
      return reply.status(400).send({ error: "Applicant must be linked to a Job Opening before scheduling an interview" });
    }
    let jo: Record<string, unknown>;
    try {
      jo = await erp.getDoc(ctx.creds, "Job Opening", joName);
    } catch (e) {
      if (e instanceof ErpError) return reply.status(400).send({ error: "Job Opening not found for applicant" });
      throw e;
    }
    if (String(jo.company) !== companyDocName) {
      return reply.status(403).send({ error: "Applicant opening is not in your company" });
    }
    const interviewType =
      String(body.interview_type ?? "").trim() || (await pickDefaultInterviewType(ctx.creds));
    if (!interviewType) {
      return reply.status(400).send({
        error: "No Interview Type in ERPNext; create one in HR > Interview Type or pass interview_type",
      });
    }
    const fromTime = String(body.from_time ?? "").trim() || "09:00:00";
    const toTime = String(body.to_time ?? "").trim() || "10:00:00";
    const doc: Record<string, unknown> = {
      interview_type: interviewType,
      job_applicant: jobApplicant,
      job_opening: joName,
      scheduled_on: scheduledOn,
      from_time: fromTime,
      to_time: toTime,
      status: "Pending",
    };
    try {
      const created = await erp.createDoc(ctx.creds, "Interview", doc);
      return reply.status(201).send({ data: { name: created.name, doctype: "Interview" } });
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * HR: submit Interview Feedback (builds skill rows from Interview Type).
   * Body: result ("Cleared" | "Rejected"), feedback (text), skill_rating? (1–5 default 3)
   */
  app.post("/v1/recruitment/interviews/:name/feedback", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR admin privileges required" });
    }
    const p = req.params as { name?: string };
    const intName = decodeURIComponent(String(p.name ?? "").trim());
    if (!intName) return reply.status(400).send({ error: "Interview name is required" });
    const body = asRecord(req.body) ?? {};
    const result = String(body.result ?? "").trim();
    if (result !== "Cleared" && result !== "Rejected") {
      return reply.status(400).send({ error: 'result must be "Cleared" or "Rejected"' });
    }
    const feedbackText = String(body.feedback ?? "").trim();
    if (!feedbackText) {
      return reply.status(400).send({ error: "feedback is required" });
    }
    const skillRating = Number(body.skill_rating);
    const defaultRating = Number.isFinite(skillRating) ? skillRating : 3;

    let interviewDoc: Record<string, unknown>;
    try {
      interviewDoc = await erp.getDoc(ctx.creds, "Interview", intName);
    } catch (e) {
      if (e instanceof ErpError) return reply.status(404).send({ error: "Interview not found" });
      throw e;
    }
    const gate = await interviewAccess(ctx, interviewDoc);
    if (gate !== "ok") return reply.status(403).send({ error: "Not allowed to update this interview" });

    const interviewType = String(interviewDoc.interview_type ?? "").trim();
    if (!interviewType) {
      return reply.status(400).send({ error: "Interview has no interview_type" });
    }
    let skillRows: Array<{ skill: string; rating: number }>;
    try {
      skillRows = await skillAssessmentFromInterviewType(ctx.creds, interviewType, defaultRating);
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
    if (skillRows.length === 0) {
      return reply.status(400).send({
        error:
          "Interview Type has no expected skills. Add Expected Skillset in ERPNext (Interview Type) before submitting feedback here.",
      });
    }

    const interviewer = ctx.userEmail.trim();
    try {
      await erp.getDoc(ctx.creds, "User", interviewer);
    } catch {
      return reply.status(400).send({
        error: `No ERPNext User named "${interviewer}". Use an email login that exists as a Frappe User.`,
      });
    }

    const fb: Record<string, unknown> = {
      interview: intName,
      interviewer,
      job_applicant: interviewDoc.job_applicant,
      interview_type: interviewType,
      result,
      feedback: feedbackText,
      skill_assessment: skillRows,
    };

    try {
      const created = await erp.createDoc(ctx.creds, "Interview Feedback", fb);
      const fbName = String(created.name ?? "");
      if (fbName) {
        try {
          await erp.submitDoc(ctx.creds, "Interview Feedback", fbName);
        } catch {
          /* draft may still be usable depending on site workflow */
        }
      }
      return reply.status(201).send({ data: { name: created.name, doctype: "Interview Feedback" } });
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

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
