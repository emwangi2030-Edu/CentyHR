/**
 * Recruitment reads + writes: Job Opening list, self-summary, my Job Applicants;
 * Job Requisition (HR); Job Applicant apply / internal interest; Interview pipeline (HR + employee reads).
 *
 * Frappe HRMS DocTypes: Job Opening, Job Applicant, Job Requisition, Interview, Interview Type,
 * Interview Feedback, Job Offer.
 */
import type { FastifyPluginAsync, FastifyReply } from "fastify";
import { defaultClient, ErpError, type ErpCredentials } from "../erpnext/client.js";
import { publicErpFailure } from "../erpnext/frappeResponse.js";
import { resolveHrContext, HttpError } from "../context/resolveHrContext.js";
import { resolveCompanyDocName } from "../lib/companyPerformanceMethodology.js";
import type { HrContext } from "../types.js";

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

async function openingNamesForCompany(ctx: {
  creds: ErpCredentials;
  company: string;
}): Promise<{ companyDocName: string; openingNames: Set<string> }> {
  const companyDocName = await resolveCompanyDocName(ctx.creds, ctx.company);
  const rows = await erp.getList(ctx.creds, "Job Opening", {
    filters: [["company", "=", companyDocName]],
    fields: ["name"],
    limit_page_length: 500,
  });
  const openingNames = new Set<string>();
  for (const raw of rows) {
    const r = asRecord(raw);
    const n = typeof r?.name === "string" ? r.name.trim() : "";
    if (n) openingNames.add(n);
  }
  return { companyDocName, openingNames };
}

/** Job Applicants whose `job_title` links to a Job Opening on this company. */
async function listApplicantsForCompanyPipeline(
  ctx: { creds: ErpCredentials; company: string },
  openingNames: Set<string>
): Promise<Record<string, unknown>[]> {
  if (openingNames.size === 0) return [];
  const names = [...openingNames];
  const merged: Record<string, unknown>[] = [];
  for (const part of chunk(names, 50)) {
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
      ],
      order_by: "creation desc",
      limit_page_length: 200,
    });
    for (const raw of rows) {
      const rec = asRecord(raw);
      if (rec) merged.push(rec);
    }
  }
  return merged;
}

async function listInterviewsForTenantOpenings(
  creds: ErpCredentials,
  openingNames: Set<string>
): Promise<Record<string, unknown>[]> {
  if (openingNames.size === 0) return [];
  const names = [...openingNames];
  const merged: Record<string, unknown>[] = [];
  for (const part of chunk(names, 40)) {
    const rows = await erp.getList(creds, "Interview", {
      filters: [["job_opening", "in", part]],
      fields: [
        "name",
        "job_applicant",
        "job_opening",
        "status",
        "scheduled_on",
        "from_time",
        "to_time",
        "creation",
      ],
      order_by: "scheduled_on desc,creation desc",
      limit_page_length: 200,
    });
    for (const raw of rows) {
      const rec = asRecord(raw);
      if (rec) merged.push(rec);
    }
  }
  return merged;
}

async function listInterviewsForApplicants(
  creds: ErpCredentials,
  applicantNames: string[]
): Promise<Record<string, unknown>[]> {
  const uniq = [...new Set(applicantNames.map((n) => n.trim()).filter(Boolean))].slice(0, 150);
  if (uniq.length === 0) return [];
  const merged: Record<string, unknown>[] = [];
  for (const part of chunk(uniq, 40)) {
    const rows = await erp.getList(creds, "Interview", {
      filters: [["job_applicant", "in", part]],
      fields: [
        "name",
        "job_applicant",
        "job_opening",
        "status",
        "scheduled_on",
        "from_time",
        "to_time",
        "creation",
      ],
      order_by: "scheduled_on desc,creation desc",
      limit_page_length: 200,
    });
    for (const raw of rows) {
      const rec = asRecord(raw);
      if (rec) merged.push(rec);
    }
  }
  return merged;
}

async function listJobOffersForCompany(
  creds: ErpCredentials,
  companyDocName: string
): Promise<Record<string, unknown>[]> {
  const rows = await erp.getList(creds, "Job Offer", {
    filters: [["company", "=", companyDocName]],
    fields: [
      "name",
      "job_applicant",
      "applicant_name",
      "applicant_email",
      "status",
      "offer_date",
      "designation",
      "creation",
    ],
    order_by: "offer_date desc,creation desc",
    limit_page_length: 300,
  });
  return rows.map((r) => asRecord(r)).filter((r): r is Record<string, unknown> => r != null);
}

function mapJobOfferRow(r: Record<string, unknown>): Record<string, unknown> {
  const offerDate = r.offer_date != null ? String(r.offer_date) : "";
  const created = r.creation != null ? String(r.creation) : "";
  return {
    id: String(r.name ?? "").trim(),
    jobApplicant: String(r.job_applicant ?? "").trim() || undefined,
    applicantName: String(r.applicant_name ?? "").trim() || undefined,
    applicantEmail: String(r.applicant_email ?? "").trim() || undefined,
    status: String(r.status ?? "").trim() || undefined,
    offerDate: offerDate.slice(0, 10) || undefined,
    designation: String(r.designation ?? "").trim() || undefined,
    createdOn: created.slice(0, 10) || undefined,
    /** Full Job Offer `creation` for time metrics (ISO-like / Frappe datetime string). */
    createdAt: created.trim() || undefined,
    /** Raw `offer_date` when present (date-only string). */
    offerDateRaw: offerDate.trim() || undefined,
  };
}

async function assertJobOfferReadable(
  ctx: HrContext,
  offerName: string,
  openingNames: Set<string>
): Promise<Record<string, unknown>> {
  const trimmed = offerName.trim();
  if (!trimmed) throw new HttpError("Offer name is required", 400);
  let doc: Record<string, unknown>;
  try {
    doc = await erp.getDoc(ctx.creds, "Job Offer", trimmed);
  } catch (e) {
    if (e instanceof ErpError && (e.status === 404 || e.status === 403)) {
      throw new HttpError("Offer not found", 404);
    }
    throw e;
  }
  const applicantId = String(doc.job_applicant ?? "").trim();
  if (!applicantId) throw new HttpError("Invalid offer record", 400);

  if (ctx.canSubmitOnBehalf) {
    const applicant = await erp.getDoc(ctx.creds, "Job Applicant", applicantId);
    const jt = String(applicant.job_title ?? "").trim();
    if (!openingNames.has(jt)) throw new HttpError("Offer is outside your company's pipeline", 403);
    return doc;
  }

  const mine = await listMyJobApplicants(ctx);
  const allowed = new Set(mine.map((a) => String(a.name ?? "").trim()).filter(Boolean));
  if (!allowed.has(applicantId)) throw new HttpError("Not allowed to view this offer", 403);
  return doc;
}

async function pickDefaultInterviewType(creds: ErpCredentials): Promise<string> {
  const rows = await erp.getList(creds, "Interview Type", {
    fields: ["name"],
    order_by: "creation asc",
    limit_page_length: 1,
  });
  const r = asRecord(rows[0]);
  const n = typeof r?.name === "string" ? r.name.trim() : "";
  return n;
}

/** Frappe Rating control is 0–1; UI sends 1–5 stars. */
function ratingFromStars(stars: number): number {
  if (!Number.isFinite(stars)) return 0.6;
  return Math.max(0, Math.min(1, stars / 5));
}

async function skillAssessmentFromInterviewType(
  creds: ErpCredentials,
  interviewTypeName: string,
  skillStars: number
): Promise<Array<{ skill: string; rating: number }>> {
  const doc = await erp.getDoc(creds, "Interview Type", interviewTypeName);
  const raw = doc.expected_skill_set;
  const rating = ratingFromStars(skillStars);
  if (!Array.isArray(raw) || raw.length === 0) {
    return [{ skill: "General Assessment", rating }];
  }
  const out: Array<{ skill: string; rating: number }> = [];
  for (const row of raw) {
    const rec = asRecord(row);
    const skill = typeof rec?.skill === "string" ? rec.skill.trim() : "";
    if (!skill) continue;
    out.push({ skill, rating });
  }
  if (out.length === 0) {
    return [{ skill: "General Assessment", rating }];
  }
  return out;
}

async function assertInterviewReadable(
  ctx: HrContext,
  interviewName: string,
  openingNames: Set<string>
): Promise<Record<string, unknown>> {
  const trimmed = interviewName.trim();
  if (!trimmed) throw new HttpError("Interview name is required", 400);
  let doc: Record<string, unknown>;
  try {
    doc = await erp.getDoc(ctx.creds, "Interview", trimmed);
  } catch (e) {
    if (e instanceof ErpError && (e.status === 404 || e.status === 403)) {
      throw new HttpError("Interview not found", 404);
    }
    throw e;
  }
  const ja = String(doc.job_applicant ?? "").trim();
  if (!ja) throw new HttpError("Invalid interview record", 400);

  if (ctx.canSubmitOnBehalf) {
    const jo = String(doc.job_opening ?? "").trim();
    if (jo && openingNames.has(jo)) return doc;
    let applicant: Record<string, unknown>;
    try {
      applicant = await erp.getDoc(ctx.creds, "Job Applicant", ja);
    } catch (e) {
      if (e instanceof ErpError && (e.status === 404 || e.status === 403)) {
        throw new HttpError("Interview is outside your company's pipeline", 403);
      }
      throw e;
    }
    const jt = String(applicant.job_title ?? "").trim();
    if (!openingNames.has(jt)) {
      throw new HttpError("Interview is outside your company's pipeline", 403);
    }
    return doc;
  }

  const mine = await listMyJobApplicants(ctx);
  const allowed = new Set(mine.map((a) => String(a.name ?? "").trim()).filter(Boolean));
  if (!allowed.has(ja)) throw new HttpError("Not allowed to view this interview", 403);
  return doc;
}

function mapInterviewListRow(r: Record<string, unknown>): Record<string, unknown> {
  const id = String(r.name ?? "").trim();
  const scheduled = r.scheduled_on != null ? String(r.scheduled_on) : "";
  const created = r.creation != null ? String(r.creation) : "";
  const interviewType = String(r.interview_type_name ?? r.interview_type ?? "").trim();
  const fromTime = r.from_time != null ? String(r.from_time) : undefined;
  return {
    id,
    jobApplicant: String(r.job_applicant ?? "").trim() || undefined,
    jobOpening: String(r.job_opening ?? "").trim() || undefined,
    status: String(r.status ?? "").trim() || undefined,
    scheduledOn: scheduled.slice(0, 10) || undefined,
    interviewType: interviewType || undefined,
    fromTime,
    toTime: r.to_time != null ? String(r.to_time) : undefined,
    /** Interview `creation` for ordering / fallbacks. */
    interviewCreatedAt: created.trim() || undefined,
  };
}

/** Child rows on Interview → Interview Detail (`interviewer` = Frappe User name). */
function interviewersFromInterviewDoc(doc: Record<string, unknown>): string[] {
  const det = doc.interview_details;
  if (!Array.isArray(det)) return [];
  const out: string[] = [];
  for (const row of det) {
    const r = asRecord(row);
    const iv = typeof r?.interviewer === "string" ? r.interviewer.trim() : "";
    if (iv) out.push(iv);
  }
  return out;
}

function parseInterviewerCandidates(body: Record<string, unknown>): string[] {
  const raw = body.interviewers;
  if (Array.isArray(raw)) {
    return raw.map((x) => String(x ?? "").trim()).filter(Boolean);
  }
  const one = String(body.interviewer ?? "").trim();
  if (one) return [one];
  const csv = String(body.interviewer_emails ?? "").trim();
  if (!csv) return [];
  return csv
    .split(/[\n,;\t]+/)
    .map((s) => s.trim())
    .filter(Boolean);
}

/** Deduplicate by lower-case while preserving first-seen casing. */
function uniqueEmailsPreserveOrder(candidates: string[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const c of candidates) {
    const k = c.trim().toLowerCase();
    if (!k || seen.has(k)) continue;
    seen.add(k);
    out.push(c.trim());
  }
  return out;
}

async function resolveFrappeInterviewerUserNames(
  creds: ErpCredentials,
  candidates: string[]
): Promise<string[]> {
  const ordered = uniqueEmailsPreserveOrder(candidates);
  const resolved: string[] = [];
  const missing: string[] = [];
  for (const c of ordered) {
    try {
      const u = await erp.getDoc(creds, "User", c);
      const nm = String(u.name ?? "").trim();
      if (nm) resolved.push(nm);
      else missing.push(c);
    } catch {
      missing.push(c);
    }
  }
  if (missing.length > 0) {
    throw new HttpError(
      `Each interviewer must be an existing Frappe User (name is usually the login email). Not found: ${missing.join(", ")}`,
      400
    );
  }
  return resolved;
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

function normKey(s: string): string {
  return s.trim().toLowerCase();
}

function weekStartMondayUtc(d: Date): string {
  const x = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  const day = x.getUTCDay();
  const diff = (day + 6) % 7;
  x.setUTCDate(x.getUTCDate() - diff);
  return x.toISOString().slice(0, 10);
}

function parseYmdToUtcDate(s: string | undefined | null): Date | null {
  if (s == null || typeof s !== "string") return null;
  const t = s.trim().slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(t)) return null;
  const d = new Date(`${t}T12:00:00.000Z`);
  return Number.isNaN(d.getTime()) ? null : d;
}

function last12WeekStarts(): string[] {
  const weeks: string[] = [];
  const anchor = weekStartMondayUtc(new Date());
  const anchorD = parseYmdToUtcDate(anchor);
  if (!anchorD) return weeks;
  for (let i = 11; i >= 0; i--) {
    const d = new Date(anchorD);
    d.setUTCDate(d.getUTCDate() - i * 7);
    weeks.push(weekStartMondayUtc(d));
  }
  return weeks;
}

type JobOpeningMeta = { name: string; department: string; jobTitle: string };

async function listJobOpeningMetaForCompany(
  creds: ErpCredentials,
  companyDocName: string
): Promise<JobOpeningMeta[]> {
  const rows = await erp.getList(creds, "Job Opening", {
    filters: [["company", "=", companyDocName]],
    fields: ["name", "department", "job_title"],
    limit_page_length: 500,
  });
  const out: JobOpeningMeta[] = [];
  for (const raw of rows) {
    const r = asRecord(raw);
    if (!r) continue;
    const name = String(r.name ?? "").trim();
    if (!name) continue;
    out.push({
      name,
      department: String(r.department ?? "").trim(),
      jobTitle: String(r.job_title ?? "").trim() || name,
    });
  }
  return out;
}

async function listInterviewFeedbackForInterviews(
  creds: ErpCredentials,
  interviewNames: string[]
): Promise<Record<string, unknown>[]> {
  const uniq = [...new Set(interviewNames.map((n) => n.trim()).filter(Boolean))];
  if (uniq.length === 0) return [];
  const merged: Record<string, unknown>[] = [];
  for (const part of chunk(uniq, 50)) {
    const rows = await erp.getList(creds, "Interview Feedback", {
      filters: [["interview", "in", part]],
      fields: ["name", "interview", "interviewer", "result", "creation"],
      order_by: "creation desc",
      limit_page_length: 200,
    });
    for (const raw of rows) {
      const rec = asRecord(raw);
      if (rec) merged.push(rec);
    }
  }
  return merged;
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

  /** Admin outcome reporting summary for recruitment + performance. */
  app.get("/v1/outcomes/reports/summary", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR admin privileges required to view outcome reports" });
    }
    try {
      const { companyDocName, openingNames } = await openingNamesForCompany(ctx);
      const applicants = await listApplicantsForCompanyPipeline(ctx, openingNames);
      const applicantNames = applicants.map((a) => String(a.name ?? "").trim()).filter(Boolean);

      const interviews = await listInterviewsForTenantOpenings(ctx.creds, openingNames);
      const interviewNames = interviews.map((r) => String(r.name ?? "").trim()).filter(Boolean);
      let feedbackSubmitted = 0;
      if (interviewNames.length > 0) {
        for (const part of chunk(interviewNames, 50)) {
          const rows = await erp.getList(ctx.creds, "Interview Feedback", {
            filters: [["interview", "in", part]],
            fields: ["name"],
            limit_page_length: 200,
          });
          feedbackSubmitted += rows.length;
        }
      }

      const offers = await listJobOffersForCompany(ctx.creds, companyDocName);
      const offersAwaiting = offers.filter((r) => String(r.status ?? "").trim() === "Awaiting Response").length;
      const offersAccepted = offers.filter((r) => String(r.status ?? "").trim() === "Accepted").length;
      const offersRejected = offers.filter((r) => String(r.status ?? "").trim() === "Rejected").length;

      let goalsMine = 0;
      try {
        const self = await resolveSelfEmployee(ctx);
        if (self?.name) {
          const goalRows = await erp.getList(ctx.creds, "Goal", {
            filters: [["employee", "=", self.name]],
            fields: ["name"],
            limit_page_length: 300,
          });
          goalsMine = goalRows.length;
        }
      } catch {
        goalsMine = 0;
      }

      let appraisalsMine = 0;
      try {
        const self = await resolveSelfEmployee(ctx);
        if (self?.name) {
          const appraisalRows = await erp.getList(ctx.creds, "Appraisal", {
            filters: [["employee", "=", self.name]],
            fields: ["name"],
            limit_page_length: 300,
          });
          appraisalsMine = appraisalRows.length;
        }
      } catch {
        appraisalsMine = 0;
      }

      let performanceSelfSummary: Record<string, unknown> | null = null;
      try {
        const self = await resolveSelfEmployee(ctx);
        if (self?.name) {
          performanceSelfSummary = {
            goals: goalsMine,
            appraisals: appraisalsMine,
          };
        }
      } catch {
        performanceSelfSummary = null;
      }

      return {
        data: {
          recruitment: {
            applicants: applicantNames.length,
            interviews: interviewNames.length,
            feedbackSubmitted,
            offersTotal: offers.length,
            offersAwaiting,
            offersAccepted,
            offersRejected,
          },
          performance: {
            goalsMine,
            appraisalsMine,
            selfSummary: performanceSelfSummary,
          },
        },
        meta: { source: "bff" as const },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * Recruitment outcome analytics: optional filters (department, designation, job opening),
   * 12-week trend buckets, feedback breakdown, interviewer counts, offer acceptance on filtered set.
   */
  app.get("/v1/outcomes/reports/recruitment-analytics", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR admin privileges required to view outcome reports" });
    }

    const q = asRecord(req.query as unknown) ?? {};
    const filterDepartment = String(q.department ?? "").trim();
    const filterDesignation = String(q.designation ?? "").trim();
    const filterJobOpening = String(q.job_opening ?? "").trim();

    try {
      const companyDocName = await resolveCompanyDocName(ctx.creds, ctx.company);
      const openingMeta = await listJobOpeningMetaForCompany(ctx.creds, companyDocName);

      let scopedOpenings = openingMeta;
      if (filterJobOpening) {
        scopedOpenings = scopedOpenings.filter((o) => o.name === filterJobOpening || normKey(o.name) === normKey(filterJobOpening));
      }
      if (filterDepartment) {
        const dk = normKey(filterDepartment);
        scopedOpenings = scopedOpenings.filter((o) => normKey(o.department) === dk);
      }

      const openingNames = new Set(scopedOpenings.map((o) => o.name));
      const applicantsAll = await listApplicantsForCompanyPipeline(ctx, openingNames);
      const desigKey = filterDesignation ? normKey(filterDesignation) : "";
      const applicants = desigKey
        ? applicantsAll.filter((a) => normKey(String(a.designation ?? "")) === desigKey)
        : applicantsAll;

      const applicantIdSet = new Set(
        applicants.map((a) => String(a.name ?? "").trim()).filter(Boolean),
      );

      const interviewsAll = await listInterviewsForTenantOpenings(ctx.creds, openingNames);
      const interviews =
        desigKey.length > 0
          ? interviewsAll.filter((row) => applicantIdSet.has(String(row.job_applicant ?? "").trim()))
          : interviewsAll;

      const interviewNames = interviews.map((r) => String(r.name ?? "").trim()).filter(Boolean);
      const feedbackRows = await listInterviewFeedbackForInterviews(ctx.creds, interviewNames);

      const offersAll = await listJobOffersForCompany(ctx.creds, companyDocName);
      const hasRecruitmentFilters = Boolean(filterDepartment || filterDesignation || filterJobOpening);
      const offers = hasRecruitmentFilters
        ? offersAll.filter((o) => applicantIdSet.has(String(o.job_applicant ?? "").trim()))
        : offersAll;

      let cleared = 0;
      let rejected = 0;
      let other = 0;
      const interviewerCounts = new Map<string, number>();
      for (const fr of feedbackRows) {
        const res = String(fr.result ?? "").trim();
        const rl = res.toLowerCase();
        if (rl === "cleared") cleared += 1;
        else if (rl === "rejected") rejected += 1;
        else other += 1;
        const iv = String(fr.interviewer ?? "").trim() || "(Unassigned)";
        interviewerCounts.set(iv, (interviewerCounts.get(iv) ?? 0) + 1);
      }

      const feedbackByInterviewer = [...interviewerCounts.entries()]
        .map(([interviewer, count]) => ({ interviewer, count }))
        .sort((a, b) => b.count - a.count)
        .slice(0, 25);

      const offersAwaiting = offers.filter((r) => String(r.status ?? "").trim() === "Awaiting Response").length;
      const offersAccepted = offers.filter((r) => String(r.status ?? "").trim() === "Accepted").length;
      const offersRejected = offers.filter((r) => String(r.status ?? "").trim() === "Rejected").length;
      const decided = offersAccepted + offersRejected;
      const decidedPercent = decided > 0 ? Math.round((offersAccepted / decided) * 1000) / 10 : null;

      const weekStarts = last12WeekStarts();
      const weekIndex = new Map<string, number>();
      weekStarts.forEach((w, i) => weekIndex.set(w, i));
      const weekly = weekStarts.map((weekStart) => ({
        weekStart,
        applicants: 0,
        interviews: 0,
        feedback: 0,
        offers: 0,
      }));

      const bump = (ymd: string | undefined | null, field: "applicants" | "interviews" | "feedback" | "offers") => {
        const d = parseYmdToUtcDate(ymd);
        if (!d) return;
        const ws = weekStartMondayUtc(d);
        const idx = weekIndex.get(ws);
        if (idx == null) return;
        weekly[idx][field] += 1;
      };

      for (const a of applicants) {
        bump(String(a.creation ?? ""), "applicants");
      }
      for (const row of interviews) {
        const sched = String(row.scheduled_on ?? "").trim().slice(0, 10);
        const cre = String(row.creation ?? "").trim().slice(0, 10);
        bump(sched || cre, "interviews");
      }
      for (const fr of feedbackRows) {
        bump(String(fr.creation ?? ""), "feedback");
      }
      for (const o of offers) {
        const od = String(o.offer_date ?? "").trim().slice(0, 10);
        const cre = String(o.creation ?? "").trim().slice(0, 10);
        bump(od || cre, "offers");
      }

      const deptOpts = [...new Set(openingMeta.map((o) => o.department).filter(Boolean))].sort((a, b) =>
        a.localeCompare(b),
      );
      const desigOpts = [
        ...new Set(applicantsAll.map((a) => String(a.designation ?? "").trim()).filter(Boolean)),
      ].sort((a, b) => a.localeCompare(b));
      const openingOpts = openingMeta
        .map((o) => ({ name: o.name, label: o.jobTitle || o.name }))
        .sort((a, b) => a.label.localeCompare(b.label));

      return {
        data: {
          filtersApplied: {
            department: filterDepartment || undefined,
            designation: filterDesignation || undefined,
            jobOpening: filterJobOpening || undefined,
          },
          filterOptions: {
            departments: deptOpts,
            designations: desigOpts,
            openings: openingOpts,
          },
          recruitment: {
            applicants: applicantIdSet.size,
            interviews: interviewNames.length,
            feedbackSubmitted: feedbackRows.length,
            offersTotal: offers.length,
            offersAwaiting,
            offersAccepted,
            offersRejected,
          },
          weeklyTrend: weekly,
          feedbackByResult: { cleared, rejected, other },
          feedbackByInterviewer,
          offerAcceptance: {
            accepted: offersAccepted,
            rejected: offersRejected,
            awaiting: offersAwaiting,
            decidedPercent,
          },
        },
        meta: { source: "bff" as const },
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

  /** HR: Job Applicants tied to this tenant's Job Openings (`job_title` → Job Opening name). */
  app.get("/v1/recruitment/applicants", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR admin privileges required to view applicants" });
    }
    try {
      const { openingNames } = await openingNamesForCompany(ctx);
      const applicants = await listApplicantsForCompanyPipeline(ctx, openingNames);
      const openingLinks = applicants.map((a) => String(a.job_title ?? "").trim()).filter((n) => n.length > 0);
      const titleMap = await jobOpeningTitleMap(ctx.creds, openingLinks);
      const data = applicants.map((a) => {
        const name = String(a.name ?? "");
        const jo = String(a.job_title ?? "").trim();
        const roleTitle =
          (jo && titleMap.get(jo)) || String(a.designation ?? "").trim() || jo || "—";
        const created = a.creation != null ? String(a.creation) : "";
        return {
          name,
          applicantName: String(a.applicant_name ?? "—"),
          email: String(a.email_id ?? "").trim() || undefined,
          status: String(a.status ?? "").trim() || undefined,
          roleTitle,
          /** Job Opening document name (same as Job Applicant `job_title` link in ERPNext). */
          jobOpening: jo || undefined,
          submittedOn: created.slice(0, 10) || undefined,
          /** Raw `creation` from ERP for time-to-hire metrics in Pay Hub. */
          applicantCreatedAt: created.trim() || undefined,
        };
      });
      return { data, meta: { source: "bff" as const } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** HR: Job Offers for this tenant company. */
  app.get("/v1/recruitment/offers", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR admin privileges required to list offers" });
    }
    try {
      const { companyDocName } = await openingNamesForCompany(ctx);
      const rows = await listJobOffersForCompany(ctx.creds, companyDocName);
      const data = rows.map((raw) => mapJobOfferRow(raw));
      return { data, meta: { source: "bff" as const } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** Single Job Offer (HR or applicant owner). */
  app.get("/v1/recruitment/offers/:name", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const name = String((req.params as { name?: string }).name ?? "").trim();
    const openingNames = ctx.canSubmitOnBehalf
      ? (await openingNamesForCompany(ctx)).openingNames
      : new Set<string>();
    try {
      const doc = await assertJobOfferReadable(ctx, decodeURIComponent(name), openingNames);
      return { data: mapJobOfferRow(doc), meta: { source: "bff" as const } };
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** HR: create Job Offer for an applicant in the tenant pipeline. */
  app.post("/v1/recruitment/offers", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR admin privileges required to create offers" });
    }
    const body = asRecord(req.body) ?? {};
    const jobApplicant = String(body.job_applicant ?? "").trim();
    const offerDateRaw = String(body.offer_date ?? "").trim();
    const joiningDateRaw = String(body.date_of_joining ?? "").trim();
    const offerDate = offerDateRaw || (joiningDateRaw ? todayYmd() : "");
    if (!jobApplicant) {
      return reply.status(400).send({
        error: "job_applicant is required",
        fieldErrors: { job_applicant: "Required" },
      });
    }
    if (!offerDate) {
      return reply.status(400).send({
        error: "Provide offer_date or date_of_joining",
        fieldErrors: { offer_date: "offer_date or date_of_joining required" },
      });
    }
    try {
      const { companyDocName, openingNames } = await openingNamesForCompany(ctx);
      const applicant = await erp.getDoc(ctx.creds, "Job Applicant", jobApplicant);
      const jt = String(applicant.job_title ?? "").trim();
      if (!openingNames.has(jt)) {
        return reply.status(400).send({ error: "Applicant is not linked to a Job Opening in your company pipeline" });
      }
      const offer: Record<string, unknown> = {
        job_applicant: jobApplicant,
        company: companyDocName,
        offer_date: offerDate,
      };
      const terms = String(body.terms ?? "").trim();
      if (terms) offer.terms = terms;
      const status = String(body.status ?? "").trim();
      if (status) offer.status = status;
      const created = await erp.createDoc(ctx.creds, "Job Offer", offer);
      const name = String(created.name ?? "").trim();
      return reply.status(201).send({ data: { name, id: name }, meta: { source: "bff" as const } });
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** HR: Interview Type list (scheduling + feedback templates). */
  app.get("/v1/recruitment/interview-types", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR admin privileges required to list interview types" });
    }
    try {
      const rows = await erp.getList(ctx.creds, "Interview Type", {
        fields: ["name", "interview_type_name"],
        order_by: "interview_type_name asc",
        limit_page_length: 200,
      });
      const data = rows
        .map((raw) => asRecord(raw))
        .filter((r): r is Record<string, unknown> => r != null)
        .map((r) => {
          const name = String(r.name ?? "").trim();
          const label = String(r.interview_type_name ?? "").trim() || name;
          return { name, label };
        });
      return { data, meta: { source: "bff" as const } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** Employee: interviews for Job Applicant rows owned by the caller (email match). */
  app.get("/v1/recruitment/interviews/mine", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    try {
      const applicants = await listMyJobApplicants(ctx);
      const names = applicants.map((a) => String(a.name ?? "").trim()).filter(Boolean);
      const rows = await listInterviewsForApplicants(ctx.creds, names);
      const data = rows.map((raw) => mapInterviewListRow(raw));
      return { data, meta: { source: "bff" as const } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** HR: interviews whose Job Opening belongs to the tenant company. */
  app.get("/v1/recruitment/interviews", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR admin privileges required to list interviews" });
    }
    try {
      const { openingNames } = await openingNamesForCompany(ctx);
      const rows = await listInterviewsForTenantOpenings(ctx.creds, openingNames);
      const data = rows.map((raw) => mapInterviewListRow(raw));
      return { data, meta: { source: "bff" as const } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  app.get("/v1/recruitment/interviews/:name/feedback", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const name = String((req.params as { name?: string }).name ?? "").trim();
    const openingNames = ctx.canSubmitOnBehalf
      ? (await openingNamesForCompany(ctx)).openingNames
      : new Set<string>();
    try {
      await assertInterviewReadable(ctx, decodeURIComponent(name), openingNames);
      const trimmed = decodeURIComponent(name).trim();
      const fbRows = await erp.getList(ctx.creds, "Interview Feedback", {
        filters: [["interview", "=", trimmed]],
        fields: ["name", "interviewer", "result", "feedback", "creation"],
        order_by: "creation desc",
        limit_page_length: 50,
      });
      const data = fbRows
        .map((raw) => asRecord(raw))
        .filter((r): r is Record<string, unknown> => r != null)
        .map((r) => {
          const created = r.creation != null ? String(r.creation) : "";
          return {
            name: String(r.name ?? "").trim(),
            interviewer: String(r.interviewer ?? "").trim() || undefined,
            result: String(r.result ?? "").trim() || undefined,
            feedback: String(r.feedback ?? "").trim() || undefined,
            submittedOn: created.slice(0, 10) || undefined,
          };
        });
      return { data, meta: { source: "bff" as const } };
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  app.get("/v1/recruitment/interviews/:name", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const name = String((req.params as { name?: string }).name ?? "").trim();
    const openingNames = ctx.canSubmitOnBehalf
      ? (await openingNamesForCompany(ctx)).openingNames
      : new Set<string>();
    try {
      const doc = await assertInterviewReadable(ctx, decodeURIComponent(name), openingNames);
      const base = mapInterviewListRow(doc);
      const interviewers = interviewersFromInterviewDoc(doc);
      return {
        data: { ...base, ...(interviewers.length > 0 ? { interviewers } : {}) },
        meta: { source: "bff" as const },
      };
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** HR: schedule Interview (Frappe HRMS). */
  app.post("/v1/recruitment/interviews", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR admin privileges required to schedule interviews" });
    }
    const body = asRecord(req.body) ?? {};
    const jobApplicant = String(body.job_applicant ?? "").trim();
    const scheduledOn = String(body.scheduled_on ?? "").trim();
    const interviewRound = String(body.interview_round ?? "").trim() || "Round 1";
    const fromTime = String(body.from_time ?? "").trim() || "09:00:00";
    const toTime = String(body.to_time ?? "").trim() || "10:00:00";
    let interviewType = String(body.interview_type ?? "").trim();
    const interviewerCandidates = parseInterviewerCandidates(body);
    if (!jobApplicant || !scheduledOn) {
      return reply.status(400).send({
        error: "job_applicant and scheduled_on are required",
        fieldErrors: { job_applicant: "Required", scheduled_on: "Required" },
      });
    }
    try {
      const { openingNames } = await openingNamesForCompany(ctx);
      let applicant: Record<string, unknown>;
      try {
        applicant = await erp.getDoc(ctx.creds, "Job Applicant", jobApplicant);
      } catch (e) {
        if (e instanceof ErpError && (e.status === 404 || e.status === 403)) {
          return reply.status(400).send({ error: "Job Applicant not found" });
        }
        throw e;
      }
      const jt = String(applicant.job_title ?? "").trim();
      if (!openingNames.has(jt)) {
        return reply.status(400).send({
          error: "Applicant is not linked to a Job Opening in your company pipeline",
        });
      }
      if (!interviewType) {
        interviewType = await pickDefaultInterviewType(ctx.creds);
        if (!interviewType) {
          return reply.status(400).send({
            error: "No Interview Type in ERPNext — create at least one Interview Type.",
          });
        }
      }
      let interviewerUserNames: string[] = [];
      if (interviewerCandidates.length > 0) {
        try {
          interviewerUserNames = await resolveFrappeInterviewerUserNames(ctx.creds, interviewerCandidates);
        } catch (e) {
          if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
          throw e;
        }
      }
      const doc: Record<string, unknown> = {
        job_applicant: jobApplicant,
        interview_round: interviewRound,
        interview_type: interviewType,
        scheduled_on: scheduledOn,
        from_time: fromTime,
        to_time: toTime,
        status: "Pending",
      };
      if (interviewerUserNames.length > 0) {
        doc.interview_details = interviewerUserNames.map((interviewer) => ({ interviewer }));
      }
      const created = await erp.createDoc(ctx.creds, "Interview", doc);
      const nm = String(created.name ?? "").trim();
      return reply.status(201).send({ data: { name: nm, id: nm || undefined }, meta: { source: "bff" as const } });
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * HR: create Interview Feedback (skills from Interview Type; interviewer = bridge user).
   * Body: result (Cleared|Rejected), feedback?, skill_rating? (1–5, default 3).
   */
  app.post("/v1/recruitment/interviews/:name/feedback", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR admin privileges required to submit interview feedback" });
    }
    const interviewParam = String((req.params as { name?: string }).name ?? "").trim();
    const interviewName = decodeURIComponent(interviewParam).trim();
    const body = asRecord(req.body) ?? {};
    const result = String(body.result ?? "").trim();
    const feedbackText = String(body.feedback ?? "").trim();
    const skillStarsRaw = body.skill_rating;
    const skillStars =
      typeof skillStarsRaw === "number" && Number.isFinite(skillStarsRaw)
        ? skillStarsRaw
        : Number.parseFloat(String(skillStarsRaw ?? "3")) || 3;
    if (result !== "Cleared" && result !== "Rejected") {
      return reply.status(400).send({
        error: "result must be Cleared or Rejected",
        fieldErrors: { result: "Invalid" },
      });
    }
    const interviewer = ctx.userEmail.trim();
    if (!interviewer) {
      return reply.status(400).send({ error: "Missing user email for interviewer" });
    }
    try {
      const { openingNames } = await openingNamesForCompany(ctx);
      const interviewDoc = await assertInterviewReadable(ctx, interviewName, openingNames);
      let itype = String(interviewDoc.interview_type ?? "").trim();
      if (!itype) {
        itype = await pickDefaultInterviewType(ctx.creds);
      }
      if (!itype) {
        return reply.status(400).send({ error: "Interview has no Interview Type — fix the Interview in ERPNext." });
      }
      let skillAssessment: Array<{ skill: string; rating: number }>;
      try {
        skillAssessment = await skillAssessmentFromInterviewType(ctx.creds, itype, skillStars);
      } catch (e) {
        if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
        throw e;
      }
      const fb: Record<string, unknown> = {
        interview: interviewName,
        interviewer,
        interview_type: itype,
        result,
        feedback: feedbackText || undefined,
        skill_assessment: skillAssessment,
      };
      const created = await erp.createDoc(ctx.creds, "Interview Feedback", fb);
      const nm = String(created.name ?? "").trim();
      try {
        if (nm) await erp.submitDoc(ctx.creds, "Interview Feedback", nm);
      } catch {
        /* draft is still useful if submit is blocked by workflow */
      }
      return reply.status(201).send({ data: { name: nm }, meta: { source: "bff" as const } });
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
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
