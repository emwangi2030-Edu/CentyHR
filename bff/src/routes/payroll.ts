/**
 * Kenya / Frappe HRMS payroll — full read + write layer for Pay Hub.
 *
 * Read routes (existing):
 *   GET  /v1/payroll/salary-slips          — list slips (HR only)
 *   GET  /v1/payroll/salary-slips/:name    — single slip detail
 *   GET  /v1/payroll/payroll-entries       — list payroll entry runs
 *
 * Write / Setup routes (new):
 *   GET  /v1/payroll/setup                 — check if Kenya salary structure is ready
 *   POST /v1/payroll/setup                 — one-time Kenya statutory structure setup
 *   GET  /v1/payroll/team                  — list employees with current base salary
 *   POST /v1/payroll/team                  — quick-add employee + salary assignment
 *   POST /v1/payroll/run                   — generate + submit salary slips for a period
 */
import type { FastifyPluginAsync, FastifyReply } from "fastify";
import type { HrContext } from "../types.js";
import { defaultClient, ErpError } from "../erpnext/client.js";
import type { ErpCredentials } from "../erpnext/client.js";
import { publicErpFailure, parseFrappeErrorBody } from "../erpnext/frappeResponse.js";
import { resolveHrContext, HttpError } from "../context/resolveHrContext.js";

const erp = defaultClient();

function replyErp(reply: FastifyReply, e: ErpError): FastifyReply {
  const status = e.status >= 500 ? 502 : e.status;
  return reply.status(status).send(publicErpFailure(e));
}

function parseDate(v: unknown): string {
  const s = String(v ?? "").trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : "";
}

// ── Kenya Salary Structure Constants ─────────────────────────────────────────

const KE_STRUCTURE_SUFFIX = "Kenya Standard Payroll";

function kenyaStructureName(company: string): string {
  return `${KE_STRUCTURE_SUFFIX} - ${company}`;
}

/**
 * Kenya PAYE formula (Frappe Python expression evaluated on gross_pay).
 * Tax bands (2024/25): 0-24k:10%, 24k-32333:25%, 32333-500k:30%, 500k-800k:32.5%, 800k+:35%
 * Personal relief: KES 2,400/month
 *
 * Note: Frappe's safe_eval sandbox does NOT expose max() / min() builtins.
 * We use Python ternary expressions instead.
 * At gross_pay < 24000 the entire tax is covered by personal relief → 0.
 * At gross_pay >= 24000 the band calculation always exceeds the 2400 relief,
 * so no outer clamp is needed.
 */
const PAYE_FORMULA =
  "(0 if gross_pay < 24000 else round(" +
  "((gross_pay if gross_pay < 32333 else 32333) - 24000) * 0.25 + " +
  "(0 if gross_pay <= 32333 else ((gross_pay if gross_pay < 500000 else 500000) - 32333) * 0.30) + " +
  "(0 if gross_pay <= 500000 else ((gross_pay if gross_pay < 800000 else 800000) - 500000) * 0.325) + " +
  "(0 if gross_pay <= 800000 else (gross_pay - 800000) * 0.35), 2))";

/** NSSF: 6% of gross, max KES 2,160 (pensionable pay ceiling KES 36,000).
 *  Frappe sandbox has no min() — use ternary. */
const NSSF_FORMULA = "(round(gross_pay * 0.06, 2) if gross_pay < 36000 else 2160)";

/** SHIF (Social Health Insurance Fund): 2.75% of gross */
const SHIF_FORMULA = "round(gross_pay * 0.0275, 2)";

/** Affordable Housing Levy (employee share): 1.5% of gross */
const HOUSING_LEVY_FORMULA = "round(gross_pay * 0.015, 2)";

// ── Setup Helpers ─────────────────────────────────────────────────────────────

/**
 * Ensure a Salary Component exists (earning or deduction).
 * If it already exists (any abbr/type), we leave it as-is.
 */
async function ensureSalaryComponent(
  creds: ErpCredentials,
  name: string,
  type: "Earning" | "Deduction",
  abbr: string
): Promise<void> {
  try {
    await erp.getDoc(creds, "Salary Component", name);
    return; // already exists — trust ERP's version
  } catch (e) {
    if (!(e instanceof ErpError) || e.status !== 404) throw e;
  }
  // Pass __newname to handle both autoname="field:..." and autoname="Prompt" setups
  await erp.callMethod(creds, "frappe.client.insert", {
    doc: {
      doctype: "Salary Component",
      __newname: name,
      salary_component: name,
      salary_component_abbr: abbr,
      type,
      is_payable: 1,
    },
  });
}

/**
 * Ensure the "Kenya Standard Payroll - {company}" salary structure exists and is submitted.
 * Creates all required salary components first.
 */
/**
 * Submit a document, working around Frappe's optimistic-lock (417) race.
 *
 * Root cause: Frappe HRMS enqueues a background job right after Salary Structure
 * is inserted. That job runs ~2-3 s later and updates `modified` in the DB.
 * `frappe.client.submit({doc:{doctype,name}})` creates an in-memory Document from
 * the sparse dict — leaving `self.modified = undefined` — so check_if_latest
 * always sees a mismatch against the DB value.
 *
 * Fix: fetch the doc first (getting the current `modified`), then pass it back
 * to `frappe.client.submit` so the in-memory `modified` matches the DB value.
 * Retry with exponential back-off if a concurrent job still slips in between.
 */
async function submitWithRetry(
  creds: ErpCredentials,
  doctype: string,
  name: string,
  maxAttempts = 4,
  /** Pass the already-fetched/inserted doc to skip the getDoc on the first attempt. */
  initialDoc?: Record<string, unknown>
): Promise<void> {
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    // Use the caller-supplied doc on the first attempt to avoid a redundant getDoc
    // round-trip (~300–600 ms). Re-fetch on retries to capture the latest modified.
    const current = (attempt === 0 && initialDoc)
      ? initialDoc
      : await erp.getDoc(creds, doctype, name);
    const modified = current.modified;
    const docstatus = Number(current.docstatus);
    console.log(
      `[payroll] submit attempt ${attempt + 1}/${maxAttempts} — ${doctype} "${name}" ` +
      `docstatus=${docstatus} modified=${modified}`
    );

    if (docstatus === 1) {
      console.log(`[payroll] ${doctype} "${name}" is already submitted — skipping`);
      return;
    }

    try {
      // Pass the full doc (not a sparse dict) so Frappe's validate doesn't fail with
      // MandatoryError for fields like company/is_active/currency, AND so that
      // check_if_latest sees self.modified == DB.modified.
      await erp.callMethod(creds, "frappe.client.submit", {
        doc: current,
      });
      console.log(`[payroll] ${doctype} "${name}" submitted OK on attempt ${attempt + 1}`);
      return;
    } catch (e) {
      const is417 = e instanceof ErpError && e.status === 417;
      const bodySnippet = e instanceof ErpError
        ? JSON.stringify(e.body ?? "").slice(0, 300)
        : String(e);
      console.log(
        `[payroll] submit attempt ${attempt + 1} failed` +
        ` (status=${e instanceof ErpError ? e.status : "?"} is417=${is417}): ${bodySnippet}`
      );
      if (is417 && attempt < maxAttempts - 1) {
        const delay = 500 * (attempt + 1); // 500 ms, 1 s, 1.5 s
        console.log(`[payroll] waiting ${delay} ms before retry…`);
        await new Promise((r) => setTimeout(r, delay));
        continue;
      }
      throw e;
    }
  }
}

/** Build the Salary Structure doc payload (used for initial create and rebuild). */
function buildStructureDoc(structName: string, company: string): Record<string, unknown> {
  return {
    doctype: "Salary Structure",
    __newname: structName,
    salary_structure_name: structName,
    company,
    currency: "KES",
    is_active: "Yes",
    payroll_frequency: "Monthly",
    earnings: [
      {
        doctype: "Salary Detail",
        salary_component: "Basic Pay",
        abbr: "BP",
        amount_based_on_formula: 1,
        formula: "base",
        idx: 1,
      },
    ],
    deductions: [
      {
        doctype: "Salary Detail",
        salary_component: "PAYE",
        abbr: "PAYE",
        amount_based_on_formula: 1,
        formula: PAYE_FORMULA,
        idx: 1,
      },
      {
        doctype: "Salary Detail",
        salary_component: "NSSF",
        abbr: "NSSF",
        amount_based_on_formula: 1,
        formula: NSSF_FORMULA,
        idx: 2,
      },
      {
        doctype: "Salary Detail",
        salary_component: "SHIF",
        abbr: "SHIF",
        amount_based_on_formula: 1,
        formula: SHIF_FORMULA,
        idx: 3,
      },
      {
        doctype: "Salary Detail",
        salary_component: "Housing Levy",
        abbr: "HL",
        amount_based_on_formula: 1,
        formula: HOUSING_LEVY_FORMULA,
        idx: 4,
      },
    ],
  };
}

/**
 * Returns true if any earning/deduction formula uses max() or min(), which are
 * NOT available in Frappe's safe_eval sandbox and will cause a ValidationError
 * when the salary slip is generated.
 */
function formulasNeedPatch(doc: Record<string, unknown>): boolean {
  for (const tableKey of ["earnings", "deductions"]) {
    const rows = doc[tableKey];
    if (!Array.isArray(rows)) continue;
    for (const row of rows) {
      const r = row as Record<string, unknown>;
      if (
        typeof r.formula === "string" &&
        (/\bmax\s*\(/.test(r.formula) || /\bmin\s*\(/.test(r.formula))
      ) {
        console.log(`[payroll] Found unsupported max/min in ${tableKey} formula: ${r.formula?.toString().slice(0, 80)}`);
        return true;
      }
    }
  }
  return false;
}

// ── In-process caches ────────────────────────────────────────────────────────
// Avoid redundant ERP round-trips for setup checks that rarely change.

const _structCache    = new Map<string, number>(); // key = structName,          value = timestamp
const _fiscalYrCache  = new Map<string, number>(); // key = `${company}:${year}`, value = timestamp
const _holidayCache   = new Map<string, number>(); // key = `${company}:${year}`, value = timestamp

const STRUCT_CACHE_TTL_MS = 5  * 60 * 1_000; //  5 minutes
const SETUP_CACHE_TTL_MS  = 30 * 60 * 1_000; // 30 minutes

async function ensureKenyaStructure(
  creds: ErpCredentials,
  company: string
): Promise<string> {
  const structName = kenyaStructureName(company);

  // Return immediately if cached (structure is good and formulas are clean)
  const cachedAt = _structCache.get(structName);
  if (cachedAt && Date.now() - cachedAt < STRUCT_CACHE_TTL_MS) return structName;

  // Check if it already exists
  try {
    const existing = await erp.getDoc(creds, "Salary Structure", structName);
    const ds = Number(existing.docstatus);
    console.log(
      `[payroll] Salary Structure "${structName}" exists: docstatus=${ds} modified=${existing.modified}`
    );

    if (ds === 1) {
      // Submitted — but check if formulas use max()/min() which Frappe's sandbox
      // doesn't support. If so, we must rebuild: cancel+delete all assignments
      // that reference this structure first (HRMS blocks cancelling a structure
      // with live assignments), then cancel+delete the structure, then recreate
      // structure + assignments with fixed formulas.
      if (!formulasNeedPatch(existing)) {
        _structCache.set(structName, Date.now()); // cache: structure is ready
        return structName; // good — nothing to do
      }

      console.log(`[payroll] Structure "${structName}" has bad formulas — collecting assignments to rebuild`);

      // ── Step 1: collect and cancel all assignments for this structure ──────
      const assignRows = (await erp.getList(creds, "Salary Structure Assignment", {
        filters: [["salary_structure", "=", structName]],
        fields: ["name", "docstatus", "employee", "from_date", "base"],
        limit_page_length: 2000,
      })) as Record<string, unknown>[];

      // Remember submitted ones so we can recreate them after the rebuild.
      const savedAssignments: Array<{ employee: string; from_date: string; base: number }> = [];

      for (const row of assignRows) {
        const aName = String(row.name ?? "");
        const ads = Number(row.docstatus ?? 0);
        if (ads === 1) {
          // Fetch full doc for complete data, then cancel
          try {
            const adoc = await erp.getDoc(creds, "Salary Structure Assignment", aName);
            savedAssignments.push({
              employee: String(adoc.employee ?? ""),
              from_date: String(adoc.from_date ?? ""),
              base: Number(adoc.base ?? 0),
            });
            await erp.cancelDoc(creds, "Salary Structure Assignment", aName);
            console.log(`[payroll] Cancelled assignment ${aName}`);
          } catch (e) {
            console.log(`[payroll] cancel assignment ${aName}: ${e instanceof Error ? e.message : String(e)}`);
          }
        }
        try {
          await erp.deleteDoc(creds, "Salary Structure Assignment", aName);
          console.log(`[payroll] Deleted assignment ${aName}`);
        } catch (e) {
          console.log(`[payroll] delete assignment ${aName}: ${e instanceof Error ? e.message : String(e)}`);
        }
      }

      // ── Step 2: cancel + delete the structure ─────────────────────────────
      try {
        await erp.cancelDoc(creds, "Salary Structure", structName);
        console.log(`[payroll] Cancelled structure "${structName}"`);
      } catch (cancelErr) {
        const msg = cancelErr instanceof ErpError
          ? (parseFrappeErrorBody(cancelErr.body) ?? cancelErr.message)
          : String(cancelErr);
        console.log(`[payroll] Cancel structure failed: ${msg}`);
        throw cancelErr;
      }
      await erp.deleteDoc(creds, "Salary Structure", structName);
      console.log(`[payroll] Deleted structure "${structName}" — will recreate with fixed formulas`);

      // ── Step 3: recreate structure (falls through to creation code below) ─
      // After the structure is submitted we recreate all saved assignments.
      // We use a nested immediately-invoked approach via a flag below.

      // Recreate structure + components
      await Promise.all([
        ensureSalaryComponent(creds, "Basic Pay", "Earning", "BP"),
        ensureSalaryComponent(creds, "PAYE", "Deduction", "PAYE"),
        ensureSalaryComponent(creds, "NSSF", "Deduction", "NSSF"),
        ensureSalaryComponent(creds, "SHIF", "Deduction", "SHIF"),
        ensureSalaryComponent(creds, "Housing Levy", "Deduction", "HL"),
      ]);
      await erp.callMethod(creds, "frappe.client.insert", { doc: buildStructureDoc(structName, company) });
      console.log(`[payroll] Structure recreated — submitting`);
      await submitWithRetry(creds, "Salary Structure", structName);
      console.log(`[payroll] Structure "${structName}" submitted with fixed formulas`);

      // ── Step 4: recreate all assignments ──────────────────────────────────
      for (const a of savedAssignments) {
        if (!a.employee || !a.from_date || !a.base) continue;
        try {
          await upsertSalaryStructureAssignment(creds, {
            employeeId: a.employee,
            structName,
            company,
            from_date: a.from_date,
            base: a.base,
          });
          console.log(`[payroll] Recreated assignment for ${a.employee}`);
        } catch (e) {
          console.log(`[payroll] Recreate assignment for ${a.employee}: ${e instanceof Error ? e.message : String(e)}`);
        }
      }

      _structCache.set(structName, Date.now()); // rebuilt — cache the result
      return structName;
    }

    // Draft (ds=0): the previous run probably failed mid-submit leaving a stale draft.
    // Deleting and recreating fresh is more reliable than retrying the submit on a
    // doc whose modified timestamp may have drifted due to Frappe hooks.
    if (ds === 0) {
      console.log(`[payroll] Deleting stale draft Salary Structure "${structName}"`);
      await erp.deleteDoc(creds, "Salary Structure", structName);
      // fall through to recreate below
    }
    // ds === 2 (cancelled) — also fall through to recreate
  } catch (e) {
    if (!(e instanceof ErpError) || e.status !== 404) throw e;
    console.log(`[payroll] Salary Structure "${structName}" not found — will create`);
  }

  // Ensure salary components exist (idempotent)
  await Promise.all([
    ensureSalaryComponent(creds, "Basic Pay", "Earning", "BP"),
    ensureSalaryComponent(creds, "PAYE", "Deduction", "PAYE"),
    ensureSalaryComponent(creds, "NSSF", "Deduction", "NSSF"),
    ensureSalaryComponent(creds, "SHIF", "Deduction", "SHIF"),
    ensureSalaryComponent(creds, "Housing Levy", "Deduction", "HL"),
  ]);

  // Create the salary structure (pass __newname to handle autoname="Prompt" setups)
  await erp.callMethod(creds, "frappe.client.insert", { doc: buildStructureDoc(structName, company) });

  console.log(`[payroll] Salary Structure created — starting submit`);
  // Submit with retry — passes the fresh modified timestamp so Frappe's check_if_latest
  // sees self.modified == DB.modified and the background-job race is avoided.
  await submitWithRetry(creds, "Salary Structure", structName);
  _structCache.set(structName, Date.now()); // fresh create — cache the result
  return structName;
}

/**
 * Ensure the company has a default Holiday List so Frappe can compute working
 * days when generating salary slips. Creates a Kenya public-holidays list for
 * the given year if one doesn't exist yet, then sets it as the company default.
 */
async function ensureCompanyHolidayList(
  creds: ErpCredentials,
  company: string,
  year: number
): Promise<void> {
  const cacheKey = `${company}:${year}`;
  const cachedAt = _holidayCache.get(cacheKey);
  if (cachedAt && Date.now() - cachedAt < SETUP_CACHE_TTL_MS) return;

  // Check if the company already has a default holiday list set
  const companyDoc = await erp.getDoc(creds, "Company", company);
  if (companyDoc.default_holiday_list) {
    _holidayCache.set(cacheKey, Date.now());
    return;
  }

  const listName = `Kenya Public Holidays ${year}`;

  // Create the holiday list if it doesn't already exist
  try {
    await erp.getDoc(creds, "Holiday List", listName);
  } catch (e) {
    if (!(e instanceof ErpError) || e.status !== 404) throw e;

    const kenyaHolidays = [
      { date: `${year}-01-01`, description: "New Year's Day" },
      { date: `${year}-05-01`, description: "Labour Day" },
      { date: `${year}-06-01`, description: "Madaraka Day" },
      { date: `${year}-10-20`, description: "Mashujaa Day" },
      { date: `${year}-12-12`, description: "Jamhuri Day" },
      { date: `${year}-12-25`, description: "Christmas Day" },
      { date: `${year}-12-26`, description: "Boxing Day" },
    ];

    await erp.callMethod(creds, "frappe.client.insert", {
      doc: {
        doctype: "Holiday List",
        holiday_list_name: listName,
        from_date: `${year}-01-01`,
        to_date: `${year}-12-31`,
        holidays: kenyaHolidays.map((h, idx) => ({
          doctype: "Holiday",
          holiday_date: h.date,
          description: h.description,
          idx: idx + 1,
        })),
      },
    }).catch((e: unknown) => {
      // 409 = duplicate from concurrent request — safe to ignore
      if (!(e instanceof ErpError) || e.status !== 409) throw e;
    });
  }

  // Set as company default
  await erp.callMethod(creds, "frappe.client.set_value", {
    doctype: "Company",
    name: company,
    fieldname: "default_holiday_list",
    value: listName,
  });
  console.log(`[payroll] Set company default holiday list: "${listName}"`);
  _holidayCache.set(cacheKey, Date.now());
}

/**
 * Ensure a Fiscal Year covering the given year exists for the company.
 * Frappe throws FiscalYearError when creating salary slips if no active fiscal
 * year covers the slip date for the company.
 *
 * Logic (mirrors Frappe's get_fiscal_year SQL):
 *   - A fiscal year applies if it covers the date AND either:
 *     (a) its companies table includes this company, OR
 *     (b) its companies table is empty (global — applies to all)
 */
async function ensureCompanyFiscalYear(
  creds: ErpCredentials,
  company: string,
  year: number
): Promise<void> {
  const cacheKey = `${company}:${year}`;
  const cachedAt = _fiscalYrCache.get(cacheKey);
  if (cachedAt && Date.now() - cachedAt < SETUP_CACHE_TTL_MS) return;

  // List all fiscal years whose range overlaps with our target year
  const candidates = (await erp.getList(creds, "Fiscal Year", {
    filters: [
      ["year_start_date", "<=", `${year}-12-31`],
      ["year_end_date", ">=", `${year}-01-01`],
      ["disabled", "=", 0],
    ],
    fields: ["name"],
    limit_page_length: 20,
  })) as Record<string, unknown>[];

  for (const c of candidates) {
    const fyName = String(c.name ?? "");
    const fyDoc = await erp.getDoc(creds, "Fiscal Year", fyName);
    const rows = Array.isArray(fyDoc.companies)
      ? (fyDoc.companies as Record<string, unknown>[])
      : [];

    // Global fiscal year (empty companies table) → covers all companies
    if (rows.length === 0) {
      console.log(`[payroll] Fiscal Year "${fyName}" is global — no action needed`);
      _fiscalYrCache.set(cacheKey, Date.now());
      return;
    }

    // Already has this company
    if (rows.some((r) => String(r.company ?? "") === company)) {
      console.log(`[payroll] Fiscal Year "${fyName}" already includes "${company}"`);
      _fiscalYrCache.set(cacheKey, Date.now());
      return;
    }

    // Add our company to this fiscal year
    rows.push({ doctype: "Fiscal Year Company", company });
    await erp.updateDoc(creds, "Fiscal Year", fyName, { ...fyDoc, companies: rows });
    console.log(`[payroll] Added "${company}" to Fiscal Year "${fyName}"`);
    _fiscalYrCache.set(cacheKey, Date.now());
    return;
  }

  // No fiscal year found — create a calendar-year one for this company
  const yearStr = String(year);
  await erp.callMethod(creds, "frappe.client.insert", {
    doc: {
      doctype: "Fiscal Year",
      year: yearStr,
      year_start_date: `${year}-01-01`,
      year_end_date: `${year}-12-31`,
      companies: [{ doctype: "Fiscal Year Company", company }],
    },
  }).catch((e: unknown) => {
    if (!(e instanceof ErpError) || e.status !== 409) throw e;
    // 409 = concurrent request already created it — safe to ignore
  });
  console.log(`[payroll] Created Fiscal Year ${yearStr} for company "${company}"`);
  _fiscalYrCache.set(cacheKey, Date.now());
}

/**
 * Upsert and SUBMIT a Salary Structure Assignment for an employee.
 *
 * Frappe HRMS's get_salary_structure query filters on ssa.docstatus = 1 AND
 * ss.docstatus = 1 — a draft assignment is invisible to payroll processing.
 * We therefore submit every assignment after create/update.
 */
async function upsertSalaryStructureAssignment(
  creds: ErpCredentials,
  opts: {
    employeeId: string;
    structName: string;
    company: string;
    from_date: string;
    base: number;
  }
): Promise<void> {
  const { employeeId, structName, company, from_date, base } = opts;

  // Check for existing assignment on the same date (any docstatus)
  const existing = await erp.getList(creds, "Salary Structure Assignment", {
    filters: [["employee", "=", employeeId], ["from_date", "=", from_date]],
    fields: ["name", "docstatus"],
    limit_page_length: 1,
  });

  let assignName: string;

  if (existing.length > 0) {
    assignName = String((existing[0] as { name: string }).name);
    const ds = Number((existing[0] as { docstatus?: unknown }).docstatus ?? 0);

    if (ds === 1) {
      // Already submitted — amend isn't needed; cancel + resubmit to change base.
      console.log(`[payroll] Assignment ${assignName} already submitted — cancel/resubmit to update`);
      await erp.callMethod(creds, "frappe.client.cancel", { doctype: "Salary Structure Assignment", name: assignName });
      await erp.deleteDoc(creds, "Salary Structure Assignment", assignName);
      assignName = ""; // fall through to fresh insert below
    } else {
      // Draft — update fields then submit
      try {
        await erp.callMethod(creds, "frappe.client.set_value", {
          doctype: "Salary Structure Assignment",
          name: assignName,
          fieldname: { base, salary_structure: structName },
        });
      } catch (e) {
        if (!(e instanceof ErpError) || e.status !== 417) throw e;
        // Version conflict — delete and recreate
        await erp.deleteDoc(creds, "Salary Structure Assignment", assignName);
        assignName = "";
      }
    }
  } else {
    assignName = "";
  }

  let insertedDoc: Record<string, unknown> | undefined;
  if (!assignName) {
    // Insert fresh — frappe.client.insert returns the full document in .message,
    // so we can pass it straight to submitWithRetry and skip the redundant getDoc.
    const inserted = (await erp.callMethod(creds, "frappe.client.insert", {
      doc: {
        doctype: "Salary Structure Assignment",
        __newname: `${employeeId}-${from_date}`,
        employee: employeeId,
        salary_structure: structName,
        company,
        from_date,
        base,
        currency: "KES",
      },
    })) as { message?: Record<string, unknown> };
    assignName = String(inserted?.message?.name ?? `${employeeId}-${from_date}`);
    insertedDoc = inserted?.message;
  }

  // Submit so Frappe's payroll query (which filters on docstatus=1) can find it
  console.log(`[payroll] Submitting assignment ${assignName}`);
  await submitWithRetry(creds, "Salary Structure Assignment", assignName, 4, insertedDoc);
  console.log(`[payroll] Assignment ${assignName} submitted`);
}

// ── Route Plugin ──────────────────────────────────────────────────────────────

export const payrollRoutes: FastifyPluginAsync = async (app) => {
  async function resolveSelfEmployee(ctx: HrContext): Promise<string | null> {
    const mine = await erp.listDocs(ctx.creds, "Employee", {
      filters: [
        ["user_id", "=", ctx.userEmail],
        ["company", "=", ctx.company],
      ],
      fields: ["name"],
      limit_page_length: 1,
    });
    const row = mine.data?.[0];
    return row && typeof (row as { name?: unknown }).name === "string"
      ? String((row as { name: string }).name)
      : null;
  }

  async function resolveEmployeeIdForRequest(ctx: HrContext, qEmp: string): Promise<string | null> {
    if (ctx.canSubmitOnBehalf) {
      if (!qEmp) return null;
      const empDoc = await erp.getDoc(ctx.creds, "Employee", qEmp);
      if (String(empDoc.company) !== ctx.company) return null;
      return qEmp;
    }
    return resolveSelfEmployee(ctx);
  }

  // ── Check Kenya salary structure setup status ────────────────────────────

  app.get("/v1/payroll/setup", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR only" });

    const structName = kenyaStructureName(ctx.company);
    try {
      const doc = await erp.getDoc(ctx.creds, "Salary Structure", structName);
      return { ready: Number(doc.docstatus) === 1, structure: structName };
    } catch (e) {
      if (e instanceof ErpError && e.status === 404) return { ready: false, structure: structName };
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  // ── One-time Kenya statutory salary structure setup ──────────────────────

  app.post("/v1/payroll/setup", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR only" });

    try {
      const structName = await ensureKenyaStructure(ctx.creds, ctx.company);
      return { ok: true, structure: structName };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  // ── List employees with current base salary ──────────────────────────────

  app.get("/v1/payroll/team", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR only" });

    const q = (req.query ?? {}) as Record<string, unknown>;
    const page = Math.max(1, Number(q.page ?? 1));
    const page_size = Math.min(50, Math.max(5, Number(q.page_size ?? 15)));
    const limit_start = (page - 1) * page_size;

    const empFilters: unknown[] = [
      ["company", "=", ctx.company],
      ["status", "=", "Active"],
    ];

    try {
      const [employees, assignments, countResult] = await Promise.all([
        erp.getList(ctx.creds, "Employee", {
          filters: empFilters,
          fields: ["name", "employee_name", "designation", "department", "date_of_joining"],
          order_by: "date_of_joining desc, name desc",
          limit_start,
          limit_page_length: page_size,
        }),
        erp.getList(ctx.creds, "Salary Structure Assignment", {
          filters: [["company", "=", ctx.company]],
          fields: ["employee", "base", "from_date", "salary_structure"],
          order_by: "from_date desc",
          limit_page_length: 500,
        }),
        erp.callMethod(ctx.creds, "frappe.client.get_count", {
          doctype: "Employee",
          filters: JSON.stringify(empFilters),
          debug: false,
        }).catch(() => null),
      ]);

      const total = Number(
        (countResult as { message?: number } | null)?.message ?? employees.length + limit_start
      );

      // Latest assignment per employee (list is ordered desc, first hit wins)
      const latestAssignment = new Map<string, Record<string, unknown>>();
      for (const a of assignments) {
        const r = a as Record<string, unknown>;
        const emp = String(r.employee ?? "");
        if (!latestAssignment.has(emp)) latestAssignment.set(emp, r);
      }

      const kenyaStruct = kenyaStructureName(ctx.company);
      const data = (employees as Record<string, unknown>[]).map((emp) => {
        const asgn = latestAssignment.get(String(emp.name ?? ""));
        return {
          ...emp,
          base_salary: asgn ? Number(asgn.base) : null,
          salary_structure: asgn ? String(asgn.salary_structure ?? "") : null,
          on_kenya_structure: asgn
            ? String(asgn.salary_structure ?? "") === kenyaStruct
            : false,
        };
      });

      return { data, total, page, page_size };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  // ── Quick-add employee with Kenya salary structure assignment ────────────

  app.post("/v1/payroll/team", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR only" });

    const body = (req.body ?? {}) as Record<string, unknown>;
    const first_name = String(body.first_name ?? "").trim();
    const last_name = String(body.last_name ?? "").trim();
    const gross_salary = Number(body.gross_salary);
    const designation = String(body.designation ?? "").trim();
    const date_of_joining =
      /^\d{4}-\d{2}-\d{2}$/.test(String(body.date_of_joining ?? ""))
        ? String(body.date_of_joining)
        : new Date().toISOString().slice(0, 10);

    if (!first_name) return reply.status(400).send({ error: "first_name is required" });
    if (!gross_salary || gross_salary <= 0)
      return reply.status(400).send({ error: "gross_salary must be a positive number" });

    try {
      // Ensure Kenya structure exists (idempotent)
      const structName = await ensureKenyaStructure(ctx.creds, ctx.company);

      // Create Employee
      const empDoc = await erp.createDoc(ctx.creds, "Employee", {
        first_name,
        ...(last_name ? { last_name } : {}),
        company: ctx.company,
        date_of_joining,
        status: "Active",
        ...(designation ? { designation } : {}),
      });

      const employeeId = String(empDoc.name ?? "");

      // Upsert Salary Structure Assignment (idempotent — updates if already exists)
      await upsertSalaryStructureAssignment(ctx.creds, {
        employeeId,
        structName,
        company: ctx.company,
        from_date: date_of_joining,
        base: gross_salary,
      });

      return {
        ok: true,
        employee: employeeId,
        employee_name: [first_name, last_name].filter(Boolean).join(" "),
        base_salary: gross_salary,
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  // ── Assign Kenya structure to an existing employee ──────────────────────

  app.post("/v1/payroll/team/:id/assign", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR only" });

    const params = (req.params ?? {}) as Record<string, unknown>;
    const employeeId = String(params.id ?? "").trim();
    if (!employeeId) return reply.status(400).send({ error: "Employee ID required" });

    const body = (req.body ?? {}) as Record<string, unknown>;
    const gross_salary = Number(body.gross_salary);
    const from_date =
      /^\d{4}-\d{2}-\d{2}$/.test(String(body.from_date ?? ""))
        ? String(body.from_date)
        : new Date().toISOString().slice(0, 10);

    if (!gross_salary || gross_salary <= 0)
      return reply.status(400).send({ error: "gross_salary must be a positive number" });

    try {
      // ── Step 1: fetch employee + warm/validate Kenya structure in parallel ──
      // ensureKenyaStructure returns from in-process cache on subsequent calls
      // (~0 ms), or does one ERPNext round-trip on the first call of the server
      // session. Running it concurrently with getDoc(Employee) means the common
      // path (cache hit) adds zero latency here.
      const [empDoc, structName] = await Promise.all([
        erp.getDoc(ctx.creds, "Employee", employeeId),
        ensureKenyaStructure(ctx.creds, ctx.company),
      ]);

      if (String(empDoc.company ?? "") !== ctx.company)
        return reply.status(403).send({ error: "Employee not in your Company" });

      // ── Step 2: fix department if needed (single getList — no wasted 404) ──
      // Frappe HRMS validates the department link when creating an SSA. The
      // employee record may hold a bare name like "hr" while Frappe stores the
      // doc as "hr - TA1". We resolve with one getList call (skipping the
      // guaranteed-to-fail getDoc), then update the employee in parallel with
      // the SSA upsert when possible.
      // Skip entirely if the name already contains " - " (already canonical).
      const empDept = String(empDoc.department ?? "").trim();
      if (empDept && !empDept.includes(" - ")) {
        const deptMatches = await erp.getList(ctx.creds, "Department", {
          filters: [["department_name", "like", empDept]],
          fields: ["name"],
          limit_page_length: 1,
        });

        let resolvedName: string;
        if (deptMatches.length > 0) {
          resolvedName = String((deptMatches[0] as { name: string }).name);
        } else {
          // Not found by department_name — check exact doc name (already canonical?)
          let exactExists = false;
          try { await erp.getDoc(ctx.creds, "Department", empDept); exactExists = true; }
          catch (e) { if (!(e instanceof ErpError) || e.status !== 404) throw e; }

          if (exactExists) {
            resolvedName = empDept;
          } else {
            // Genuinely missing — create. Frappe names the doc "{dept} - {abbr}".
            console.log(`[payroll] Creating missing Department: "${empDept}"`);
            let created: unknown;
            try {
              created = await erp.callMethod(ctx.creds, "frappe.client.insert", {
                doc: { doctype: "Department", department_name: empDept, company: ctx.company },
              });
            } catch (e: unknown) {
              if (!(e instanceof ErpError) || e.status !== 409) throw e;
              const existing = await erp.getList(ctx.creds, "Department", {
                filters: [["department_name", "like", empDept]],
                fields: ["name"],
                limit_page_length: 1,
              });
              created = existing[0] ?? { name: empDept };
            }
            resolvedName = String((created as Record<string, unknown>)?.name ?? empDept);
          }
        }

        if (resolvedName !== empDept) {
          console.log(`[payroll] Updating employee department: "${empDept}" → "${resolvedName}"`);
          await erp.callMethod(ctx.creds, "frappe.client.set_value", {
            doctype: "Employee",
            name: employeeId,
            fieldname: "department",
            value: resolvedName,
          });
        }
      }

      // ── Step 3: upsert + submit assignment ───────────────────────────────────
      await upsertSalaryStructureAssignment(ctx.creds, {
        employeeId,
        structName,
        company: ctx.company,
        from_date,
        base: gross_salary,
      });

      return { ok: true, employee: employeeId, base_salary: gross_salary };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  // ── Generate + submit salary slips for a period ──────────────────────────

  app.post("/v1/payroll/run", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR only" });

    const body = (req.body ?? {}) as Record<string, unknown>;
    const start_date = parseDate(body.start_date);
    const end_date = parseDate(body.end_date);

    if (!start_date) return reply.status(400).send({ error: "start_date required (YYYY-MM-DD)" });
    if (!end_date) return reply.status(400).send({ error: "end_date required (YYYY-MM-DD)" });

    const employeeFilter = String(body.employee ?? "").trim();

    try {
      const runYear = new Date(start_date).getFullYear();

      // Run all three setup checks in parallel — they're independent of each other.
      // All three are cached after the first successful run so repeat clicks are ~0 ms.
      let structName: string;
      try {
        [structName] = await Promise.all([
          ensureKenyaStructure(ctx.creds, ctx.company),
          ensureCompanyFiscalYear(ctx.creds, ctx.company, runYear),
          ensureCompanyHolidayList(ctx.creds, ctx.company, runYear),
        ]);
      } catch (e) {
        if (e instanceof ErpError) return replyErp(reply, e);
        throw e;
      }

      // Fetch only employees who have an active salary structure assignment —
      // no point touching the other 190+ employees at all.
      // Run both lookups in parallel to save a round-trip.
      const assignmentFilters: unknown[] = [
        ["company", "=", ctx.company],
        ["from_date", "<=", end_date],
        ["salary_structure", "=", structName],
      ];
      if (employeeFilter) assignmentFilters.push(["employee", "=", employeeFilter]);

      const [assignmentRows, existingSlipRows] = await Promise.all([
        erp.getList(ctx.creds, "Salary Structure Assignment", {
          filters: assignmentFilters,
          fields: ["name", "employee", "employee_name", "from_date", "docstatus"],
          order_by: "from_date desc",   // latest assignment first for dedup
          limit_page_length: 2000,
        }),
        erp.getList(ctx.creds, "Salary Slip", {
          filters: [
            ["company", "=", ctx.company],
            // Use end_date (consistent for all employees) rather than start_date
            // which varies for mid-month joiners whose slip starts on their
            // assignment from_date rather than the period start.
            ["end_date", "=", end_date],
            ["docstatus", "!=", 2],
          ],
          fields: ["name", "employee", "docstatus"],
          limit_page_length: 2000,
        }),
      ]);

      if (assignmentRows.length === 0) {
        return reply.status(400).send({
          error: "No employees have a salary assigned yet. Go to the Team tab and set each employee's salary first.",
        });
      }

      // Deduplicate by employee (an employee may have multiple assignments; use the latest)
      const seen = new Set<string>();
      const toProcess = (assignmentRows as Record<string, unknown>[]).filter((a) => {
        const id = String(a.employee ?? "");
        if (seen.has(id)) return false;
        seen.add(id);
        return true;
      });

      // Map employee → existing slip info (name + docstatus) so we can submit drafts
      const existingSlipByEmployee = new Map(
        (existingSlipRows as Record<string, unknown>[]).map((s) => [
          String(s.employee),
          { name: String(s.name ?? ""), docstatus: Number(s.docstatus ?? 0) },
        ])
      );

      type RunResult = {
        employee: string;
        employee_name: string;
        slip?: string;
        skipped?: boolean;
        skip_reason?: string;
        error?: string;
      };

      // Auto-submit any draft assignments (docstatus=0) so Frappe's payroll query
      // (which filters WHERE ssa.docstatus = 1) can find them.
      const draftAssignments = toProcess.filter(
        (a) => Number((a as Record<string, unknown>).docstatus ?? 0) === 0
      );
      if (draftAssignments.length > 0) {
        console.log(`[payroll] Submitting ${draftAssignments.length} draft assignment(s)…`);
        await Promise.all(
          draftAssignments.map(async (a) => {
            const aName = String((a as Record<string, unknown>).name ?? "");
            if (!aName) return;
            try {
              await submitWithRetry(ctx.creds, "Salary Structure Assignment", aName);
              console.log(`[payroll] Assignment ${aName} submitted`);
            } catch (e) {
              console.log(`[payroll] Could not submit assignment ${aName}: ${e instanceof Error ? e.message : String(e)}`);
            }
          })
        );
      }

      // Process concurrently in batches of 6 — keeps Frappe load reasonable while
      // cutting total time to ~(ceil(n/6) × 4 s) instead of n × 4 s.
      const CONCURRENCY = 6;
      const queue = [...toProcess];
      const results: RunResult[] = [];

      while (queue.length > 0) {
        const batch = queue.splice(0, CONCURRENCY);
        const batchResults = await Promise.all(
          batch.map(async (emp) => {
            const employeeId = String(emp.employee ?? emp.name ?? "");
            const employeeName = String(emp.employee_name ?? employeeId);

            // If a slip already exists for this period, submit it if draft or skip if submitted
            const existingSlip = existingSlipByEmployee.get(employeeId);
            if (existingSlip) {
              if (existingSlip.docstatus === 0) {
                // Draft left over from a previous run that failed mid-submit — submit it now
                console.log(`[payroll] Found draft slip ${existingSlip.name} for ${employeeId} — submitting`);
                try {
                  await submitWithRetry(ctx.creds, "Salary Slip", existingSlip.name);
                  return { employee: employeeId, employee_name: employeeName, slip: existingSlip.name } as RunResult;
                } catch (e) {
                  const msg = e instanceof ErpError ? (parseFrappeErrorBody(e.body) ?? e.message) : String(e);
                  return { employee: employeeId, employee_name: employeeName, error: msg } as RunResult;
                }
              }
              // Already submitted (docstatus=1) — skip
              return { employee: employeeId, employee_name: employeeName, skipped: true } as RunResult;
            }

            try {
              // If the employee's assignment starts after the period start (mid-month
              // joiner), use their assignment from_date so Frappe can find the structure.
              const assignmentFrom = String(emp.from_date ?? "");
              const slipStart = assignmentFrom > start_date ? assignmentFrom : start_date;
              console.log(`[payroll] slip for ${employeeId}: assignmentFrom=${assignmentFrom} slipStart=${slipStart}`);

              const slip = await erp.createDoc(ctx.creds, "Salary Slip", {
                naming_series: "Sal Slip/.YYYY.-.MM.-.#####",
                employee: employeeId,
                salary_structure: structName,
                company: ctx.company,
                start_date: slipStart,
                end_date,
                posting_date: end_date,
                currency: "KES",
              });
              const slipName = String(slip.name ?? "");
              // createDoc returns the full document — pass it to skip the redundant
              // getDoc inside submitWithRetry on the first attempt (~400 ms × N employees).
              await submitWithRetry(ctx.creds, "Salary Slip", slipName, 4, slip);
              return { employee: employeeId, employee_name: employeeName, slip: slipName } as RunResult;
            } catch (e) {
              // Frappe may still reject with "already created" if a concurrent request
              // or a slip with a different start_date exists. Find and submit/skip it.
              const msg = e instanceof ErpError ? (parseFrappeErrorBody(e.body) ?? e.message) : String(e);
              if (typeof msg === "string" && msg.toLowerCase().includes("already created")) {
                const existingRows = await erp.getList(ctx.creds, "Salary Slip", {
                  filters: [
                    ["employee", "=", employeeId],
                    ["end_date", "=", end_date],
                    ["docstatus", "!=", 2],
                  ],
                  fields: ["name", "docstatus"],
                  limit_page_length: 1,
                }).catch(() => [] as unknown[]);
                const existing = (existingRows as Record<string, unknown>[])[0];
                if (existing) {
                  const eName = String(existing.name ?? "");
                  const eDs = Number(existing.docstatus ?? 0);
                  if (eDs === 0) {
                    try {
                      await submitWithRetry(ctx.creds, "Salary Slip", eName);
                      return { employee: employeeId, employee_name: employeeName, slip: eName } as RunResult;
                    } catch (se) {
                      const sm = se instanceof ErpError ? (parseFrappeErrorBody(se.body) ?? se.message) : String(se);
                      return { employee: employeeId, employee_name: employeeName, error: sm } as RunResult;
                    }
                  }
                  // Already submitted — count as skipped
                  return { employee: employeeId, employee_name: employeeName, skipped: true } as RunResult;
                }
              }
              return { employee: employeeId, employee_name: employeeName, error: msg } as RunResult;
            }
          })
        );
        results.push(...batchResults);
      }

      return {
        ok: true,
        period: { start_date, end_date },
        created: results.filter((r) => r.slip).length,
        skipped: results.filter((r) => r.skipped).length,
        failed: results.filter((r) => r.error).length,
        results,
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  // ── Existing read routes ──────────────────────────────────────────────────

  /**
   * HR: salary slips whose pay period overlaps [from_date, to_date].
   */
  app.get("/v1/payroll/salary-slips", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "Only HR can list salary slips for the company." });
    }

    const q = (req.query ?? {}) as Record<string, unknown>;
    const qEmp = String(q.employee ?? "").trim();
    const from = parseDate(q.from_date ?? q.from);
    const to = parseDate(q.to_date ?? q.to);
    if (!from || !to)
      return reply.status(400).send({ error: "from_date and to_date are required (YYYY-MM-DD)" });

    try {
      const employeeId = qEmp ? await resolveEmployeeIdForRequest(ctx, qEmp) : null;
      if (qEmp && !employeeId) return reply.status(403).send({ error: "Employee not in your Company" });

      const filters: unknown[] = [
        ["company", "=", ctx.company],
        ["docstatus", "!=", 2],
        ["start_date", "<=", to],
        ["end_date", ">=", from],
      ];
      if (employeeId) filters.push(["employee", "=", employeeId]);

      const rows = (await erp.getList(ctx.creds, "Salary Slip", {
        fields: [
          "name",
          "employee",
          "employee_name",
          "company",
          "posting_date",
          "start_date",
          "end_date",
          "currency",
          "status",
          "docstatus",
        ],
        filters,
        order_by: "start_date desc, employee asc",
        limit_page_length: 200,
      })) as Record<string, unknown>[];

      return { data: rows };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** HR: single salary slip (full doc as ERP returns it). */
  app.get("/v1/payroll/salary-slips/:name", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "Only HR can view salary slip detail." });
    }

    const params = (req.params ?? {}) as Record<string, unknown>;
    const name = String(params.name ?? "").trim();
    if (!name) return reply.status(400).send({ error: "Salary Slip name is required" });

    try {
      const doc = await erp.getDoc(ctx.creds, "Salary Slip", name);
      if (String(doc.company ?? "") !== ctx.company) {
        return reply.status(403).send({ error: "Salary Slip not in your Company" });
      }
      return { data: doc };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** HR: payroll entry documents (runs) overlapping the date range. */
  app.get("/v1/payroll/payroll-entries", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "Only HR can list payroll entries." });
    }

    const q = (req.query ?? {}) as Record<string, unknown>;
    const from = parseDate(q.from_date ?? q.from);
    const to = parseDate(q.to_date ?? q.to);
    if (!from || !to)
      return reply.status(400).send({ error: "from_date and to_date are required (YYYY-MM-DD)" });

    try {
      const filters: unknown[] = [
        ["company", "=", ctx.company],
        ["docstatus", "!=", 2],
        ["start_date", "<=", to],
        ["end_date", ">=", from],
      ];

      const rows = (await erp.getList(ctx.creds, "Payroll Entry", {
        fields: [
          "name",
          "company",
          "posting_date",
          "start_date",
          "end_date",
          "currency",
          "exchange_rate",
          "payroll_frequency",
          "status",
          "docstatus",
        ],
        filters,
        order_by: "start_date desc",
        limit_page_length: 100,
      })) as Record<string, unknown>[];

      return { data: rows };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });
};
