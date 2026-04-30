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
 * Kenya PAYE formula — KRA Finance Act 2023.
 * Taxable income = gross_pay − NSSF − SHIF − HL  (KRA-compliant: statutory
 * deductions reduce the PAYE base before bands are applied).
 * NSSF, SHIF, and HL are referenced by their component abbreviations; they
 * must be evaluated BEFORE PAYE in the salary structure deductions list.
 *
 * Tax bands: 0-24k@10%, 24k-32333@25%, 32333-500k@30%, 500k-800k@32.5%, 800k+@35%
 * Personal relief: KES 2,400/month
 *
 * Algebra shortcut: when taxable ≤ 24,000 the first band (10%) ≤ 2,400 so
 * personal relief fully covers the tax → PAYE = 0.
 * When taxable > 24,000 the first band is exactly 2,400 which cancels the
 * personal relief, so PAYE = round(band2 + band3 + band4 + band5, 2).
 *
 * Frappe safe_eval has no max()/min() — ternary expressions are used instead.
 */
const _T = "gross_pay-NSSF-SHIF-HL"; // taxable income variable (inlined)
const PAYE_FORMULA =
  `(0 if (${_T})<=24000 else round(` +
  `(((${_T}) if (${_T})<32333 else 32333)-24000)*0.25+` +
  `(0 if (${_T})<=32333 else (((${_T}) if (${_T})<500000 else 500000)-32333)*0.30)+` +
  `(0 if (${_T})<=500000 else (((${_T}) if (${_T})<800000 else 800000)-500000)*0.325)+` +
  `(0 if (${_T})<=800000 else ((${_T})-800000)*0.35)` +
  `,2))`;

/**
 * NSSF Phase 3 (effective Dec 2024): 6% of gross, pensionable pay ceiling
 * raised to KES 108,000 → max employee contribution KES 6,480/month.
 * Frappe sandbox has no min() — use ternary.
 */
const NSSF_FORMULA = "(round(gross_pay * 0.06, 2) if gross_pay < 108000 else 6480)";

/** SHIF (Social Health Insurance Fund): 2.75% of gross */
const SHIF_FORMULA = "round(gross_pay * 0.0275, 2)";

/** Affordable Housing Levy (employee share): 1.5% of gross */
const HOUSING_LEVY_FORMULA = "round(gross_pay * 0.015, 2)";

/**
 * Overtime Pay formula.
 * overtime_hours is populated on the Salary Slip when a Timesheet is linked.
 * Without a linked timesheet the field is 0, so the component contributes nothing.
 * Rate: 1.5 × daily rate (base ÷ 22 working days ÷ 8 hours per day).
 */
const OVERTIME_FORMULA = "round(overtime_hours * (base / 22 / 8) * 1.5, 2)";

// ── Setup Helpers ─────────────────────────────────────────────────────────────

/**
 * Ensure a Salary Component exists (earning or deduction).
 * If it already exists (any abbr/type), we leave it as-is.
 */
async function ensureSalaryComponent(
  creds: ErpCredentials,
  name: string,
  type: "Earning" | "Deduction",
  abbr: string,
  opts: { depends_on_payment_days?: 0 | 1 } = {}
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
      ...(opts.depends_on_payment_days !== undefined
        ? { depends_on_payment_days: opts.depends_on_payment_days }
        : {}),
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
      {
        // Overtime: populated only when a Timesheet is linked to the Salary Slip.
        // When no timesheet is linked, overtime_hours = 0 → component contributes KES 0.
        doctype: "Salary Detail",
        salary_component: "Overtime Pay",
        abbr: "OT",
        amount_based_on_formula: 1,
        formula: OVERTIME_FORMULA,
        idx: 2,
      },
    ],
    deductions: [
      // NSSF, SHIF, and HL are evaluated first (idx 1-3) so their computed
      // amounts are available as variables (NSSF, SHIF, HL) when PAYE (idx 4)
      // calculates taxable income = gross_pay − NSSF − SHIF − HL.
      {
        doctype: "Salary Detail",
        salary_component: "NSSF",
        abbr: "NSSF",
        amount_based_on_formula: 1,
        formula: NSSF_FORMULA,
        idx: 1,
      },
      {
        doctype: "Salary Detail",
        salary_component: "SHIF",
        abbr: "SHIF",
        amount_based_on_formula: 1,
        formula: SHIF_FORMULA,
        idx: 2,
      },
      {
        doctype: "Salary Detail",
        salary_component: "Housing Levy",
        abbr: "HL",
        amount_based_on_formula: 1,
        formula: HOUSING_LEVY_FORMULA,
        idx: 3,
      },
      {
        doctype: "Salary Detail",
        salary_component: "PAYE",
        abbr: "PAYE",
        amount_based_on_formula: 1,
        formula: PAYE_FORMULA,
        idx: 4,
      },
    ],
  };
}

/**
 * Returns true if the salary structure needs to be rebuilt.
 * Triggers:
 *  1. Any formula uses max()/min() — not available in Frappe's safe_eval sandbox.
 *  2. NSSF formula still uses the old Phase 2 ceiling (KES 36,000 / KES 2,160).
 *  3. PAYE formula computes on gross_pay directly instead of taxable income
 *     (i.e., it doesn't reference NSSF to subtract statutory deductions first).
 *  4. PAYE is evaluated before NSSF (idx 1) — deduction order must be NSSF→SHIF→HL→PAYE.
 */
function formulasNeedPatch(doc: Record<string, unknown>): boolean {
  for (const tableKey of ["earnings", "deductions"]) {
    const rows = doc[tableKey];
    if (!Array.isArray(rows)) continue;
    for (const row of rows) {
      const r = row as Record<string, unknown>;
      if (typeof r.formula !== "string") continue;
      const comp = String(r.salary_component ?? "");
      const formula = r.formula;

      // 1. Unsupported builtins in Frappe safe_eval
      if (/\bmax\s*\(/.test(formula) || /\bmin\s*\(/.test(formula)) {
        console.log(`[payroll] Found unsupported max/min in ${comp} formula — rebuild needed`);
        return true;
      }
      // 2. Old Phase 2 NSSF cap (36,000 ceiling → 2,160 max)
      if (comp === "NSSF" && formula.includes("36000")) {
        console.log(`[payroll] Found Phase 2 NSSF formula — rebuild needed for Phase 3`);
        return true;
      }
      // 3. Old PAYE formula computes on gross_pay, not taxable income
      if (comp === "PAYE" && !formula.includes("NSSF")) {
        console.log(`[payroll] Found PAYE formula on gross_pay basis — rebuild needed for taxable income basis`);
        return true;
      }
      // 4. PAYE evaluated before NSSF (NSSF must be idx 1, PAYE idx 4)
      if (comp === "PAYE" && Number(r.idx ?? 0) < 4) {
        console.log(`[payroll] PAYE idx=${r.idx} is before NSSF — rebuild needed to fix deduction order`);
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
        // HELB is a per-employee recurring deduction added via Additional Salary,
        // not part of the base structure. Register the component master so ERPNext
        // accepts Additional Salary records that reference it.
        ensureSalaryComponent(creds, "HELB", "Deduction", "HELB", { depends_on_payment_days: 0 }),
        ensureSalaryComponent(creds, "Overtime Pay", "Earning", "OT", { depends_on_payment_days: 0 }),
        // Loan Deduction: per-employee recurring deduction via Additional Salary.
        // Not in the base structure formula; registered here so ERPNext accepts
        // Additional Salary records that reference this component.
        ensureSalaryComponent(creds, "Loan Deduction", "Deduction", "LOAN", { depends_on_payment_days: 0 }),
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
    // HELB: per-employee fixed deduction via Additional Salary; not in the base
    // structure formula. depends_on_payment_days=0 keeps the amount constant
    // even on partial-month payroll runs.
    ensureSalaryComponent(creds, "HELB", "Deduction", "HELB", { depends_on_payment_days: 0 }),
    // Overtime Pay: included in the structure formula; resolves to 0 when no
    // timesheet is linked. depends_on_payment_days=0 to keep formula-driven.
    ensureSalaryComponent(creds, "Overtime Pay", "Earning", "OT", { depends_on_payment_days: 0 }),
    // Loan Deduction: per-employee recurring deduction via Additional Salary.
    // Not in the base structure formula; registered so ERPNext accepts records.
    ensureSalaryComponent(creds, "Loan Deduction", "Deduction", "LOAN", { depends_on_payment_days: 0 }),
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

/**
 * Idempotent helper: create, update, or remove the recurring HELB Additional
 * Salary record for an employee.
 *
 * amount = 0  → cancel + delete any existing record (employee is clearing HELB)
 * amount > 0  → cancel + delete old, insert fresh recurring record
 *
 * HELB is a post-tax fixed monthly repayment; `depends_on_payment_days=0` on
 * the Salary Component master ensures ERPNext doesn't pro-rate it.
 */
async function upsertHelbAdditionalSalary(
  creds: ErpCredentials,
  employeeId: string,
  company: string,
  from_date: string,
  amount: number,
): Promise<void> {
  // Find ALL HELB Additional Salary records for this employee (any is_recurring value,
  // any docstatus except cancelled) so we clean up both old recurring and new non-recurring ones.
  const existing = (await erp.getList(creds, "Additional Salary", {
    filters: [
      ["employee", "=", employeeId],
      ["salary_component", "=", "HELB"],
      ["docstatus", "!=", 2],
    ],
    fields: ["name", "docstatus", "to_date"],
    limit_page_length: 20,
  })) as Array<{ name: string; docstatus: number; to_date?: string }>;

  console.log(`[payroll] Found ${existing.length} existing HELB record(s) for ${employeeId}`);

  // Cancel + delete each existing record.
  // Old records created without to_date: patch to_date first so ERPNext's
  // cancel validation passes (it re-validates the doc on cancel).
  for (const row of existing) {
    try {
      if (Number(row.docstatus) === 1) {
        const hasToDate = String(row.to_date ?? "").trim() !== "";
        if (!hasToDate) {
          // Best-effort: try to add to_date before cancelling
          try {
            await erp.callMethod(creds, "frappe.client.set_value", {
              doctype: "Additional Salary",
              name: row.name,
              fieldname: "to_date",
              value: "2099-12-31",
            });
            console.log(`[payroll] Patched to_date on ${row.name} before cancel`);
          } catch (patchErr) {
            // set_value may fail on submitted docs whose field lacks allow_on_submit.
            // Try an alternative: update the full doc via REST PUT.
            try {
              await erp.updateDoc(creds, "Additional Salary", row.name, {
                to_date: "2099-12-31",
              });
              console.log(`[payroll] updateDoc patched to_date on ${row.name}`);
            } catch (putErr) {
              console.log(
                `[payroll] Could not patch to_date on ${row.name} (set_value: ` +
                `${patchErr instanceof Error ? patchErr.message : String(patchErr)}, ` +
                `updateDoc: ${putErr instanceof Error ? putErr.message : String(putErr)})`
              );
            }
          }
        }
        await erp.callMethod(creds, "frappe.client.cancel", {
          doctype: "Additional Salary",
          name: row.name,
        });
      }
      await erp.deleteDoc(creds, "Additional Salary", row.name);
      console.log(`[payroll] Removed HELB record ${row.name} for ${employeeId}`);
    } catch (e) {
      console.log(
        `[payroll] Could not remove HELB record ${row.name}: ` +
        (e instanceof Error ? e.message : String(e))
      );
    }
  }

  if (amount <= 0) {
    console.log(`[payroll] HELB cleared for ${employeeId}`);
    return;
  }

  // Insert recurring HELB deduction. is_recurring:1 requires both from_date and
  // to_date — use a far-future sentinel so it applies to every future payroll run.
  await erp.callMethod(creds, "frappe.client.insert", {
    doc: {
      doctype: "Additional Salary",
      employee: employeeId,
      salary_component: "HELB",
      type: "Deduction",
      amount,
      company,
      is_recurring: 1,
      from_date,
      to_date: "2099-12-31",
      currency: "KES",
    },
  });
  console.log(`[payroll] Set HELB ${amount} KES/mo from ${from_date} for ${employeeId}`);
}

/**
 * Upsert a one-time Overtime Pay Additional Salary for an employee.
 * Used when HR inputs overtime manually for employees without a timesheet.
 *
 * amount = 0  → cancel + delete any existing OT record for this payroll_date
 * amount > 0  → cancel + delete old, insert fresh one-time earning
 *
 * Non-recurring (is_recurring: 0) — needs payroll_date (the period posting date).
 */
async function upsertOvertimeAdditionalSalary(
  creds: ErpCredentials,
  employeeId: string,
  company: string,
  payroll_date: string,
  amount: number,
): Promise<void> {
  // Find existing one-time OT Additional Salary records for this employee + date
  const existing = (await erp.getList(creds, "Additional Salary", {
    filters: [
      ["employee", "=", employeeId],
      ["salary_component", "=", "Overtime Pay"],
      ["payroll_date", "=", payroll_date],
      ["docstatus", "!=", 2],
    ],
    fields: ["name", "docstatus"],
    limit_page_length: 10,
  })) as Array<{ name: string; docstatus: number }>;

  for (const row of existing) {
    try {
      if (Number(row.docstatus) === 1) {
        await erp.callMethod(creds, "frappe.client.cancel", {
          doctype: "Additional Salary",
          name: row.name,
        });
      }
      await erp.deleteDoc(creds, "Additional Salary", row.name);
      console.log(`[payroll] Removed Overtime Additional Salary ${row.name} for ${employeeId}`);
    } catch (e) {
      console.log(
        `[payroll] Could not remove Overtime record ${row.name}: ` +
        (e instanceof Error ? e.message : String(e))
      );
    }
  }

  if (amount <= 0) {
    console.log(`[payroll] Overtime cleared for ${employeeId} on ${payroll_date}`);
    return;
  }

  await erp.callMethod(creds, "frappe.client.insert", {
    doc: {
      doctype: "Additional Salary",
      employee: employeeId,
      salary_component: "Overtime Pay",
      type: "Earning",
      amount,
      company,
      is_recurring: 0,
      payroll_date,
      currency: "KES",
    },
  });
  console.log(`[payroll] Set manual Overtime Pay KES ${amount} on ${payroll_date} for ${employeeId}`);
}

/**
 * Upsert a recurring Loan Deduction Additional Salary for an employee.
 *
 * amount = 0  → cancel + delete any existing record (loan fully repaid / removed)
 * amount > 0  → cancel + delete old, insert fresh recurring record
 *
 * is_recurring:1 — applies every payroll run until the to_date sentinel.
 * depends_on_payment_days=0 on the component master keeps the amount constant.
 */
async function upsertLoanDeductionAdditionalSalary(
  creds: ErpCredentials,
  employeeId: string,
  company: string,
  from_date: string,
  amount: number,
  loanRef?: string,
): Promise<void> {
  const filters: unknown[] = [
    ["employee", "=", employeeId],
    ["salary_component", "=", "Loan Deduction"],
    ["docstatus", "!=", 2],
  ];
  if (loanRef) filters.push(["remarks", "=", loanRef]);

  const existing = (await erp.getList(creds, "Additional Salary", {
    filters,
    fields: ["name", "docstatus", "to_date"],
    limit_page_length: 20,
  })) as Array<{ name: string; docstatus: number; to_date?: string }>;

  console.log(`[payroll] Found ${existing.length} existing Loan Deduction record(s) for ${employeeId}`);

  for (const row of existing) {
    try {
      if (Number(row.docstatus) === 1) {
        const hasToDate = String(row.to_date ?? "").trim() !== "";
        if (!hasToDate) {
          try {
            await erp.callMethod(creds, "frappe.client.set_value", {
              doctype: "Additional Salary",
              name: row.name,
              fieldname: "to_date",
              value: "2099-12-31",
            });
          } catch {
            try {
              await erp.updateDoc(creds, "Additional Salary", row.name, { to_date: "2099-12-31" });
            } catch { /* best-effort */ }
          }
        }
        await erp.callMethod(creds, "frappe.client.cancel", {
          doctype: "Additional Salary",
          name: row.name,
        });
      }
      await erp.deleteDoc(creds, "Additional Salary", row.name);
      console.log(`[payroll] Removed Loan Deduction record ${row.name} for ${employeeId}`);
    } catch (e) {
      console.log(
        `[payroll] Could not remove Loan Deduction record ${row.name}: ` +
        (e instanceof Error ? e.message : String(e))
      );
    }
  }

  if (amount <= 0) {
    console.log(`[payroll] Loan Deduction cleared for ${employeeId}`);
    return;
  }

  const doc: Record<string, unknown> = {
    doctype: "Additional Salary",
    employee: employeeId,
    salary_component: "Loan Deduction",
    type: "Deduction",
    amount,
    company,
    is_recurring: 1,
    from_date,
    to_date: "2099-12-31",
    currency: "KES",
  };
  if (loanRef) doc.remarks = loanRef;

  await erp.callMethod(creds, "frappe.client.insert", { doc });
  console.log(`[payroll] Set Loan Deduction KES ${amount}/mo from ${from_date} for ${employeeId}${loanRef ? ` (loan: ${loanRef})` : ""}`);
}

// ── Recurring earning-type allowance upsert ───────────────────────────────────

const EARNING_ALLOWANCE_COMPONENTS: Record<string, { abbr: string; label: string }> = {
  house_allowance:      { abbr: "HOUSE", label: "House Allowance" },
  transport_allowance:  { abbr: "TRANS", label: "Transport Allowance" },
  meal_allowance:       { abbr: "MEAL",  label: "Meal Allowance" },
  directors_fee:        { abbr: "DIR",   label: "Directors Fee" },
  reimbursement:        { abbr: "REIMB", label: "Reimbursement" },
};

/**
 * Upsert a recurring earning Additional Salary record for a single component.
 * amount = 0 → cancel + delete existing record.
 * amount > 0 → cancel + delete old, insert fresh recurring record.
 */
async function upsertRecurringEarningAllowance(
  creds: ErpCredentials,
  employeeId: string,
  company: string,
  componentName: string,
  from_date: string,
  amount: number,
): Promise<void> {
  const existing = (await erp.getList(creds, "Additional Salary", {
    filters: [
      ["employee", "=", employeeId],
      ["salary_component", "=", componentName],
      ["docstatus", "!=", 2],
    ],
    fields: ["name", "docstatus", "to_date"],
    limit_page_length: 20,
  })) as Array<{ name: string; docstatus: number; to_date?: string }>;

  for (const row of existing) {
    try {
      if (Number(row.docstatus) === 1) {
        if (!String(row.to_date ?? "").trim()) {
          try {
            await erp.callMethod(creds, "frappe.client.set_value", {
              doctype: "Additional Salary", name: row.name, fieldname: "to_date", value: "2099-12-31",
            });
          } catch {
            await erp.updateDoc(creds, "Additional Salary", row.name, { to_date: "2099-12-31" }).catch(() => {});
          }
        }
        await erp.callMethod(creds, "frappe.client.cancel", { doctype: "Additional Salary", name: row.name });
      }
      await erp.deleteDoc(creds, "Additional Salary", row.name);
    } catch (e) {
      console.log(`[payroll] Could not remove ${componentName} record ${row.name}: ${e instanceof Error ? e.message : String(e)}`);
    }
  }

  if (amount <= 0) return;

  await erp.callMethod(creds, "frappe.client.insert", {
    doc: {
      doctype: "Additional Salary",
      employee: employeeId,
      salary_component: componentName,
      type: "Earning",
      amount,
      company,
      is_recurring: 1,
      from_date,
      to_date: "2099-12-31",
      currency: "KES",
    },
  });
  console.log(`[payroll] Set ${componentName} KES ${amount}/mo from ${from_date} for ${employeeId}`);
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
      const [employees, assignments, helbRows, overtimeRows, countResult] = await Promise.all([
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
        erp.getList(ctx.creds, "Additional Salary", {
          filters: [
            ["company", "=", ctx.company],
            ["salary_component", "=", "HELB"],
            ["docstatus", "!=", 2],
          ],
          fields: ["employee", "amount"],
          order_by: "modified desc",
          limit_page_length: 1000,
        }),
        erp.getList(ctx.creds, "Additional Salary", {
          filters: [
            ["company", "=", ctx.company],
            ["salary_component", "=", "Overtime Pay"],
            ["docstatus", "!=", 2],
          ],
          fields: ["employee", "amount", "payroll_date"],
          order_by: "payroll_date desc, modified desc",
          limit_page_length: 1000,
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
      const helbByEmployee = new Map<string, number>();
      for (const row of helbRows as Record<string, unknown>[]) {
        const emp = String(row.employee ?? "");
        if (!emp || helbByEmployee.has(emp)) continue;
        const amount = Number(row.amount ?? 0);
        helbByEmployee.set(emp, Number.isFinite(amount) ? amount : 0);
      }
      const overtimeByEmployee = new Map<string, number>();
      for (const row of overtimeRows as Record<string, unknown>[]) {
        const emp = String(row.employee ?? "");
        if (!emp || overtimeByEmployee.has(emp)) continue;
        const amount = Number(row.amount ?? 0);
        if (!Number.isFinite(amount) || amount <= 0) {
          overtimeByEmployee.set(emp, 0);
          continue;
        }
        const asgn = latestAssignment.get(emp);
        const base = Number(asgn?.base ?? 0);
        const hourly = base > 0 ? base / 22 / 8 : 0;
        const estimatedHours = hourly > 0 ? amount / (hourly * 1.5) : 0;
        overtimeByEmployee.set(emp, Math.max(0, Math.round(estimatedHours * 100) / 100));
      }

      const kenyaStruct = kenyaStructureName(ctx.company);
      const data = (employees as Record<string, unknown>[]).map((emp) => {
        const employeeId = String(emp.name ?? "");
        const asgn = latestAssignment.get(employeeId);
        return {
          ...emp,
          base_salary: asgn ? Number(asgn.base) : null,
          salary_structure: asgn ? String(asgn.salary_structure ?? "") : null,
          on_kenya_structure: asgn
            ? String(asgn.salary_structure ?? "") === kenyaStruct
            : false,
          helb_monthly: helbByEmployee.get(employeeId) ?? 0,
          overtime_hours: overtimeByEmployee.get(employeeId) ?? 0,
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
    const helb_monthly = Number(body.helb_monthly ?? 0) || 0;
    const overtime_hours =
      "overtime_hours" in body ? Math.max(0, Number(body.overtime_hours) || 0) : -1;
    const overtimeProvided = overtime_hours >= 0;

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

      // HELB must run before the SSA upsert: the SSA cancel step triggers ERPNext
      // on_cancel hooks that re-validate existing Additional Salary records. Any
      // stuck HELB record (no to_date) must be cleaned up first to avoid a 417.
      if (helb_monthly > 0) {
        await upsertHelbAdditionalSalary(ctx.creds, employeeId, ctx.company, date_of_joining, helb_monthly);
      }
      await upsertSalaryStructureAssignment(ctx.creds, {
        employeeId,
        structName,
        company: ctx.company,
        from_date: date_of_joining,
        base: gross_salary,
      });
      if (overtimeProvided) {
        const hourlyRate = gross_salary > 0 ? gross_salary / 22 / 8 : 0;
        const overtimeAmount = Math.round(overtime_hours * hourlyRate * 1.5 * 100) / 100;
        await upsertOvertimeAdditionalSalary(
          ctx.creds,
          employeeId,
          ctx.company,
          date_of_joining,
          overtimeAmount,
        );
      }

      return {
        ok: true,
        employee: employeeId,
        employee_name: [first_name, last_name].filter(Boolean).join(" "),
        base_salary: gross_salary,
        ...(helb_monthly > 0 ? { helb_monthly } : {}),
        ...(overtimeProvided ? { overtime_hours } : {}),
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
    // helb_monthly: undefined/absent = leave HELB unchanged; 0 = clear HELB; >0 = set/update
    const helb_monthly = "helb_monthly" in body ? Number(body.helb_monthly) || 0 : -1;
    const helbProvided = helb_monthly >= 0; // true if caller explicitly sent the field
    // overtime_hours: undefined/absent = leave overtime unchanged; 0 = clear; >0 = set/update
    const overtime_hours =
      "overtime_hours" in body ? Math.max(0, Number(body.overtime_hours) || 0) : -1;
    const overtimeProvided = overtime_hours >= 0;

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

      // ── Step 3: Clear old Additional Salary records BEFORE SSA cancel ─────────
      // ERPNext's on_cancel hook for Salary Structure Assignment re-validates all
      // existing Additional Salary records. If a HELB/OT record is stuck (e.g.
      // missing to_date), the SSA cancel fails. We clear them first (amount=0 /
      // overtimeAmount=0) so the SSA cancel hook has nothing to trip over.
      // New employees have no existing records so this is a safe no-op for them.
      if (helbProvided) {
        await upsertHelbAdditionalSalary(ctx.creds, employeeId, ctx.company, from_date, 0);
      }
      if (overtimeProvided) {
        await upsertOvertimeAdditionalSalary(ctx.creds, employeeId, ctx.company, from_date, 0);
      }

      // ── Step 4: SSA — now safe to cancel/recreate ────────────────────────────
      await upsertSalaryStructureAssignment(ctx.creds, {
        employeeId,
        structName,
        company: ctx.company,
        from_date,
        base: gross_salary,
      });

      // ── Step 5: Insert new Additional Salary records after SSA is active ─────
      // ERPNext requires an active (docstatus=1) SSA before Additional Salary
      // records can be inserted/submitted for the employee.
      if (helbProvided && helb_monthly > 0) {
        await upsertHelbAdditionalSalary(ctx.creds, employeeId, ctx.company, from_date, helb_monthly);
      }
      if (overtimeProvided && overtime_hours > 0) {
        const hourlyRate = gross_salary > 0 ? gross_salary / 22 / 8 : 0;
        const overtimeAmount = Math.round(overtime_hours * hourlyRate * 1.5 * 100) / 100;
        await upsertOvertimeAdditionalSalary(
          ctx.creds,
          employeeId,
          ctx.company,
          from_date,
          overtimeAmount,
        );
      }

      return {
        ok: true,
        employee: employeeId,
        base_salary: gross_salary,
        ...(helbProvided ? { helb_monthly } : {}),
        ...(overtimeProvided ? { overtime_hours } : {}),
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  // ── Recurring earning allowances — per employee write ─────────────────────
  //
  // PATCH /v1/payroll/team/:id/recurring-allowances
  // Body: { house_allowance?, transport_allowance?, meal_allowance?, directors_fee?, reimbursement?, from_date? }
  // Upserts Additional Salary (Earning, is_recurring=1) for each field supplied.
  // 0 = clear, >0 = set/update. Idempotent — safe to call on every allowance edit.

  app.patch("/v1/payroll/team/:id/recurring-allowances", async (req, reply) => {
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
    const from_date = String(body.from_date ?? new Date().toISOString().slice(0, 10));

    // Collect only fields that were explicitly provided
    const updates: Array<{ key: string; componentName: string; amount: number }> = [];
    for (const [key, meta] of Object.entries(EARNING_ALLOWANCE_COMPONENTS)) {
      if (key in body) {
        updates.push({ key, componentName: meta.label, amount: Math.max(0, Number(body[key]) || 0) });
      }
    }

    if (updates.length === 0)
      return reply.status(400).send({ error: "At least one allowance field is required" });

    try {
      // Verify employee belongs to this company
      const empDoc = await erp.getDoc(ctx.creds, "Employee", employeeId);
      if (String(empDoc.company ?? "") !== ctx.company)
        return reply.status(403).send({ error: "Employee not in your Company" });

      // Ensure all required salary components exist in ERPNext, then upsert
      await Promise.all(
        updates.map(({ componentName }) => {
          const meta = Object.values(EARNING_ALLOWANCE_COMPONENTS).find((m) => m.label === componentName);
          return ensureSalaryComponent(ctx.creds, componentName, "Earning", meta?.abbr ?? componentName.slice(0, 5).toUpperCase());
        }),
      );

      await Promise.all(
        updates.map(({ componentName, amount }) =>
          upsertRecurringEarningAllowance(ctx.creds, employeeId, ctx.company, componentName, from_date, amount),
        ),
      );

      const result: Record<string, number> = {};
      for (const { key, amount } of updates) result[key] = amount;
      return { ok: true, employee: employeeId, from_date, updated: result };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  // ── HELB recurring assignments — bulk read ──────────────────────────────
  //
  // GET /v1/payroll/loan-deductions?employees=EMP001,EMP002,...
  // Returns { data: { "EMP001": 5000, "EMP002": 0, ... } } — the current recurring
  // monthly "Loan Deduction" amount per employee. Employees with no active record → 0.

  app.get("/v1/payroll/loan-deductions", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR only" });

    const q = (req.query ?? {}) as Record<string, unknown>;
    const rawEmployees = String(q.employees ?? "").trim();
    const employeeIds = rawEmployees
      ? rawEmployees.split(",").map((s) => s.trim()).filter(Boolean)
      : [];

    if (employeeIds.length === 0) return { data: {} };

    try {
      const rows = (await erp.getList(ctx.creds, "Additional Salary", {
        filters: [
          ["employee", "in", employeeIds],
          ["salary_component", "=", "Loan Deduction"],
          ["is_recurring", "=", 1],
          ["docstatus", "!=", 2],
        ],
        fields: ["employee", "amount"],
        limit_page_length: (employeeIds.length * 3) + 10,
      })) as Array<{ employee: string; amount: number }>;

      // Per employee: sum all active loan deduction records (support for multiple loans).
      const data: Record<string, number> = Object.fromEntries(employeeIds.map((id) => [id, 0]));
      for (const row of rows) {
        const empId = String(row.employee ?? "");
        const amt = Number(row.amount ?? 0);
        if (empId && empId in data) data[empId] = (data[empId] ?? 0) + amt;
      }

      return { data };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  // GET /v1/payroll/helb-assignments?employees=EMP001,EMP002,...
  // Returns { data: { "EMP001": 2000, "EMP002": 0, ... } } keyed by employee ID.
  // Employees with no active HELB record are returned as 0.

  app.get("/v1/payroll/helb-assignments", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR only" });

    const q = (req.query ?? {}) as Record<string, unknown>;
    const rawEmployees = String(q.employees ?? "").trim();
    const employeeIds = rawEmployees
      ? rawEmployees.split(",").map((s) => s.trim()).filter(Boolean)
      : [];

    if (employeeIds.length === 0) return { data: {} };

    try {
      const rows = (await erp.getList(ctx.creds, "Additional Salary", {
        filters: [
          ["employee", "in", employeeIds],
          ["salary_component", "=", "HELB"],
          ["is_recurring", "=", 1],
          ["docstatus", "!=", 2],
        ],
        fields: ["employee", "amount"],
        limit_page_length: employeeIds.length + 10,
      })) as Array<{ employee: string; amount: number }>;

      // Build lookup — if multiple records per employee (shouldn't happen after upsert),
      // take the highest active amount.
      const data: Record<string, number> = Object.fromEntries(employeeIds.map((id) => [id, 0]));
      for (const row of rows) {
        const empId = String(row.employee ?? "");
        const amt = Number(row.amount ?? 0);
        if (empId && amt > (data[empId] ?? 0)) data[empId] = amt;
      }

      return { data };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  // ── HELB recurring assignment — single employee write ────────────────────
  //
  // PATCH /v1/payroll/team/:id/helb
  // Body: { helb_monthly: number }  (0 = clear, >0 = set/update)
  // Uses today as from_date; idempotent.

  app.patch("/v1/payroll/team/:id/helb", async (req, reply) => {
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
    if (!("helb_monthly" in body))
      return reply.status(400).send({ error: "helb_monthly is required" });

    const helb_monthly = Number(body.helb_monthly) || 0;
    const from_date = new Date().toISOString().slice(0, 10);

    try {
      // Verify employee belongs to this company
      const empDoc = await erp.getDoc(ctx.creds, "Employee", employeeId);
      if (String(empDoc.company ?? "") !== ctx.company)
        return reply.status(403).send({ error: "Employee not in your Company" });

      await upsertHelbAdditionalSalary(ctx.creds, employeeId, ctx.company, from_date, helb_monthly);
      return { ok: true, employee: employeeId, helb_monthly };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  // ── Manual overtime — single employee write ─────────────────────────────
  //
  // PATCH /v1/payroll/team/:id/overtime
  // Body: { overtime_hours, payroll_date?, overtime_rate? }
  // Calculates amount = hours × (base/22/8) × 1.5 and upserts Additional Salary.
  // Set overtime_hours = 0 to clear manual overtime for the period.

  app.patch("/v1/payroll/team/:id/overtime", async (req, reply) => {
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
    if (!("overtime_hours" in body))
      return reply.status(400).send({ error: "overtime_hours is required" });

    const overtime_hours = Math.max(0, Number(body.overtime_hours) || 0);
    const payroll_date = parseDate(body.payroll_date) || new Date().toISOString().slice(0, 10);

    try {
      // Fetch employee to verify company and get base salary for rate calculation
      const [empDoc, assignmentRows] = await Promise.all([
        erp.getDoc(ctx.creds, "Employee", employeeId),
        erp.getList(ctx.creds, "Salary Structure Assignment", {
          filters: [
            ["employee", "=", employeeId],
            ["docstatus", "=", 1],
          ],
          fields: ["base"],
          order_by: "from_date desc",
          limit_page_length: 1,
        }),
      ]);

      if (String(empDoc.company ?? "") !== ctx.company)
        return reply.status(403).send({ error: "Employee not in your Company" });

      // Use caller-supplied rate, or derive from latest base salary assignment
      const base = Number((assignmentRows[0] as Record<string, unknown>)?.base ?? 0);
      const hourly_rate = Number(body.overtime_rate) > 0
        ? Number(body.overtime_rate)
        : base > 0 ? base / 22 / 8 : 0;

      const amount = Math.round(overtime_hours * hourly_rate * 1.5 * 100) / 100;

      await upsertOvertimeAdditionalSalary(ctx.creds, employeeId, ctx.company, payroll_date, amount);
      return { ok: true, employee: employeeId, overtime_hours, hourly_rate, amount, payroll_date };
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

              // Look up any submitted Timesheet for this employee covering the period.
              // If found, link it so ERPNext populates overtime_hours on the slip.
              let timesheetLinks: unknown[] = [];
              try {
                const tsRows = (await erp.getList(ctx.creds, "Timesheet", {
                  filters: [
                    ["employee", "=", employeeId],
                    ["docstatus", "=", 1],
                    ["start_date", "<=", end_date],
                    ["end_date", ">=", slipStart],
                  ],
                  fields: ["name"],
                  limit_page_length: 5,
                })) as Array<{ name: string }>;
                timesheetLinks = tsRows.map((ts, i) => ({
                  doctype: "Salary Slip Timesheet",
                  time_sheet: ts.name,
                  idx: i + 1,
                }));
                if (timesheetLinks.length > 0) {
                  console.log(`[payroll] Linking ${timesheetLinks.length} timesheet(s) for ${employeeId}`);
                }
              } catch (tsErr) {
                // Non-fatal: proceed without timesheet (overtime_hours = 0)
                console.log(`[payroll] Timesheet lookup for ${employeeId}: ${tsErr instanceof Error ? tsErr.message : String(tsErr)}`);
              }

              const slip = await erp.createDoc(ctx.creds, "Salary Slip", {
                naming_series: "Sal Slip/.YYYY.-.MM.-.#####",
                employee: employeeId,
                salary_structure: structName,
                company: ctx.company,
                start_date: slipStart,
                end_date,
                posting_date: end_date,
                currency: "KES",
                ...(timesheetLinks.length > 0 ? { timesheets: timesheetLinks } : {}),
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
          "gross_pay",
          "net_pay",
          "total_deduction",
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

      // Attach employee statutory IDs (KRA PIN, NSSF, SHIF) so the payslip
      // view/PDF can show them without a separate API call.
      let empStatutory: Record<string, string> = {};
      const empId = String(doc.employee ?? "").trim();
      if (empId) {
        try {
          const empDoc = await erp.getDoc(ctx.creds, "Employee", empId);
          empStatutory = {
            _emp_national_id: String(empDoc.custom_national_id ?? "").trim(),
            _emp_tax_id: String(empDoc.tax_id ?? empDoc.custom_kra_pin ?? "").trim(),
            _emp_nssf_number: String(empDoc.custom_nssf_number ?? "").trim(),
            _emp_shif_number: String(empDoc.custom_nhif__shif_number ?? empDoc.custom_shif_number ?? "").trim(),
            _emp_designation: String(empDoc.designation ?? "").trim(),
          };
        } catch (e) {
          // Non-fatal: statutory IDs are supplementary display info
          console.log(
            `[payroll] Could not fetch statutory IDs for employee ${empId}: ` +
            (e instanceof Error ? e.message : String(e))
          );
        }
      }

      return { data: { ...doc, ...empStatutory } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * POST /v1/loans/deductions
   * Set (upsert) a recurring Loan Deduction Additional Salary for an employee.
   * Body: { employee_id, company, from_date, monthly_amount, loan_ref? }
   */
  app.post("/v1/loans/deductions", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "Only HR admins can set loan deductions." });
    }

    const body = (req.body ?? {}) as Record<string, unknown>;
    const employeeId = String(body.employee_id ?? "").trim();
    const company = String(body.company ?? ctx.company).trim();
    const from_date = String(body.from_date ?? "").trim();
    const monthly_amount = Number(body.monthly_amount ?? 0);
    const loan_ref = body.loan_ref ? String(body.loan_ref).trim() : undefined;

    if (!employeeId) return reply.status(400).send({ error: "employee_id is required" });
    if (!/^\d{4}-\d{2}-\d{2}$/.test(from_date)) return reply.status(400).send({ error: "from_date must be YYYY-MM-DD" });
    // monthly_amount = 0 is allowed (clears the deduction); negative is not
    if (monthly_amount < 0) return reply.status(400).send({ error: "monthly_amount must be 0 (to clear) or a positive number" });

    try {
      // Verify the employee exists in ERPNext and belongs to this company
      let empDoc: Record<string, unknown>;
      try {
        empDoc = await erp.getDoc(ctx.creds, "Employee", employeeId) as Record<string, unknown>;
      } catch (e) {
        if (e instanceof ErpError && e.status === 404) {
          return reply.status(400).send({ error: `Employee "${employeeId}" not found in ERPNext` });
        }
        throw e;
      }
      if (String(empDoc.company ?? "") !== company) {
        return reply.status(400).send({ error: `Employee "${employeeId}" does not belong to company "${company}"` });
      }
      if (String(empDoc.status ?? "").toLowerCase() !== "active") {
        return reply.status(400).send({ error: `Employee "${employeeId}" is not active (status: ${empDoc.status ?? "unknown"})` });
      }

      // Ensure the Loan Deduction salary component exists before inserting Additional Salary
      await ensureSalaryComponent(ctx.creds, "Loan Deduction", "Deduction", "LOAN", { depends_on_payment_days: 0 });
      await upsertLoanDeductionAdditionalSalary(ctx.creds, employeeId, company, from_date, monthly_amount, loan_ref);
      return {
        ok: true,
        employee_id: employeeId,
        employee_name: String(empDoc.employee_name ?? ""),
        monthly_amount,
        from_date,
        action: monthly_amount === 0 ? "cleared" : "set",
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * POST /v1/payroll/salary-components/ensure
   * Idempotent: register an ERPNext Salary Component master if it doesn't exist.
   * Body: { name, type ("Earning"|"Deduction"), abbr, depends_on_payment_days? }
   */
  app.post("/v1/payroll/salary-components/ensure", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "Only HR admins can manage salary components." });
    }

    const body = (req.body ?? {}) as Record<string, unknown>;
    const name = String(body.name ?? "").trim();
    const type = String(body.type ?? "Earning").trim() as "Earning" | "Deduction";
    const abbr = String(body.abbr ?? name.toUpperCase().replace(/\s+/g, "_").slice(0, 8)).trim();
    const extraOptions: Record<string, unknown> = {};
    if (body.depends_on_payment_days != null) {
      extraOptions.depends_on_payment_days = body.depends_on_payment_days === true || body.depends_on_payment_days === 1 ? 1 : 0;
    }

    if (!name) return reply.status(400).send({ error: "name is required" });

    try {
      await ensureSalaryComponent(ctx.creds, name, type, abbr, extraOptions);
      return { ok: true, name, type, abbr };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * GET /v1/payroll/compliance-check
   * Returns a compliance status report for this company's Kenya salary structure:
   * - Whether formulas are Phase 3 NSSF (cap 6,480 at 108,000 ceiling)
   * - Whether PAYE uses taxable income basis (not raw gross_pay)
   * - Whether deduction evaluation order is correct (NSSF→SHIF→HL→PAYE)
   * - Whether any unsupported builtins (max/min) are present
   * Safe to call frequently — reads from cache where possible.
   */
  app.get("/v1/payroll/compliance-check", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR admins only" });
    }

    const structName = kenyaStructureName(ctx.company);
    const checks: Array<{ id: string; label: string; pass: boolean; detail: string }> = [];

    try {
      const existing = await erp.getDoc(ctx.creds, "Salary Structure", structName) as Record<string, unknown>;
      const ds = Number(existing.docstatus);

      checks.push({
        id: "structure_exists",
        label: "Salary Structure exists",
        pass: true,
        detail: `Found "${structName}" (docstatus=${ds})`,
      });

      checks.push({
        id: "structure_submitted",
        label: "Salary Structure is submitted",
        pass: ds === 1,
        detail: ds === 1 ? "Submitted (docstatus=1)" : `Not submitted (docstatus=${ds}) — payroll runs may fail`,
      });

      // Inspect individual components
      const allRows: Array<Record<string, unknown>> = [];
      for (const tableKey of ["earnings", "deductions"]) {
        const rows = existing[tableKey];
        if (Array.isArray(rows)) allRows.push(...rows);
      }

      let hasMax = false, nssfPhase3 = true, payeTaxable = true, deductionOrder = true;
      let payeIdx = -1, nssfIdx = -1;

      for (const r of allRows) {
        const comp = String(r.salary_component ?? "");
        const formula = typeof r.formula === "string" ? r.formula : "";
        const idx = Number(r.idx ?? 0);

        if (/\bmax\s*\(/.test(formula) || /\bmin\s*\(/.test(formula)) hasMax = true;
        if (comp === "NSSF") {
          nssfIdx = idx;
          if (formula.includes("36000")) nssfPhase3 = false;
        }
        if (comp === "PAYE") {
          payeIdx = idx;
          if (!formula.includes("NSSF")) payeTaxable = false;
          if (idx < 4) deductionOrder = false;
        }
      }

      checks.push({
        id: "no_max_min",
        label: "No unsupported builtins (max/min)",
        pass: !hasMax,
        detail: hasMax
          ? "FAIL: max() or min() found in formula — Frappe safe_eval will raise NameError"
          : "OK: only ternary expressions used",
      });

      checks.push({
        id: "nssf_phase3",
        label: "NSSF Phase 3 formula (cap KES 6,480 at KES 108,000)",
        pass: nssfPhase3,
        detail: nssfPhase3
          ? "OK: Phase 3 ceiling of 108,000 detected"
          : "FAIL: Old Phase 2 ceiling of 36,000 still in formula — run a payroll to trigger auto-rebuild",
      });

      checks.push({
        id: "paye_taxable_basis",
        label: "PAYE computed on taxable income (gross_pay − NSSF − SHIF − HL)",
        pass: payeTaxable,
        detail: payeTaxable
          ? "OK: PAYE formula references NSSF (taxable income basis)"
          : "FAIL: PAYE formula does not reference NSSF — old gross_pay basis; run a payroll to trigger auto-rebuild",
      });

      checks.push({
        id: "deduction_order",
        label: "Deduction evaluation order: NSSF(1) → SHIF(2) → HL(3) → PAYE(4)",
        pass: deductionOrder,
        detail: deductionOrder
          ? `OK: PAYE idx=${payeIdx}, NSSF idx=${nssfIdx}`
          : `FAIL: PAYE idx=${payeIdx} < 4 — PAYE may evaluate before NSSF/SHIF; run a payroll to trigger auto-rebuild`,
      });

    } catch (e) {
      if (e instanceof ErpError && e.status === 404) {
        checks.push({
          id: "structure_exists",
          label: "Salary Structure exists",
          pass: false,
          detail: `"${structName}" not found in ERPNext — run a payroll to auto-create it`,
        });
      } else {
        if (e instanceof ErpError) return replyErp(reply, e);
        throw e;
      }
    }

    const allPass = checks.every((c) => c.pass);
    return {
      ok: allPass,
      structure_name: structName,
      company: ctx.company,
      checks,
      summary: allPass
        ? "All compliance checks passed"
        : `${checks.filter((c) => !c.pass).length} check(s) failed — see details`,
    };
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
