import pg from "pg";

export type ExpensePolicy = {
  max_total_claim?: number;
  receipt_threshold?: number;
  per_diem_per_day?: number;
  claim_type_limits?: Record<string, { max_amount?: number }>;
};

export type ExpenseWorkflow = {
  /** Amount above which only finance (`canSubmitOnBehalf`) may approve in Pay Hub. */
  approve_ceiling_for_non_finance?: number;
  /** Display-only steps for UI (employee / approver / finance lanes). */
  steps?: Array<{ key: string; label: string; lane?: "employee" | "approver" | "finance" }>;
};

export type ExpenseFeatureFlags = {
  wallet_pay?: boolean;
  offline_pay?: boolean;
  bulk_actions?: boolean;
};

export type CompanyRulesPack = {
  policy: ExpensePolicy;
  workflow: ExpenseWorkflow;
  feature_flags: ExpenseFeatureFlags;
};

export type PolicyFinding = {
  code: string;
  message: string;
  severity: "warning" | "block";
};

export type PolicyWarningPublic = {
  code: string;
  message: string;
  severity: "warning" | "block";
};

const TTL_MS = 60_000;
const cache = new Map<string, { pack: CompanyRulesPack; exp: number }>();

// Lazy singleton pg pool — connects to the same DATABASE_URL as the main app.
let pool: pg.Pool | null | undefined;

function getPool(): pg.Pool | null {
  if (pool !== undefined) return pool;
  const url = process.env.DATABASE_URL_RUNTIME?.trim() || process.env.DATABASE_URL?.trim();
  if (!url) {
    pool = null;
    return null;
  }
  const sslMode = (process.env.DATABASE_SSL || "auto").toLowerCase();
  const useSsl =
    sslMode === "require" ||
    (sslMode !== "disable" && /supabase\.co/i.test(url));
  pool = new pg.Pool({
    connectionString: url,
    ssl: useSsl ? { rejectUnauthorized: false } : undefined,
    max: 2,
    idleTimeoutMillis: 10_000,
  });
  pool.on("error", (err) => {
    console.warn("[expense-rules] pg pool error:", err.message);
  });
  return pool;
}

/** Whether the backing DB is available (used by the rules endpoint to set supabase_configured). */
export function isDbConfigured(): boolean {
  return !!getPool();
}

/** Default two-stage labels for UIs (`workflow.steps`) — line manager then HR / finance. */
function defaultWorkflowSteps(): NonNullable<ExpenseWorkflow["steps"]> {
  return [
    { key: "submit", label: "Submit", lane: "employee" },
    { key: "line_manager", label: "Line manager", lane: "approver" },
    { key: "hr_finance", label: "HR / Finance", lane: "finance" },
  ];
}

function defaultPack(): CompanyRulesPack {
  return {
    policy: {},
    workflow: { steps: defaultWorkflowSteps() },
    feature_flags: { wallet_pay: true, offline_pay: true, bulk_actions: true },
  };
}

function normalizeFlags(f: ExpenseFeatureFlags): ExpenseFeatureFlags {
  return {
    wallet_pay: f.wallet_pay !== false,
    offline_pay: f.offline_pay !== false,
    bulk_actions: f.bulk_actions !== false,
  };
}

export function invalidateRulesCache(companyKey: string): void {
  cache.delete(companyKey);
}

export async function loadCompanyRulesPack(companyKey: string): Promise<CompanyRulesPack> {
  const now = Date.now();
  const hit = cache.get(companyKey);
  if (hit && hit.exp > now) return hit.pack;

  const db = getPool();
  if (!db) {
    const empty = defaultPack();
    cache.set(companyKey, { pack: empty, exp: now + TTL_MS });
    return empty;
  }

  try {
    const res = await db.query<{
      policy: unknown;
      workflow: unknown;
      feature_flags: unknown;
    }>(
      "SELECT policy, workflow, feature_flags FROM hr_expense_company_rules WHERE company_key = $1",
      [companyKey],
    );
    const data = res.rows[0];
    const wf = (data?.workflow as ExpenseWorkflow) ?? {};
    const pack: CompanyRulesPack = {
      policy: (data?.policy as ExpensePolicy) ?? {},
      workflow: {
        ...wf,
        steps: wf.steps && wf.steps.length > 0 ? wf.steps : defaultWorkflowSteps(),
      },
      feature_flags: normalizeFlags((data?.feature_flags as ExpenseFeatureFlags) ?? {}),
    };
    cache.set(companyKey, { pack, exp: now + TTL_MS });
    return pack;
  } catch (err) {
    console.error("[expense-rules] DB load failed:", (err as Error).message);
    const empty = defaultPack();
    cache.set(companyKey, { pack: empty, exp: now + 5_000 });
    return empty;
  }
}

export function claimTotal(doc: Record<string, unknown>): number {
  const a = Number(doc.total_claimed_amount ?? doc.grand_total ?? 0);
  return Number.isFinite(a) ? a : 0;
}

export function evaluateExpenseClaim(doc: Record<string, unknown>, pack: CompanyRulesPack): PolicyFinding[] {
  const findings: PolicyFinding[] = [];
  const policy = pack.policy;
  const total = claimTotal(doc);

  if (typeof policy.max_total_claim === "number" && policy.max_total_claim >= 0 && total > policy.max_total_claim) {
    findings.push({
      code: "max_total_claim",
      message: `Claim total exceeds policy maximum (${policy.max_total_claim}).`,
      severity: "block",
    });
  }

  const lines = Array.isArray(doc.expenses) ? doc.expenses : [];
  for (const raw of lines) {
    if (!raw || typeof raw !== "object") continue;
    const line = raw as Record<string, unknown>;
    const et = String(line.expense_type ?? "").trim();
    const amt = Number(line.amount ?? 0);
    const lim = et ? policy.claim_type_limits?.[et] : undefined;
    if (lim && typeof lim.max_amount === "number" && amt > lim.max_amount) {
      findings.push({
        code: "claim_type_limit",
        message: `Line "${et}": amount exceeds per-type limit (${lim.max_amount}).`,
        severity: "block",
      });
    }
    if (typeof policy.per_diem_per_day === "number" && policy.per_diem_per_day > 0) {
      const travel = /travel|per diem|perdiem/i.test(et);
      if (travel && amt > policy.per_diem_per_day) {
        findings.push({
          code: "per_diem_hint",
          message: `Travel/per-diem line is above the typical daily cap (${policy.per_diem_per_day}); confirm days and policy.`,
          severity: "warning",
        });
      }
    }
  }

  if (typeof policy.receipt_threshold === "number" && total >= policy.receipt_threshold) {
    findings.push({
      code: "receipt_expected",
      message: `Claims at or above ${policy.receipt_threshold} should include receipts before submission.`,
      severity: "warning",
    });
  }

  return findings;
}

export function evaluateApproveWorkflow(
  doc: Record<string, unknown>,
  pack: CompanyRulesPack,
  ctx: { canSubmitOnBehalf: boolean }
): PolicyFinding | null {
  const ceiling = pack.workflow.approve_ceiling_for_non_finance;
  if (typeof ceiling !== "number" || ceiling < 0) return null;
  const total = claimTotal(doc);
  if (total > ceiling && !ctx.canSubmitOnBehalf) {
    return {
      code: "approve_ceiling",
      message: `Only finance may approve claims above ${ceiling} (Pay Hub policy).`,
      severity: "block",
    };
  }
  return null;
}

export function evaluateMarkPaid(
  mode: "wallet" | "offline",
  pack: CompanyRulesPack
): PolicyFinding | null {
  const f = pack.feature_flags;
  if (mode === "wallet" && f.wallet_pay === false) {
    return { code: "wallet_disabled", message: "Wallet payments are disabled by policy.", severity: "block" };
  }
  if (mode === "offline" && f.offline_pay === false) {
    return { code: "offline_disabled", message: "Offline payments are disabled by policy.", severity: "block" };
  }
  return null;
}

export function mergeClaimPolicyWarnings(
  doc: Record<string, unknown>,
  pack: CompanyRulesPack
): Record<string, unknown> {
  const findings = evaluateExpenseClaim(doc, pack);
  const policy_warnings: PolicyWarningPublic[] = findings.map((x) => ({
    code: x.code,
    message: x.message,
    severity: x.severity,
  }));
  return { ...doc, policy_warnings };
}

export function hasBlockingFinding(findings: PolicyFinding[]): PolicyFinding | undefined {
  return findings.find((f) => f.severity === "block");
}

export async function upsertCompanyRules(
  companyKey: string,
  body: { policy?: Record<string, unknown>; workflow?: Record<string, unknown>; feature_flags?: Record<string, unknown> }
): Promise<CompanyRulesPack> {
  const db = getPool();
  if (!db) {
    throw new Error("DATABASE_URL not configured on BFF — cannot save rules");
  }

  // Read current row
  const cur = await db.query<{ policy: unknown; workflow: unknown; feature_flags: unknown }>(
    "SELECT policy, workflow, feature_flags FROM hr_expense_company_rules WHERE company_key = $1",
    [companyKey],
  );
  const existing = cur.rows[0];

  const next = {
    policy: { ...((existing?.policy as object) ?? {}), ...(body.policy ?? {}) },
    workflow: { ...((existing?.workflow as object) ?? {}), ...(body.workflow ?? {}) },
    feature_flags: normalizeFlags({
      ...((existing?.feature_flags as ExpenseFeatureFlags) ?? {}),
      ...((body.feature_flags as ExpenseFeatureFlags) ?? {}),
    }),
  };

  await db.query(
    `INSERT INTO hr_expense_company_rules (company_key, policy, workflow, feature_flags, updated_at)
     VALUES ($1, $2, $3, $4, now())
     ON CONFLICT (company_key) DO UPDATE
       SET policy = $2, workflow = $3, feature_flags = $4, updated_at = now()`,
    [companyKey, JSON.stringify(next.policy), JSON.stringify(next.workflow), JSON.stringify(next.feature_flags)],
  );

  invalidateRulesCache(companyKey);
  return loadCompanyRulesPack(companyKey);
}

export async function fetchRulesRowForResponse(companyKey: string): Promise<{
  policy: ExpensePolicy;
  workflow: ExpenseWorkflow;
  feature_flags: ExpenseFeatureFlags;
  updated_at: string | null;
} | null> {
  const db = getPool();
  if (!db) return null;
  try {
    const res = await db.query<{
      policy: unknown;
      workflow: unknown;
      feature_flags: unknown;
      updated_at: Date | null;
    }>(
      "SELECT policy, workflow, feature_flags, updated_at FROM hr_expense_company_rules WHERE company_key = $1",
      [companyKey],
    );
    const data = res.rows[0];
    if (!data) return null;
    return {
      policy: (data.policy as ExpensePolicy) ?? {},
      workflow: (data.workflow as ExpenseWorkflow) ?? {},
      feature_flags: normalizeFlags((data.feature_flags as ExpenseFeatureFlags) ?? {}),
      updated_at: data.updated_at ? data.updated_at.toISOString() : null,
    };
  } catch {
    return null;
  }
}
