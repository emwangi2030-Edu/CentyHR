/**
 * Create a test Employee (linked to an existing Frappe User) and a draft Expense Claim.
 *
 * Run from the BFF directory (uses ./.env for ERP_BASE_URL + API keys):
 *   SEED_USER_EMAIL=you@company.com SEED_COMPANY="Your ERP Company" npx tsx scripts/seed-test-hr-data.ts
 *
 * Optional:
 *   SEED_LIST_USERS=1     — print a few Frappe users (pick an email for SEED_USER_EMAIL)
 *   SEED_SKIP_EXPENSE=1   — only create/update Employee
 *   SEED_EXPENSE_TYPE     — defaults to first available Expense Claim Type, or "General"
 */
import { config } from "dotenv";
import { dirname, join } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
config({ path: join(__dirname, "..", ".env") });

const { ErpNextClient, ErpError } = await import("../src/erpnext/client.js");
const appConfig = await import("../src/config.js");

const creds = {
  apiKey: process.env.ERP_API_KEY ?? appConfig.ERP_API_KEY,
  apiSecret: process.env.ERP_API_SECRET ?? appConfig.ERP_API_SECRET,
};

if (!creds.apiKey || !creds.apiSecret) {
  console.error("Set ERP_API_KEY and ERP_API_SECRET in bff/.env (or env).");
  process.exit(1);
}

const erp = new ErpNextClient(appConfig.ERP_BASE_URL);

function today(): string {
  return new Date().toISOString().slice(0, 10);
}

/** Pick a leaf Account for expense claims (HRMS needs Expense Claim Type → Accounts for the company). */
async function pickExpenseAccount(company: string): Promise<string | null> {
  const rows = await erp.getList(creds, "Account", {
    filters: [
      ["company", "=", company],
      ["is_group", "=", 0],
    ],
    fields: ["name", "account_type"],
    limit_page_length: 80,
  });
  const list = rows as { name: string; account_type?: string }[];
  const badName = (n: string) => /cost of goods sold|cogs/i.test(n);
  const byType = list.find(
    (a) =>
      !badName(a.name) &&
      a.account_type &&
      /expense|charge|cost of sales|indirect/i.test(a.account_type)
  );
  if (byType) return byType.name;
  const byName = list.find((a) => !badName(a.name) && /expense|travel|fuel|misc|general/i.test(a.name));
  if (byName) return byName.name;
  const fallback = list.find((a) => !badName(a.name));
  return fallback?.name ?? list[0]?.name ?? null;
}

/** Ensure Expense Claim Type has an Accounts row for this company (HRMS validation). */
async function ensureClaimTypeAccount(expenseTypeName: string, company: string, glAccount: string): Promise<void> {
  const doc = await erp.getDoc(creds, "Expense Claim Type", expenseTypeName);
  const existing = Array.isArray(doc.accounts) ? [...doc.accounts] : [];
  const hasRow = existing.some((r) => String((r as Record<string, unknown>).company ?? "") === company);
  if (hasRow) return;
  const rows = [...existing, { company, default_account: glAccount }];
  await erp.updateDoc(creds, "Expense Claim Type", expenseTypeName, {
    accounts: rows as unknown as Record<string, unknown>[],
  });
  console.log(`Configured Expense Claim Type "${expenseTypeName}" with account ${glAccount} for ${company}.`);
}

async function main() {
  if (process.env.SEED_LIST_USERS === "1") {
    const res = await erp.listDocs(creds, "User", {
      fields: ["name", "email", "enabled"],
      filters: [["user_type", "=", "System User"]],
      limit_page_length: 25,
    });
    const rows = res.data ?? [];
    console.log("Frappe System Users (use SEED_USER_EMAIL = one of these emails):\n");
    for (const r of rows) {
      const o = r as Record<string, unknown>;
      console.log(`  ${String(o.email ?? o.name ?? "")}  (enabled=${o.enabled})`);
    }
    return;
  }

  const userEmail = (process.env.SEED_USER_EMAIL ?? "").trim();
  const company = (process.env.SEED_COMPANY ?? "").trim();
  if (!userEmail || !company) {
    console.error("Required: SEED_USER_EMAIL and SEED_COMPANY (must match Pay Hub / bridge company).");
    console.error("Tip: SEED_LIST_USERS=1 npx tsx scripts/seed-test-hr-data.ts");
    process.exit(1);
  }

  const firstName = (process.env.SEED_FIRST_NAME ?? "Centy").trim();
  const lastName = (process.env.SEED_LAST_NAME ?? "Test User").trim();
  const gender = (process.env.SEED_GENDER ?? "Male").trim();
  const dob = (process.env.SEED_DATE_OF_BIRTH ?? "1990-01-15").trim();
  const doj = (process.env.SEED_DATE_OF_JOINING ?? today()).trim();

  const genderRows = await erp.getList(creds, "Gender", {
    filters: [["name", "=", gender]],
    fields: ["name"],
    limit_page_length: 1,
  });
  if (!genderRows.length) {
    const any = await erp.getList(creds, "Gender", { fields: ["name"], limit_page_length: 5 });
    console.error(
      `Gender "${gender}" not found. Available Gender names:`,
      any.map((g) => (g as { name?: string }).name).join(", ")
    );
    process.exit(1);
  }

  let userName = userEmail;
  try {
    const users = await erp.getList(creds, "User", {
      filters: [["email", "=", userEmail]],
      fields: ["name", "email"],
      limit_page_length: 1,
    });
    if (!users.length) {
      console.error(
        `No Frappe User with email "${userEmail}". Create the user in Desk first, or pick another email (SEED_LIST_USERS=1).`
      );
      process.exit(1);
    }
    userName = String((users[0] as { name?: string }).name ?? userEmail);
  } catch (e) {
    if (e instanceof ErpError) {
      console.error("Frappe list User failed:", e.message, e.body);
    }
    throw e;
  }

  const existing = await erp.getList(creds, "Employee", {
    filters: [
      ["user_id", "=", userName],
      ["company", "=", company],
    ],
    fields: ["name", "employee_name", "company", "user_id"],
    limit_page_length: 1,
  });

  let employeeName: string;
  if (existing.length) {
    employeeName = String((existing[0] as { name: string }).name);
    console.log(`Employee already exists: ${employeeName} (${(existing[0] as { employee_name?: string }).employee_name ?? ""})`);
  } else {
    const created = await erp.createDoc(creds, "Employee", {
      doctype: "Employee",
      naming_series: "HR-EMP-",
      first_name: firstName,
      last_name: lastName,
      gender,
      date_of_birth: dob,
      date_of_joining: doj,
      company,
      status: "Active",
      user_id: userName,
    });
    employeeName = String(created.name ?? "");
    console.log(`Created Employee: ${employeeName} (user_id=${userName})`);
  }

  if (process.env.SEED_SKIP_EXPENSE === "1") {
    console.log("SEED_SKIP_EXPENSE=1 — skipping Expense Claim.");
    return;
  }

  let expenseType = (process.env.SEED_EXPENSE_TYPE ?? "").trim();
  if (!expenseType) {
    const types = await erp.getList(creds, "Expense Claim Type", {
      fields: ["name"],
      limit_page_length: 50,
    });
    const names = types.map((t) => String((t as { name: string }).name));
    const preferred = ["General", "Travel", "Fuel", "Calls"];
    expenseType =
      preferred.find((p) => names.includes(p)) ?? names[0] ?? "";
    if (!expenseType) {
      console.error("No Expense Claim Type in ERPNext. Create one in Desk (HR → Expense Claim Type).");
      process.exit(1);
    }
    console.log(`Using expense type: ${expenseType}`);
  }

  const glOverride = (process.env.SEED_EXPENSE_GL_ACCOUNT ?? "").trim();
  const glAccount = glOverride || (await pickExpenseAccount(company));
  if (!glAccount) {
    console.error(`No suitable GL Account found for Company "${company}". Set SEED_EXPENSE_GL_ACCOUNT to an Account name.`);
    process.exit(1);
  }
  if (!glOverride) console.log(`Using expense GL account: ${glAccount}`);

  await ensureClaimTypeAccount(expenseType, company, glAccount);

  const pd = today();
  const claim = await erp.createDoc(creds, "Expense Claim", {
    doctype: "Expense Claim",
    company,
    employee: employeeName,
    posting_date: pd,
    expenses: [
      {
        expense_type: expenseType,
        expense_date: pd,
        amount: 2500,
        description: "Sample test expense (seed script)",
      },
    ],
  });

  const claimName = String(claim.name ?? "");
  console.log(`Created draft Expense Claim: ${claimName} (submit from Pay Hub / Desk when ready)`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
