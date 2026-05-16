import Fastify from "fastify";
import * as config from "./config.js";
import { healthRoutes } from "./routes/health.js";
import { employeeRoutes } from "./routes/employee.js";
import { employeePublicRoutes } from "./routes/employeePublic.js";
import { expenseRoutes } from "./routes/expenses.js";
import { assetsRoutes } from "./routes/assets.js";
import { leaveRoutes } from "./routes/leaves.js";
import { attendanceRoutes } from "./routes/attendance.js";
import { payrollRoutes } from "./routes/payroll.js";
import { hrApprovalMetaRoutes } from "./routes/hrApprovalMeta.js";
import { loanRoutes } from "./routes/loans.js";
import { capabilitiesRoutes } from "./routes/capabilities.js";
import { orgUnitRoutes } from "./routes/orgUnits.js";
import { companyRoutes } from "./routes/company.js";
import { lifecycleRoutes } from "./routes/lifecycle.js";
import { onboardingRoutes } from "./routes/onboarding.js";
import { documentsRoutes } from "./routes/documents.js";
import { recruitmentRoutes } from "./routes/recruitment.js";
import { zktecoRoutes } from "./routes/zkteco.js";
import { runZktecoSync } from "./lib/zktecoSync.js";

async function main() {
  const prefix = config.BASE_PATH;
  const app = Fastify({
    logger: { level: "warn" }, // only log warnings + errors; suppress per-request info noise (bump this to "info" to debug)
    trustProxy: true,
  });
  await app.register(healthRoutes, { prefix });
  await app.register(employeePublicRoutes, { prefix });
  await app.register(employeeRoutes, { prefix });
  await app.register(expenseRoutes, { prefix });
  await app.register(assetsRoutes, { prefix });
  await app.register(leaveRoutes, { prefix });
  await app.register(attendanceRoutes, { prefix });
  await app.register(payrollRoutes, { prefix });
  await app.register(hrApprovalMetaRoutes, { prefix });
  await app.register(loanRoutes, { prefix });
  await app.register(capabilitiesRoutes, { prefix });
  await app.register(orgUnitRoutes, { prefix });
  await app.register(companyRoutes, { prefix });
  await app.register(lifecycleRoutes, { prefix });
  await app.register(onboardingRoutes, { prefix });
  await app.register(documentsRoutes, { prefix });
  await app.register(recruitmentRoutes, { prefix });
  await app.register(zktecoRoutes, { prefix });
  await app.listen({ port: config.PORT, host: config.HR_BFF_HOST });

  // Background ZKTeco sync — runs every ZKTECO_SYNC_INTERVAL_MS when ZKTECO_COMPANY is set
  if (config.ZKTECO_COMPANY && config.ERP_API_KEY && config.ERP_API_SECRET) {
    const creds = { apiKey: config.ERP_API_KEY, apiSecret: config.ERP_API_SECRET };
    const company = config.ZKTECO_COMPANY;
    const intervalMs = config.ZKTECO_SYNC_INTERVAL_MS;
    const tag = "[zkteco-sync]";

    const tick = async () => {
      try {
        const stats = await runZktecoSync(creds, company);
        if (stats.synced > 0 || stats.errors > 0) {
          console.log(`${tag} synced=${stats.synced} skipped=${stats.skipped} errors=${stats.errors}`);
        }
      } catch (e) {
        const msg = String(e);
        const hint = msg.includes("fetch failed") || msg.includes("ECONNREFUSED") || msg.includes("TimeoutError")
          ? ` — cannot reach ERPNext at ${config.ERP_BASE_URL}`
          : msg.includes("unreachable")
            ? ` — cannot reach ZKTeco device at ${config.ZKTECO_HOST}:${config.ZKTECO_PORT}`
            : "";
        console.warn(`${tag} failed:`, msg + hint);
      }
    };

    // First run after 10s (give ERPNext a moment after startup), then on interval
    setTimeout(() => {
      void tick();
      setInterval(() => void tick(), intervalMs);
    }, 10_000);

    console.log(`${tag} scheduled every ${intervalMs / 60_000} min for company "${company}"`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
