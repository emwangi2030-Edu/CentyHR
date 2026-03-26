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
import { capabilitiesRoutes } from "./routes/capabilities.js";
import { orgUnitRoutes } from "./routes/orgUnits.js";

async function main() {
  const prefix = config.BASE_PATH;
  const app = Fastify({
    logger: true,
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
  await app.register(capabilitiesRoutes, { prefix });
  await app.register(orgUnitRoutes, { prefix });
  await app.listen({ port: config.PORT, host: config.HR_BFF_HOST });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
