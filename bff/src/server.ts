import Fastify from "fastify";
import * as config from "./config.js";
import { healthRoutes } from "./routes/health.js";
import { employeeRoutes } from "./routes/employee.js";
import { employeePublicRoutes } from "./routes/employeePublic.js";
import { expenseRoutes } from "./routes/expenses.js";

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
  await app.listen({ port: config.PORT, host: config.HR_BFF_HOST });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
