/**
 * Centy HR BFF — deploy from this directory (same Git revision as Pay Hub).
 *
 *   cd CentyHR/bff && cp .env.example .env   # then fill secrets
 *   npm ci && npm run build
 *   pm2 startOrReload ecosystem.config.cjs --update-env
 *
 * Or from repo root: ./scripts/deploy-hr-bff.sh
 *
 * When `.env` exists in this directory, it is loaded so `PAY_HUB_INTERNAL_URL`, `HR_BRIDGE_SECRET`,
 * and ERP keys match production/staging without duplicating them in this file.
 */
const path = require("path");
const fs = require("fs");

const BFF_ROOT = __dirname;
const envFile = path.join(BFF_ROOT, ".env");
if (fs.existsSync(envFile)) {
  require("dotenv").config({ path: envFile });
}

const optionalFromProcess = [
  "ERP_BASE_URL",
  "ERP_SITE_NAME",
  "HR_BRIDGE_SECRET",
  "PAY_HUB_INTERNAL_URL",
  "ERP_API_KEY",
  "ERP_API_SECRET",
  "HR_ERP_API_KEY",
  "HR_ERP_API_SECRET",
  "SUPABASE_URL",
  "SUPABASE_SERVICE_ROLE_KEY",
  "DEV_INSECURE_HEADERS",
  "HR_CAPABILITIES_JSON",
  "GUARD_GEOFENCE_DEFAULT_METERS",
  "GUARD_EXCEPTION_REVIEW_DOCTYPE",
];

const env = {
  NODE_ENV: process.env.NODE_ENV || "production",
  PORT: process.env.PORT || "3040",
  BASE_PATH: process.env.BASE_PATH || "/hr-api",
  HR_BFF_HOST: process.env.HR_BFF_HOST || "127.0.0.1",
};

for (const k of optionalFromProcess) {
  const v = process.env[k];
  if (v != null && String(v).trim() !== "") {
    env[k] = v;
  }
}

module.exports = {
  apps: [
    {
      name: "centy-hr-bff",
      cwd: BFF_ROOT,
      script: "dist/server.js",
      instances: 1,
      exec_mode: "fork",
      autorestart: true,
      max_memory_restart: "250M",
      env,
    },
  ],
};
