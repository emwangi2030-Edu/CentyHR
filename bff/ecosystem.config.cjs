/**
 * Centy HR BFF — deploy from this directory (same Git revision as Pay Hub).
 *
 *   cd CentyHR/bff && cp .env.example .env   # then fill secrets
 *   npm ci && npm run build
 *   pm2 startOrReload ecosystem.config.cjs --update-env
 *
 * Or from repo root: ./scripts/deploy-hr-bff.sh
 */
const path = require("path");

const BFF_ROOT = __dirname;

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
      env: {
        NODE_ENV: "production",
        PORT: "3040",
        BASE_PATH: "/hr-api",
        HR_BFF_HOST: "127.0.0.1",
      },
    },
  ],
};
