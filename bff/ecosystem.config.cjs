/** PM2: `pm2 start ecosystem.config.cjs` from `bff/` */
module.exports = {
  apps: [
    {
      name: "centy-hr-bff",
      cwd: __dirname,
      script: "dist/server.js",
      instances: 1,
      exec_mode: "fork",
      autorestart: true,
      max_memory_restart: "200M",
      env: {
        NODE_ENV: "production",
        PORT: "3040",
        BASE_PATH: "/hr-api",
        HR_BFF_HOST: "127.0.0.1",
        ERP_BASE_URL: "https://erp.tarakilishicloud.com",
        // Set HR_BRIDGE_SECRET in production (same value as Pay Hub). Omit for local dev + DEV_INSECURE_HEADERS.
      },
    },
  ],
};
