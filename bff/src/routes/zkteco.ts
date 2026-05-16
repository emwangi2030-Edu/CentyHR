import type { FastifyPluginAsync } from "fastify";
import { ErpError } from "../erpnext/client.js";
import { resolveHrContext, HttpError } from "../context/resolveHrContext.js";
import * as config from "../config.js";
import {
  zkConnect,
  zkDisconnect,
  zkGetAttendances,
  zkGetInfo,
  zkGetUsers,
} from "../lib/zktecoClient.js";
import { runZktecoSync } from "../lib/zktecoSync.js";


export const zktecoRoutes: FastifyPluginAsync = async (app) => {
  // GET /v1/zkteco/status — ping device and return basic info
  app.get("/v1/zkteco/status", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    let zk: unknown = null;
    try {
      zk = await zkConnect(config.ZKTECO_HOST, config.ZKTECO_PORT, config.ZKTECO_TIMEOUT_MS);
      const info = await zkGetInfo(zk);
      return {
        data: {
          host: config.ZKTECO_HOST,
          port: config.ZKTECO_PORT,
          online: true,
          user_count: info.userCounts,
          log_count: info.logCounts,
          log_capacity: info.logCapacity,
        },
      };
    } catch (e) {
      return {
        data: {
          host: config.ZKTECO_HOST,
          port: config.ZKTECO_PORT,
          online: false,
          error: String(e),
        },
      };
    } finally {
      if (zk) await zkDisconnect(zk);
    }
  });

  // GET /v1/zkteco/users — list all enrolled users on the terminal
  app.get("/v1/zkteco/users", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    let zk: unknown = null;
    try {
      zk = await zkConnect(config.ZKTECO_HOST, config.ZKTECO_PORT, config.ZKTECO_TIMEOUT_MS);
      const users = await zkGetUsers(zk);
      return { data: users };
    } catch (e) {
      return reply.status(502).send({ error: `ZKTeco device unreachable: ${String(e)}` });
    } finally {
      if (zk) await zkDisconnect(zk);
    }
  });

  // GET /v1/zkteco/logs — return raw attendance logs stored on the terminal
  app.get("/v1/zkteco/logs", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    let zk: unknown = null;
    try {
      zk = await zkConnect(config.ZKTECO_HOST, config.ZKTECO_PORT, config.ZKTECO_TIMEOUT_MS);
      const logs = await zkGetAttendances(zk);
      return {
        data: [...logs]
          .sort((a, b) => b.recordTime.getTime() - a.recordTime.getTime())
          .map((l) => ({
            uid: l.userSn,
            enrollment_id: l.deviceUserId,
            timestamp: l.recordTime instanceof Date ? l.recordTime.toISOString() : String(l.recordTime),
          })),
      };
    } catch (e) {
      return reply.status(502).send({ error: `ZKTeco logs failed: ${String(e)}` });
    } finally {
      if (zk) await zkDisconnect(zk);
    }
  });

  // POST /v1/zkteco/sync — pull punch logs and write to ERPNext Employee Checkin
  // Body (optional): { from_date?: "YYYY-MM-DD" }
  app.post("/v1/zkteco/sync", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    const body = (req.body ?? {}) as Record<string, unknown>;
    const fromDateOverride = String(body.from_date ?? "").trim() || undefined;

    try {
      const stats = await runZktecoSync(ctx.creds, ctx.company, fromDateOverride);
      return { data: stats };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      return reply.status(502).send({ error: String(e) });
    }
  });
};
