import type { FastifyPluginAsync, FastifyReply, FastifyRequest } from "fastify";
import { defaultClient } from "../erpnext/client.js";
import { ErpError } from "../erpnext/client.js";
import { publicErpFailure } from "../erpnext/frappeResponse.js";
import { resolveHrContext, HttpError } from "../context/resolveHrContext.js";
import type { HrContext, Asset } from "../types.js";

const erp = defaultClient();

function replyErp(reply: FastifyReply, e: ErpError) {
  const status = e.status >= 500 ? 502 : e.status;
  return reply.status(status).send(publicErpFailure(e));
}

function asRecord(v: unknown): Record<string, unknown> | null {
  return v && typeof v === "object" && !Array.isArray(v) ? (v as Record<string, unknown>) : null;
}

export const assetsRoutes: FastifyPluginAsync = async (app) => {
  // List company assets
  app.get("/assets", async (req: FastifyRequest, reply: FastifyReply) => {
    try {
      const ctx = await resolveHrContext(req);
      const assets = await erp.listDocs(ctx.creds, "Asset", {
        filters: [["company", "=", ctx.company]],
        fields: ["name", "asset_name", "item_code", "asset_category", "location", "custodian", "status", "image", "purchase_date"],
      });
      const data = assets.data?.map(asRecord).filter(Boolean) as unknown as Asset[];
      return { data };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
  });

  // Get asset by id
  app.get("/assets/:id", async (req: FastifyRequest<{ Params: { id: string } }>, reply: FastifyReply) => {
    try {
      const ctx = await resolveHrContext(req);
      const asset = await erp.getDoc(ctx.creds, "Asset", req.params.id);
      // Check company
      if (String(asset.company) !== ctx.company) {
        return reply.status(403).send({ error: "Asset not in your company" });
      }
      return { data: asset };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
  });

  // Create asset
  app.post("/assets", async (req: FastifyRequest<{ Body: Partial<Asset> }>, reply: FastifyReply) => {
    try {
      const ctx = await resolveHrContext(req);
      if (!ctx.canSubmitOnBehalf) {
        return reply.status(403).send({ error: "Only HR can create assets" });
      }
      const doc = { ...req.body, company: ctx.company };
      const result = await erp.createDoc(ctx.creds, "Asset", doc);
      return { data: result };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
  });

  // Update asset
  app.put("/assets/:id", async (req: FastifyRequest<{ Params: { id: string }; Body: Partial<Asset> }>, reply: FastifyReply) => {
    try {
      const ctx = await resolveHrContext(req);
      if (!ctx.canSubmitOnBehalf) {
        return reply.status(403).send({ error: "Only HR can update assets" });
      }
      const doc = req.body;
      const result = await erp.updateDoc(ctx.creds, "Asset", req.params.id, doc);
      return { data: result };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
  });

  // List assets for an employee
  app.get("/employees/:employeeId/assets", async (req: FastifyRequest<{ Params: { employeeId: string } }>, reply: FastifyReply) => {
    try {
      const ctx = await resolveHrContext(req);
      // Check if employee is in company
      const emp = await erp.getDoc(ctx.creds, "Employee", req.params.employeeId);
      if (String(emp.company) !== ctx.company) {
        return reply.status(403).send({ error: "Employee not in your company" });
      }
      // If not HR, only own assets
      if (!ctx.canSubmitOnBehalf) {
        const myEmp = await erp.listDocs(ctx.creds, "Employee", {
          filters: [["user_id", "=", ctx.userEmail], ["company", "=", ctx.company]],
          fields: ["name"],
          limit_page_length: 1,
        });
        const myName = asRecord(myEmp.data?.[0])?.name;
        if (String(myName) !== req.params.employeeId) {
          return reply.status(403).send({ error: "You can only view your own assets" });
        }
      }
      const assets = await erp.listDocs(ctx.creds, "Asset", {
        filters: [["custodian", "=", req.params.employeeId], ["company", "=", ctx.company]],
        fields: ["name", "asset_name", "item_code", "asset_category", "location", "custodian", "status", "image", "purchase_date"],
      });
      const data = assets.data?.map(asRecord).filter(Boolean) as unknown as Asset[];
      return { data };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
  });
};