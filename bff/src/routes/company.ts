import type { FastifyPluginAsync } from "fastify";
import { defaultClient, ErpError } from "../erpnext/client.js";
import { publicErpFailure } from "../erpnext/frappeResponse.js";
import { resolveHrContext, HttpError } from "../context/resolveHrContext.js";
const erp = defaultClient();

function abbrFromCompanyName(name: string): string {
  const cleaned = name.replace(/[^a-z0-9\s]/gi, " ").trim();
  const parts = cleaned.split(/\s+/).filter(Boolean);
  const letters = parts.map((p) => p[0]!.toUpperCase()).join("");
  const abbr = (letters || cleaned.slice(0, 3).toUpperCase()).slice(0, 5);
  return abbr || "CO";
}

export const companyRoutes: FastifyPluginAsync = async (app) => {
  /**
   * Ensure ERPNext Company exists for this tenant.
   *
   * Uses ctx.company from the bridge token (Pay Hub business name or HR_ERP_COMPANY_NAME override).
   * No-op if it already exists.
   */
  app.post("/v1/company/ensure", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR admin privileges required" });
    }

    const name = String(ctx.company || "").trim();
    if (!name) return reply.status(400).send({ error: "Missing company context" });

    // 1) Try direct getDoc by name (fast path).
    try {
      await erp.getDoc(ctx.creds, "Company", name);
      return { data: { name, created: false } };
    } catch (e) {
      // keep going on 404-ish failures
      if (!(e instanceof ErpError)) throw e;
    }

    // 2) Try lookup by company_name field (some tenants may not use name==company_name).
    try {
      const rows = await erp.getList(ctx.creds, "Company", {
        filters: [["company_name", "=", name]],
        fields: ["name", "company_name"],
        limit_page_length: 1,
      });
      if (Array.isArray(rows) && rows[0] && typeof rows[0] === "object") {
        const existingName = String((rows[0] as any).name ?? name).trim() || name;
        return { data: { name: existingName, created: false } };
      }
    } catch {
      /* proceed to create */
    }

    // 3) Create Company (best-effort minimal fields).
    const currency = String((process.env.ERP_DEFAULT_CURRENCY ?? "KES") || "KES").trim();
    const country = String((process.env.ERP_DEFAULT_COUNTRY ?? "Kenya") || "Kenya").trim();
    const abbrBase = abbrFromCompanyName(name);

    const attempts = [abbrBase, `${abbrBase}1`, `${abbrBase}2`, `${abbrBase}3`];
    let lastErr: unknown = null;
    for (const abbr of attempts) {
      try {
        const created = await erp.createDoc(ctx.creds, "Company", {
          company_name: name,
          abbr,
          default_currency: currency,
          country,
        });
        const createdName = String((created as any)?.name ?? name).trim() || name;
        return { data: { name: createdName, created: true } };
      } catch (e) {
        lastErr = e;
        // Try next abbr on duplicates / validation failures around abbr
        if (e instanceof ErpError && (e.status === 409 || String(e.body ?? "").toLowerCase().includes("abbr"))) {
          continue;
        }
        break;
      }
    }

    if (lastErr instanceof ErpError) {
      const status = lastErr.status >= 500 ? 502 : lastErr.status;
      return reply.status(status).send(publicErpFailure(lastErr));
    }
    throw lastErr;
  });
};

