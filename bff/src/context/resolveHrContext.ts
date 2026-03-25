import type { FastifyRequest } from "fastify";
import * as config from "../config.js";
import type { ErpCredentials } from "../erpnext/client.js";
import type { HrContext } from "../types.js";
import { verifyBridgeAuth } from "../lib/bridge.js";

export class HttpError extends Error {
  constructor(
    message: string,
    public readonly status: number
  ) {
    super(message);
    this.name = "HttpError";
  }
}

function headerOne(req: FastifyRequest, name: string): string {
  const v = req.headers[name.toLowerCase()];
  if (Array.isArray(v)) return String(v[0] ?? "");
  return String(v ?? "");
}

/**
 * Resolves tenant (Company) and ERP credentials.
 *
 * 1. **Production:** `HR_BRIDGE_SECRET` + `X-Bridge-Auth` (HMAC). Optional `X-Erp-Api-Key` / `X-Erp-Api-Secret` from Pay Hub DB.
 * 2. **Legacy dev:** `DEV_INSECURE_HEADERS=1` + `X-Dev-*` headers + global ERP keys.
 */
export function resolveHrContext(req: FastifyRequest): HrContext {
  const bridgeSecret = (config.HR_BRIDGE_SECRET || "").trim();
  if (bridgeSecret.length > 0) {
    const authHeader = headerOne(req, "x-bridge-auth");
    if (!authHeader) throw new HttpError("Missing X-Bridge-Auth", 401);
    const verified = verifyBridgeAuth(authHeader, bridgeSecret);
    if (!verified) throw new HttpError("Invalid or expired bridge token", 401);

    const key = headerOne(req, "x-erp-api-key");
    const sec = headerOne(req, "x-erp-api-secret");
    let creds: ErpCredentials;
    if (key && sec) {
      creds = { apiKey: key, apiSecret: sec };
    } else if (config.ERP_API_KEY && config.ERP_API_SECRET) {
      creds = { apiKey: config.ERP_API_KEY, apiSecret: config.ERP_API_SECRET };
    } else {
      throw new HttpError("The HR service is missing connection settings. Your administrator needs to finish HR integration setup.", 500);
    }

    const appRoleRaw = verified.appRole != null ? String(verified.appRole).trim() : "";
    return {
      userEmail: verified.email,
      company: verified.company,
      creds,
      canSubmitOnBehalf: verified.canHr,
      ...(appRoleRaw ? { appRole: appRoleRaw } : {}),
    };
  }

  if (config.DEV_INSECURE_HEADERS) {
    const userEmail = String(req.headers["x-dev-user-email"] ?? "").trim();
    const company = String(req.headers["x-dev-company"] ?? "").trim();
    const canSubmitOnBehalf = req.headers["x-dev-hr"] === "1";
    const appRole = String(req.headers["x-dev-app-role"] ?? "").trim();
    if (!userEmail || !company) {
      throw new HttpError("Missing X-Dev-User-Email or X-Dev-Company", 401);
    }
    if (!config.ERP_API_KEY || !config.ERP_API_SECRET) {
      throw new HttpError("Local development mode requires HR integration keys on the HR service.", 500);
    }
    return {
      userEmail,
      company,
      creds: { apiKey: config.ERP_API_KEY, apiSecret: config.ERP_API_SECRET },
      canSubmitOnBehalf,
      ...(appRole ? { appRole } : {}),
    };
  }

  throw new HttpError("HR sign-in isn’t configured on this server. Your administrator needs to enable the HR bridge.", 501);
}
