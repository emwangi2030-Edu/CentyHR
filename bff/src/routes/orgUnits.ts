/**
 * Org unit master-data sync: Department, Designation, Branch.
 * Creates the corresponding ERPNext record so Link validation passes
 * when employees are later created with these values.
 */
import type { FastifyPluginAsync } from "fastify";
import { defaultClient, ErpError } from "../erpnext/client.js";
import { resolveHrContext, HttpError } from "../context/resolveHrContext.js";

const erp = defaultClient();

const VALID_KINDS = ["department", "designation", "branch"] as const;
type OrgKind = (typeof VALID_KINDS)[number];

/** ERPNext doctype + field name for each org unit kind. */
const ERP_SPEC: Record<OrgKind, { doctype: string; field: string; companyScoped: boolean }> = {
  department:  { doctype: "Department",   field: "department_name", companyScoped: true  },
  designation: { doctype: "Designation",  field: "designation_name", companyScoped: false },
  branch:      { doctype: "Branch",       field: "branch",           companyScoped: false },
};

export const orgUnitRoutes: FastifyPluginAsync = async (app) => {
  /**
   * POST /v1/org/:kind  — ensure an org unit exists in ERPNext.
   * Body: { value: string }
   * Returns: { data: { doctype, name, created: boolean } }
   */
  app.post("/v1/org/:kind", async (req, reply) => {
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

    const kind = (req.params as { kind: string }).kind?.toLowerCase() as OrgKind;
    if (!VALID_KINDS.includes(kind)) {
      return reply.status(400).send({ error: `kind must be one of: ${VALID_KINDS.join(", ")}` });
    }

    const value = String((req.body as Record<string, unknown>)?.value ?? "").trim();
    if (!value) return reply.status(400).send({ error: "value is required" });

    const spec = ERP_SPEC[kind];

    // Check if it already exists in ERPNext — avoid duplicate errors
    try {
      const existing = await erp.getList(ctx.creds, spec.doctype, {
        filters: spec.companyScoped
          ? [[spec.field, "=", value], ["company", "=", ctx.company]]
          : [[spec.field, "=", value]],
        fields: ["name"],
        limit_page_length: 1,
      });
      if (Array.isArray(existing) && existing.length > 0) {
        return { data: { doctype: spec.doctype, name: (existing[0] as Record<string, unknown>).name, created: false } };
      }
    } catch {
      /* If check fails, attempt creation anyway */
    }

    // Create in ERPNext
    try {
      const doc: Record<string, unknown> = { [spec.field]: value };
      if (spec.companyScoped) doc.company = ctx.company;
      const created = await erp.createDoc(ctx.creds, spec.doctype, doc);
      const name = (created as Record<string, unknown>).name ?? value;
      return { data: { doctype: spec.doctype, name, created: true } };
    } catch (e) {
      if (e instanceof ErpError) {
        // 409 / duplicate entry — already exists, treat as success
        if (e.status === 409 || String(e.message).toLowerCase().includes("duplicate")) {
          return { data: { doctype: spec.doctype, name: value, created: false } };
        }
        return reply.status(e.status >= 500 ? 502 : e.status).send({ error: e.message });
      }
      throw e;
    }
  });
};
