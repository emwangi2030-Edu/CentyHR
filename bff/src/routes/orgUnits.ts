/**
 * Org unit master-data: Department, Designation, Branch.
 * - POST   /v1/org/:kind              — ensure a unit exists (create if missing)
 * - GET    /v1/org/options            — list all departments, designations, branches
 * - GET    /v1/org/integrity          — check for reporting chain issues
 * - DELETE /v1/org/options/:kind/:val — delete a unit (errors if employees linked)
 * - PATCH  /v1/org/options/:kind/:val — rename a unit
 */
import type { FastifyPluginAsync } from "fastify";
import { defaultClient, ErpError } from "../erpnext/client.js";
import { resolveHrContext, HttpError } from "../context/resolveHrContext.js";

const erp = defaultClient();

const VALID_KINDS = ["department", "designation", "branch"] as const;
type OrgKind = (typeof VALID_KINDS)[number];

/**
 * ERPNext doctype spec for each org unit kind.
 *
 * - listField:   field returned in GET /org/options — must be the document `name`
 *                because Employee.department is a Link that stores the doc name.
 * - writeField:  field set when creating a new doc (the human-readable title field).
 *                ERPNext auto-generates `name` from this via its naming series
 *                (e.g. department_name "Engineering" → name "Engineering - NT").
 * - filterField: field used to check whether a doc already exists before creating.
 */
const ERP_SPEC: Record<OrgKind, {
  doctype: string;
  listField: string;
  writeField: string;
  filterField: string;
  companyScoped: boolean;
}> = {
  department:  { doctype: "Department",  listField: "name",             writeField: "department_name",  filterField: "department_name",  companyScoped: true  },
  designation: { doctype: "Designation", listField: "designation_name", writeField: "designation_name", filterField: "designation_name", companyScoped: false },
  branch:      { doctype: "Branch",      listField: "branch",           writeField: "branch",           filterField: "branch",           companyScoped: false },
};

function requireHr(ctx: ReturnType<typeof resolveHrContext>, reply: Parameters<Parameters<FastifyPluginAsync>[0]["post"]>[1]): boolean {
  if (!ctx.canSubmitOnBehalf) {
    reply.status(403).send({ error: "HR admin privileges required" });
    return false;
  }
  return true;
}

export const orgUnitRoutes: FastifyPluginAsync = async (app) => {

  // ── GET /v1/org/options ──────────────────────────────────────────────────
  // Returns all departments, designations, and branches for the company.
  app.get("/v1/org/options", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }

    const fetchKind = async (kind: OrgKind): Promise<string[]> => {
      const spec = ERP_SPEC[kind];
      try {
        const rows = await erp.getList(ctx!.creds, spec.doctype, {
          filters: spec.companyScoped ? [["company", "=", ctx!.company]] : [],
          fields: ["name", spec.listField],
          order_by: `${spec.listField} asc`,
          limit_page_length: 500,
        });
        return (rows as Record<string, unknown>[])
          .map((r) => String(r[spec.listField] ?? r.name ?? "").trim())
          .filter(Boolean);
      } catch {
        return [];
      }
    };

    const [departments, designations, branches] = await Promise.all([
      fetchKind("department"),
      fetchKind("designation"),
      fetchKind("branch"),
    ]);

    return { data: { department: departments, designation: designations, branch: branches } };
  });

  // ── GET /v1/org/integrity ────────────────────────────────────────────────
  // Checks for broken reporting chains in the employee directory.
  app.get("/v1/org/integrity", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    try {
      const rows = await erp.getList(ctx.creds, "Employee", {
        filters: [["company", "=", ctx.company], ["status", "=", "Active"]],
        fields: ["name", "employee_name", "reports_to", "department", "user_id"],
        limit_page_length: 5000,
      });

      const employees = rows as { name: string; employee_name?: string; reports_to?: string; department?: string }[];
      const nameSet = new Set(employees.map((e) => e.name));
      const totalEmployees = employees.length;

      let missingManagerLinks = 0;
      let selfReporting = 0;
      const issues: Array<{ employeeId: string; employeeName: string; issue: string }> = [];

      for (const emp of employees) {
        const rt = String(emp.reports_to ?? "").trim();
        if (rt) {
          if (rt === emp.name) {
            selfReporting++;
            issues.push({ employeeId: emp.name, employeeName: String(emp.employee_name ?? emp.name), issue: "Reports to themselves" });
          } else if (!nameSet.has(rt)) {
            missingManagerLinks++;
            issues.push({ employeeId: emp.name, employeeName: String(emp.employee_name ?? emp.name), issue: `Manager "${rt}" not found in company roster` });
          }
        }
      }

      // Cycle detection via path traversal
      const reportsToMap = new Map<string, string>();
      for (const emp of employees) {
        if (emp.reports_to && nameSet.has(emp.reports_to) && emp.reports_to !== emp.name) {
          reportsToMap.set(emp.name, emp.reports_to);
        }
      }
      let cycles = 0;
      const cycleChecked = new Set<string>();
      for (const startId of reportsToMap.keys()) {
        if (cycleChecked.has(startId)) continue;
        const visited = new Set<string>();
        let cur: string | undefined = startId;
        while (cur && !visited.has(cur) && !cycleChecked.has(cur)) {
          visited.add(cur);
          cur = reportsToMap.get(cur);
        }
        if (cur && visited.has(cur)) {
          cycles++;
          for (const id of visited) cycleChecked.add(id);
        } else {
          for (const id of visited) cycleChecked.add(id);
        }
      }

      return {
        data: {
          totalEmployees,
          missingManagerLinks,
          selfReporting,
          cycles,
          issues: issues.slice(0, 50), // Return first 50 for display
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });

  // ── POST /v1/org/:kind ────────────────────────────────────────────────────
  // Legacy create-or-ensure endpoint (kept for backwards compat + bulk upload).
  app.post("/v1/org/:kind", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }
    if (!requireHr(ctx, reply)) return;

    const kind = (req.params as { kind: string }).kind?.toLowerCase() as OrgKind;
    if (!VALID_KINDS.includes(kind)) return reply.status(400).send({ error: `kind must be one of: ${VALID_KINDS.join(", ")}` });

    const value = String((req.body as Record<string, unknown>)?.value ?? "").trim();
    if (!value) return reply.status(400).send({ error: "value is required" });

    return ensureOrgUnit(ctx, kind, value, reply);
  });

  // ── POST /v1/org/options/:kind ────────────────────────────────────────────
  // Create a new org unit (called from org settings UI).
  app.post("/v1/org/options/:kind", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }
    if (!requireHr(ctx, reply)) return;

    const kind = (req.params as { kind: string }).kind?.toLowerCase() as OrgKind;
    if (!VALID_KINDS.includes(kind)) return reply.status(400).send({ error: `kind must be one of: ${VALID_KINDS.join(", ")}` });

    const value = String((req.body as Record<string, unknown>)?.value ?? "").trim();
    if (!value) return reply.status(400).send({ error: "value is required" });

    return ensureOrgUnit(ctx, kind, value, reply);
  });

  // ── DELETE /v1/org/options/:kind/:value ───────────────────────────────────
  // Delete an org unit. Refuses if employees are still using it.
  app.delete("/v1/org/options/:kind/:value", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }
    if (!requireHr(ctx, reply)) return;

    const kind = (req.params as { kind: string; value: string }).kind?.toLowerCase() as OrgKind;
    const rawValue = decodeURIComponent((req.params as { kind: string; value: string }).value ?? "");
    if (!VALID_KINDS.includes(kind)) return reply.status(400).send({ error: `Invalid kind` });
    if (!rawValue) return reply.status(400).send({ error: "value is required" });

    const spec = ERP_SPEC[kind];

    // Find the ERPNext doc name for this value
    let docName: string;
    try {
      const rows = await erp.getList(ctx.creds, spec.doctype, {
        filters: spec.companyScoped
          ? [[spec.filterField, "=", rawValue], ["company", "=", ctx.company]]
          : [[spec.filterField, "=", rawValue]],
        fields: ["name"],
        limit_page_length: 1,
      });
      const row = (rows as { name: string }[])[0];
      if (!row) return reply.status(404).send({ error: `${kind} "${rawValue}" not found` });
      docName = row.name;
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }

    // Check that no active employees reference this value
    try {
      const linked = await erp.getList(ctx.creds, "Employee", {
        filters: [["company", "=", ctx.company], [kind, "=", docName], ["status", "=", "Active"]],
        fields: ["name"],
        limit_page_length: 1,
      });
      if ((linked as unknown[]).length > 0) {
        return reply.status(409).send({
          error: `Cannot delete — active employees are still assigned to this ${kind}. Reassign them first.`,
        });
      }
    } catch { /* continue — check failed, attempt delete anyway */ }

    try {
      await erp.deleteDoc(ctx.creds, spec.doctype, docName);
      return { data: { deleted: true, kind, value: rawValue } };
    } catch (e) {
      if (e instanceof ErpError) {
        if (e.status === 417 || String(e.message).toLowerCase().includes("link")) {
          return reply.status(409).send({
            error: `This ${kind} is still referenced by other records and cannot be deleted. Reassign employees first.`,
          });
        }
        return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      }
      throw e;
    }
  });

  // ── PATCH /v1/org/options/:kind/:value ────────────────────────────────────
  // Rename an org unit. Body: { newValue: string }
  app.patch("/v1/org/options/:kind/:value", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }
    if (!requireHr(ctx, reply)) return;

    const kind = (req.params as { kind: string; value: string }).kind?.toLowerCase() as OrgKind;
    const oldValue = decodeURIComponent((req.params as { kind: string; value: string }).value ?? "");
    const newValue = String((req.body as Record<string, unknown>)?.newValue ?? "").trim();

    if (!VALID_KINDS.includes(kind)) return reply.status(400).send({ error: "Invalid kind" });
    if (!oldValue || !newValue) return reply.status(400).send({ error: "oldValue and newValue are required" });
    if (oldValue === newValue) return { data: { kind, oldValue, newValue, changed: false } };

    const spec = ERP_SPEC[kind];

    // Find the existing ERPNext doc
    let docName: string;
    try {
      const rows = await erp.getList(ctx.creds, spec.doctype, {
        filters: spec.companyScoped
          ? [[spec.filterField, "=", oldValue], ["company", "=", ctx.company]]
          : [[spec.filterField, "=", oldValue]],
        fields: ["name"],
        limit_page_length: 1,
      });
      const row = (rows as { name: string }[])[0];
      if (!row) return reply.status(404).send({ error: `${kind} "${oldValue}" not found` });
      docName = row.name;
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }

    try {
      await erp.updateDoc(ctx.creds, spec.doctype, docName, { [spec.writeField]: newValue });
      return { data: { kind, oldValue, newValue, changed: true } };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });
};

// ── Shared helper ────────────────────────────────────────────────────────────
async function ensureOrgUnit(
  ctx: ReturnType<typeof resolveHrContext>,
  kind: OrgKind,
  value: string,
  reply: Parameters<Parameters<FastifyPluginAsync>[0]["post"]>[1]
) {
  const spec = ERP_SPEC[kind];
  try {
    const existing = await erp.getList(ctx.creds, spec.doctype, {
      filters: spec.companyScoped
        ? [[spec.filterField, "=", value], ["company", "=", ctx.company]]
        : [[spec.filterField, "=", value]],
      fields: ["name"],
      limit_page_length: 1,
    });
    if (Array.isArray(existing) && existing.length > 0) {
      return { data: { doctype: spec.doctype, name: (existing[0] as Record<string, unknown>).name, created: false } };
    }
  } catch { /* attempt creation anyway */ }

  try {
    const doc: Record<string, unknown> = { [spec.writeField]: value };
    if (spec.companyScoped) doc.company = ctx.company;
    const created = await erp.createDoc(ctx.creds, spec.doctype, doc);
    const name = (created as Record<string, unknown>).name ?? value;
    return { data: { doctype: spec.doctype, name, created: true } };
  } catch (e) {
    if (e instanceof ErpError) {
      if (e.status === 409 || String(e.message).toLowerCase().includes("duplicate")) {
        return { data: { doctype: spec.doctype, name: value, created: false } };
      }
      return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
    }
    throw e;
  }
}
