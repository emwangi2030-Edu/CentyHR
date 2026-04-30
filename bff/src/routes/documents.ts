/**
 * Employee document management — upload, list, delete.
 *
 * Uses ERPNext's standard File doctype to attach documents to Employee records.
 * Multipart uploads are proxied to ERPNext's `/api/method/upload_file` endpoint.
 *
 * Routes:
 *   GET    /v1/employees/:id/documents              — list attached documents
 *   POST   /v1/employees/:id/documents              — upload a document (multipart/form-data)
 *   DELETE /v1/employees/:id/documents/:fileId      — delete a document
 */
import type { FastifyPluginAsync } from "fastify";
import multipart from "@fastify/multipart";
import { defaultClient, ErpError } from "../erpnext/client.js";
import { resolveHrContext, HttpError } from "../context/resolveHrContext.js";

const erp = defaultClient();

// Max file size allowed through the BFF proxy (10 MB)
const MAX_FILE_BYTES = 10 * 1024 * 1024;

// Allowed MIME types for employee documents
const ALLOWED_MIME_PREFIXES = ["image/", "application/pdf", "application/msword",
  "application/vnd.openxmlformats", "application/vnd.ms-excel", "text/plain"];

function isMimeAllowed(mime: string): boolean {
  return ALLOWED_MIME_PREFIXES.some((p) => mime.startsWith(p));
}

function friendlySize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

// Common HR document types (ERPNext custom doctype not required)
const DOCUMENT_TYPES = [
  "Employment Contract", "Offer Letter", "NDA / Confidentiality",
  "ID Copy", "KRA PIN Certificate", "NHIF Card", "NSSF Certificate",
  "Academic Certificate", "Professional Certificate", "Bank Letter",
  "Payslip", "Warning Letter", "Termination Letter", "Other",
];

export const documentsRoutes: FastifyPluginAsync = async (app) => {
  await app.register(multipart, { limits: { fileSize: MAX_FILE_BYTES, files: 1 } });

  // ── GET /v1/documents/meta/document-types ────────────────────────────────
  app.get("/v1/documents/meta/document-types", async (req, reply) => {
    try { resolveHrContext(req as any); } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e;
    }
    return { data: DOCUMENT_TYPES.map((name) => ({ name })) };
  });

  // ── GET /v1/documents/meta/templates ────────────────────────────────────
  app.get("/v1/documents/meta/templates", async (req, reply) => {
    try { resolveHrContext(req as any); } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e;
    }
    return { data: [] };
  });

  // ── GET /v1/documents/employee ───────────────────────────────────────────
  app.get("/v1/documents/employee", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req as any); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }

    const q = req.query as { page?: string; page_size?: string; employee?: string };
    const page = Math.max(1, Number(q.page || 1));
    const pageSize = Math.min(50, Math.max(1, Number(q.page_size || 20)));
    const employeeFilter = (q.employee || "").trim();

    try {
      // Always scope to employees in ctx.company
      let empScope = employeeFilter;
      if (!ctx.canSubmitOnBehalf) {
        // Non-HR: only their own employee record in this company
        const mine = await erp.getList(ctx.creds, "Employee", {
          filters: [["user_id", "=", ctx.userEmail], ["company", "=", ctx.company]],
          fields: ["name"],
          limit_page_length: 1,
        }) as Array<{ name: string }>;
        if (!mine.length) return { data: [], page, page_size: pageSize, has_more: false };
        empScope = mine[0].name;
      } else if (empScope) {
        // HR filtered to a specific employee — verify they belong to this company
        const check = await erp.getList(ctx.creds, "Employee", {
          filters: [["name", "=", empScope], ["company", "=", ctx.company]],
          fields: ["name"],
          limit_page_length: 1,
        }) as Array<{ name: string }>;
        if (!check.length) return { data: [], page, page_size: pageSize, has_more: false };
      } else {
        // HR listing all — get all employee names in the company first
        const allEmps = await erp.getList(ctx.creds, "Employee", {
          filters: [["company", "=", ctx.company]],
          fields: ["name"],
          limit_page_length: 500,
        }) as Array<{ name: string }>;
        if (!allEmps.length) return { data: [], page, page_size: pageSize, has_more: false };
        // Use "in" filter with comma-separated names
        const namesCsv = allEmps.map((e) => e.name).join(",");
        const filters: Array<[string, string, string]> = [
          ["attached_to_doctype", "=", "Employee"],
          ["attached_to_name", "in", namesCsv],
        ];
        const rows = await erp.getList(ctx.creds, "File", {
          filters,
          fields: ["name", "file_name", "file_url", "file_size", "attached_to_name", "creation", "modified"],
          order_by: "modified desc",
          limit_page_length: pageSize + 1,
          limit_start: (page - 1) * pageSize,
        }) as Array<Record<string, unknown>>;
        const hasMore = rows.length > pageSize;
        return {
          data: rows.slice(0, pageSize).map((r) => ({
            name: String(r.name ?? ""),
            employee: String(r.attached_to_name ?? ""),
            company: ctx.company,
            document_type: String(r.file_name ?? ""),
            signing_status: "not_required",
            modified: String(r.modified ?? ""),
          })),
          page, page_size: pageSize, has_more: hasMore,
        };
      }

      const filters: Array<[string, string, string]> = [["attached_to_doctype", "=", "Employee"]];
      if (empScope) filters.push(["attached_to_name", "=", empScope]);

      const rows = await erp.getList(ctx.creds, "File", {
        filters,
        fields: ["name", "file_name", "file_url", "file_size", "attached_to_name", "creation", "modified"],
        order_by: "modified desc",
        limit_page_length: pageSize + 1,
        limit_start: (page - 1) * pageSize,
      }) as Array<Record<string, unknown>>;

      const hasMore = rows.length > pageSize;
      return {
        data: rows.slice(0, pageSize).map((r) => ({
          name: String(r.name ?? ""),
          employee: String(r.attached_to_name ?? ""),
          company: ctx.company,
          document_type: String(r.file_name ?? ""),
          signing_status: "not_required",
          modified: String(r.modified ?? ""),
        })),
        page, page_size: pageSize, has_more: hasMore,
      };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });

  // ── GET /v1/documents/employee/:id ──────────────────────────────────────
  app.get("/v1/documents/employee/:id", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req as any); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }

    const fileId = (req.params as { id: string }).id;
    try {
      const file = await erp.getDoc(ctx.creds, "File", fileId) as Record<string, unknown>;
      // Verify the attached employee belongs to this company (security check)
      if (String(file.attached_to_doctype) === "Employee") {
        const emp = await erp.getList(ctx.creds, "Employee", {
          filters: [["name", "=", String(file.attached_to_name)], ["company", "=", ctx.company]],
          fields: ["name"],
          limit_page_length: 1,
        }) as Array<{ name: string }>;
        if (!emp.length && ctx.canSubmitOnBehalf === false) {
          return reply.status(403).send({ error: "Access denied" });
        }
      }
      return {
        data: {
          name: String(file.name ?? fileId),
          employee: String(file.attached_to_name ?? ""),
          document_type: String(file.file_name ?? ""),
          signing_status: "not_required",
          signed_document_file: "",
          modified: String(file.modified ?? ""),
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });

  // ── GET /v1/documents/employee/:id/files ────────────────────────────────
  app.get("/v1/documents/employee/:id/files", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req as any); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }

    const fileId = (req.params as { id: string }).id;
    try {
      const file = await erp.getDoc(ctx.creds, "File", fileId) as Record<string, unknown>;
      return { data: [{ name: String(file.name ?? fileId), file_name: String(file.file_name ?? fileId), file_url: String(file.file_url ?? "") }] };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });

  // ── GET /v1/documents/company/:id ───────────────────────────────────────
  app.get("/v1/documents/company/:id", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req as any); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    const fileId = (req.params as { id: string }).id;
    try {
      const file = await erp.getDoc(ctx.creds, "File", fileId) as Record<string, unknown>;
      return {
        data: {
          name: String(file.name ?? fileId),
          company: ctx.company,
          document_type: String(file.file_name ?? ""),
          signing_status: "not_required",
          signed_document_file: "",
          modified: String(file.modified ?? ""),
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });

  // ── GET /v1/documents/company/:id/files ─────────────────────────────────
  app.get("/v1/documents/company/:id/files", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req as any); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    const fileId = (req.params as { id: string }).id;
    try {
      const file = await erp.getDoc(ctx.creds, "File", fileId) as Record<string, unknown>;
      return { data: [{ name: String(file.name ?? fileId), file_name: String(file.file_name ?? fileId), file_url: String(file.file_url ?? "") }] };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });

  // ── GET /v1/documents/files/:id/content ─────────────────────────────────
  // Proxies a private ERPNext file back to the browser with auth.
  app.get("/v1/documents/files/:id/content", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req as any); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }

    const fileId = (req.params as { id: string }).id;
    try {
      const file = await erp.getDoc(ctx.creds, "File", fileId) as Record<string, unknown>;
      const fileUrl = String(file.file_url ?? "");
      const fileName = String(file.file_name ?? fileId);

      if (!fileUrl) return reply.status(404).send({ error: "File has no download URL" });

      // Build the full URL — ERPNext stores relative paths like /private/files/...
      const { ERP_BASE_URL } = await import("../config.js");
      const fullUrl = fileUrl.startsWith("http") ? fileUrl : `${ERP_BASE_URL}${fileUrl}`;

      const upstream = await fetch(fullUrl, {
        headers: {
          Authorization: `token ${ctx.creds.apiKey}:${ctx.creds.apiSecret}`,
        },
      });

      if (!upstream.ok) return reply.status(upstream.status).send({ error: `ERPNext returned ${upstream.status}` });

      const contentType = upstream.headers.get("content-type") ?? "application/octet-stream";
      const buffer = Buffer.from(await upstream.arrayBuffer());

      // Only set the headers we need — do NOT forward Content-Location or other ERPNext headers
      // that could cause the browser to navigate away from the proxy URL.
      reply
        .removeHeader("Content-Location")
        .header("Content-Type", contentType)
        .header("Content-Disposition", `inline; filename="${fileName}"`)
        .header("Content-Length", String(buffer.length))
        .send(buffer);
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });

  // ── GET /v1/documents/company ────────────────────────────────────────────
  app.get("/v1/documents/company", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req as any); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    const q = req.query as { page?: string; page_size?: string };
    const page = Math.max(1, Number(q.page || 1));
    const pageSize = Math.min(50, Math.max(1, Number(q.page_size || 20)));

    try {
      const rows = await erp.getList(ctx.creds, "File", {
        filters: [
          ["attached_to_doctype", "=", "Company"],
          ["attached_to_name", "=", ctx.company],
        ],
        fields: ["name", "file_name", "file_url", "file_size", "creation", "modified"],
        order_by: "modified desc",
        limit_page_length: pageSize + 1,
        limit_start: (page - 1) * pageSize,
      }) as Array<Record<string, unknown>>;

      const hasMore = rows.length > pageSize;
      return {
        data: rows.slice(0, pageSize).map((r) => ({
          name: String(r.name ?? ""),
          company: ctx.company,
          document_type: String(r.file_name ?? ""),
          signing_status: "not_required",
          modified: String(r.modified ?? ""),
        })),
        page, page_size: pageSize, has_more: hasMore,
      };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });

  // ── POST /v1/documents/employee ──────────────────────────────────────────
  // Creates a document record. Returns the employee ID as `name` for the
  // follow-up attachment upload step.
  app.post("/v1/documents/employee", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req as any); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    const body = req.body as { document_type?: string; employee?: string };
    let targetEmployee = (body.employee || "").trim();

    if (!targetEmployee) {
      // Resolve the current user's employee record
      try {
        const mine = await erp.getList(ctx.creds, "Employee", {
          filters: [["user_id", "=", ctx.userEmail], ["company", "=", ctx.company]],
          fields: ["name"],
          limit_page_length: 1,
        }) as Array<{ name: string }>;
        targetEmployee = mine[0]?.name ?? "";
      } catch { /* leave empty */ }
    }

    if (!targetEmployee) return reply.status(400).send({ error: "Employee not found for this user. Please specify an employee." });
    return { data: { name: targetEmployee, document_type: body.document_type ?? "" } };
  });

  // ── POST /v1/documents/employee/:id/attachments ──────────────────────────
  app.post("/v1/documents/employee/:id/attachments", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req as any); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    const employeeId = (req.params as { id: string }).id;
    let fileBuffer: Buffer, fileName: string, contentType: string;
    try {
      const data = await req.file();
      if (!data) return reply.status(400).send({ error: "No file provided" });
      fileName = data.filename || "document";
      contentType = data.mimetype || "application/octet-stream";
      if (!isMimeAllowed(contentType)) return reply.status(415).send({ error: `File type "${contentType}" is not allowed.` });
      const chunks: Buffer[] = [];
      let total = 0;
      for await (const chunk of data.file) {
        total += (chunk as Buffer).length;
        if (total > MAX_FILE_BYTES) return reply.status(413).send({ error: `File too large. Max ${friendlySize(MAX_FILE_BYTES)}.` });
        chunks.push(chunk as Buffer);
      }
      fileBuffer = Buffer.concat(chunks);
    } catch { return reply.status(400).send({ error: "Could not read the uploaded file." }); }

    try {
      const uploaded = await erp.uploadFile(ctx.creds, { buffer: fileBuffer, filename: fileName, contentType, isPrivate: true, doctype: "Employee", docname: employeeId });
      const fileDoc = ((uploaded as Record<string, unknown>).message as Record<string, unknown>) ?? (uploaded as Record<string, unknown>);
      return { data: { name: String(fileDoc.name ?? ""), file_name: String(fileDoc.file_name ?? fileName) } };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });

  // ── POST /v1/documents/company ───────────────────────────────────────────
  app.post("/v1/documents/company", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req as any); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    const body = req.body as { document_type?: string };
    return { data: { name: ctx.company, document_type: body.document_type ?? "" } };
  });

  // ── POST /v1/documents/company/:id/attachments ───────────────────────────
  app.post("/v1/documents/company/:id/attachments", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req as any); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    const companyId = (req.params as { id: string }).id;
    let fileBuffer: Buffer, fileName: string, contentType: string;
    try {
      const data = await req.file();
      if (!data) return reply.status(400).send({ error: "No file provided" });
      fileName = data.filename || "document";
      contentType = data.mimetype || "application/octet-stream";
      if (!isMimeAllowed(contentType)) return reply.status(415).send({ error: `File type "${contentType}" is not allowed.` });
      const chunks: Buffer[] = [];
      let total = 0;
      for await (const chunk of data.file) {
        total += (chunk as Buffer).length;
        if (total > MAX_FILE_BYTES) return reply.status(413).send({ error: `File too large. Max ${friendlySize(MAX_FILE_BYTES)}.` });
        chunks.push(chunk as Buffer);
      }
      fileBuffer = Buffer.concat(chunks);
    } catch { return reply.status(400).send({ error: "Could not read the uploaded file." }); }

    try {
      const uploaded = await erp.uploadFile(ctx.creds, { buffer: fileBuffer, filename: fileName, contentType, isPrivate: false, doctype: "Company", docname: companyId });
      const fileDoc = ((uploaded as Record<string, unknown>).message as Record<string, unknown>) ?? (uploaded as Record<string, unknown>);
      return { data: { name: String(fileDoc.name ?? ""), file_name: String(fileDoc.file_name ?? fileName) } };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });

  // ── GET /v1/employees/:id/documents ──────────────────────────────────────
  app.get("/v1/employees/:id/documents", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }

    const employeeId = (req.params as { id: string }).id;

    // HR can see anyone's docs in the company; employees can only see their own
    if (!ctx.canSubmitOnBehalf) {
      try {
        const mine = await erp.getList(ctx.creds, "Employee", {
          filters: [["user_id", "=", ctx.userEmail], ["company", "=", ctx.company]],
          fields: ["name"],
          limit_page_length: 1,
        });
        if (!mine.length || String((mine[0] as { name: string }).name) !== employeeId) {
          return reply.status(403).send({ error: "You can only access your own documents" });
        }
      } catch (e) {
        if (e instanceof ErpError) return reply.status(403).send({ error: "Access denied" });
        throw e;
      }
    } else {
      // Verify employee belongs to this company
      try {
        const emp = await erp.getDoc(ctx.creds, "Employee", employeeId) as Record<string, unknown>;
        if (String(emp.company) !== ctx.company) return reply.status(403).send({ error: "Employee not in your company" });
      } catch (e) {
        if (e instanceof ErpError) return reply.status(404).send({ error: "Employee not found" });
        throw e;
      }
    }

    try {
      const rows = await erp.getList(ctx.creds, "File", {
        filters: [
          ["attached_to_doctype", "=", "Employee"],
          ["attached_to_name", "=", employeeId],
        ],
        fields: ["name", "file_name", "file_url", "file_size", "is_private", "creation", "modified", "content_hash"],
        order_by: "creation desc",
        limit_page_length: 200,
      });

      const data = (rows as Record<string, unknown>[]).map((r) => ({
        id: String(r.name ?? ""),
        fileName: String(r.file_name ?? ""),
        fileUrl: String(r.file_url ?? ""),
        fileSize: Number(r.file_size ?? 0),
        fileSizeLabel: friendlySize(Number(r.file_size ?? 0)),
        isPrivate: Boolean(r.is_private),
        uploadedAt: String(r.creation ?? ""),
        modifiedAt: String(r.modified ?? ""),
        contentHash: String(r.content_hash ?? ""),
      }));

      return { data };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });

  // ── POST /v1/employees/:id/documents ─────────────────────────────────────
  // Accepts multipart/form-data with fields: file (binary), isPrivate (boolean, default 1)
  app.post("/v1/employees/:id/documents", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    const employeeId = (req.params as { id: string }).id;

    // Verify employee belongs to company
    try {
      const emp = await erp.getDoc(ctx.creds, "Employee", employeeId) as Record<string, unknown>;
      if (String(emp.company) !== ctx.company) return reply.status(403).send({ error: "Employee not in your company" });
    } catch (e) {
      if (e instanceof ErpError) return reply.status(404).send({ error: "Employee not found" });
      throw e;
    }

    // Parse multipart body
    let fileBuffer: Buffer;
    let fileName: string;
    let contentType: string;
    let isPrivate = true;

    try {
      const data = await req.file();
      if (!data) return reply.status(400).send({ error: "No file provided" });

      fileName = data.filename || "document";
      contentType = data.mimetype || "application/octet-stream";

      if (!isMimeAllowed(contentType)) {
        return reply.status(415).send({
          error: `File type "${contentType}" is not allowed. Upload PDF, Word, Excel, or image files.`,
        });
      }

      const chunks: Buffer[] = [];
      let total = 0;
      for await (const chunk of data.file) {
        total += (chunk as Buffer).length;
        if (total > MAX_FILE_BYTES) {
          return reply.status(413).send({ error: `File is too large. Maximum size is ${friendlySize(MAX_FILE_BYTES)}.` });
        }
        chunks.push(chunk as Buffer);
      }
      fileBuffer = Buffer.concat(chunks);

      // isPrivate can be passed as a field alongside the file
      const fields = data.fields as Record<string, { value: string }>;
      if (fields?.isPrivate?.value === "0" || fields?.isPrivate?.value === "false") {
        isPrivate = false;
      }
    } catch (e) {
      if ((e as { code?: string }).code === "FST_FILES_LIMIT") {
        return reply.status(400).send({ error: "Too many files. Upload one at a time." });
      }
      return reply.status(400).send({ error: "Could not read the uploaded file." });
    }

    try {
      const uploaded = await erp.uploadFile(ctx.creds, {
        buffer: fileBuffer,
        filename: fileName,
        contentType,
        isPrivate,
        doctype: "Employee",
        docname: employeeId,
      });

      const u = uploaded as Record<string, unknown>;
      const fileDoc = u.message as Record<string, unknown> ?? u;
      return {
        data: {
          id: String(fileDoc.name ?? ""),
          fileName: String(fileDoc.file_name ?? fileName),
          fileUrl: String(fileDoc.file_url ?? ""),
          fileSize: Number(fileDoc.file_size ?? fileBuffer.length),
          fileSizeLabel: friendlySize(Number(fileDoc.file_size ?? fileBuffer.length)),
          isPrivate,
          uploadedAt: String(fileDoc.creation ?? new Date().toISOString()),
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });

  // ── DELETE /v1/employees/:id/documents/:fileId ────────────────────────────
  app.delete("/v1/employees/:id/documents/:fileId", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    const { id: employeeId, fileId } = req.params as { id: string; fileId: string };

    // Verify the file belongs to this employee (security check)
    try {
      const file = await erp.getDoc(ctx.creds, "File", fileId) as Record<string, unknown>;
      if (String(file.attached_to_doctype) !== "Employee" || String(file.attached_to_name) !== employeeId) {
        return reply.status(403).send({ error: "File does not belong to this employee" });
      }
    } catch (e) {
      if (e instanceof ErpError) return reply.status(404).send({ error: "Document not found" });
      throw e;
    }

    try {
      await erp.deleteDoc(ctx.creds, "File", fileId);
      return { data: { deleted: true, fileId } };
    } catch (e) {
      if (e instanceof ErpError) return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      throw e;
    }
  });
};
