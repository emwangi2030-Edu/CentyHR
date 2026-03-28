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

export const documentsRoutes: FastifyPluginAsync = async (app) => {
  await app.register(multipart, { limits: { fileSize: MAX_FILE_BYTES, files: 1 } });

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
