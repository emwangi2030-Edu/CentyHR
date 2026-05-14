import { Router } from "express";
import { storage } from "../storage";
import { requireAuth } from "../middleware/auth";

const router = Router();

router.get("/api/centypack/labourers/next-code", requireAuth, async (req, res) => {
  const businessId = req.business?.id ?? req.user?.businessId;
  if (!businessId) return res.status(400).json({ error: "No business linked to this account" });
  try {
    const code = await storage.nextCentypackLabourerCode(businessId);
    return res.json({ code });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return res.status(500).json({ error: `Failed to generate code: ${msg}` });
  }
});

router.get("/api/centypack/labourers", requireAuth, async (req, res) => {
  const businessId = req.business?.id ?? req.user?.businessId;
  if (!businessId) return res.status(400).json({ error: "No business linked to this account" });
  const search = typeof req.query.search === "string" ? req.query.search.trim() : undefined;
  const status = typeof req.query.status === "string" ? req.query.status : undefined;
  const workerCategoryId = typeof req.query.workerCategoryId === "string" ? req.query.workerCategoryId : undefined;
  const limit = Math.min(Number(req.query.limit) || 100, 500);
  const offset = Number(req.query.offset) || 0;
  try {
    const result = await storage.listCentypackLabourers(businessId, { search, status, workerCategoryId, limit, offset });
    return res.json(result);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return res.status(500).json({ error: `Failed to list labourers: ${msg}` });
  }
});

router.get("/api/centypack/labourers/:id", requireAuth, async (req, res) => {
  const businessId = req.business?.id ?? req.user?.businessId;
  if (!businessId) return res.status(400).json({ error: "No business linked to this account" });
  try {
    const row = await storage.getCentypackLabourerById(String(req.params.id), businessId);
    if (!row) return res.status(404).json({ error: "Labourer not found" });
    return res.json(row);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return res.status(500).json({ error: `Failed to get labourer: ${msg}` });
  }
});

router.post("/api/centypack/labourers", requireAuth, async (req, res) => {
  const businessId = req.business?.id ?? req.user?.businessId;
  if (!businessId) return res.status(400).json({ error: "No business linked to this account" });
  const { labourerCode: rawCode, firstName, lastName, phone, workerCategoryId, status, notes } = req.body as Record<string, unknown>;
  if (!firstName || !lastName) {
    return res.status(400).json({ error: "firstName and lastName are required" });
  }
  try {
    const labourerCode = rawCode ? String(rawCode).trim().toUpperCase() : await storage.nextCentypackLabourerCode(businessId);
    const row = await storage.createCentypackLabourer({
      businessId,
      labourerCode,
      firstName: String(firstName).trim(),
      lastName: String(lastName).trim(),
      phone: phone ? String(phone).trim() : null,
      workerCategoryId: workerCategoryId ? String(workerCategoryId) : null,
      status: status ? String(status) : "active",
      notes: notes ? String(notes).trim() : null,
    });
    return res.status(201).json(row);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (msg.includes("unique") || msg.includes("duplicate")) {
      return res.status(409).json({ error: "A labourer with that code already exists" });
    }
    return res.status(500).json({ error: `Failed to create labourer: ${msg}` });
  }
});

router.put("/api/centypack/labourers/:id", requireAuth, async (req, res) => {
  const businessId = req.business?.id ?? req.user?.businessId;
  if (!businessId) return res.status(400).json({ error: "No business linked to this account" });
  const body = req.body as Record<string, unknown>;
  try {
    const row = await storage.updateCentypackLabourer(String(req.params.id), businessId, {
      firstName: body.firstName ? String(body.firstName).trim() : undefined,
      lastName: body.lastName ? String(body.lastName).trim() : undefined,
      phone: body.phone !== undefined ? (body.phone ? String(body.phone).trim() : null) : undefined,
      workerCategoryId: body.workerCategoryId !== undefined ? (body.workerCategoryId ? String(body.workerCategoryId) : null) : undefined,
      status: body.status ? String(body.status) : undefined,
      notes: body.notes !== undefined ? (body.notes ? String(body.notes).trim() : null) : undefined,
    });
    if (!row) return res.status(404).json({ error: "Labourer not found" });
    return res.json(row);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return res.status(500).json({ error: `Failed to update labourer: ${msg}` });
  }
});

export default router;
