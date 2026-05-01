import { Router } from "express";
import { storage } from "../storage";
import { requireAuth } from "../middleware/auth";

const router = Router();

router.get("/api/centypack/labour-attendance", requireAuth, async (req, res) => {
  const businessId = req.business?.id ?? req.user?.businessId;
  if (!businessId) return res.status(400).json({ error: "No business linked to this account" });
  const date = typeof req.query.date === "string" ? req.query.date : "";
  if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    return res.status(400).json({ error: "date (YYYY-MM-DD) is required" });
  }
  try {
    const rows = await storage.getLabourAttendanceByDate(businessId, date);
    return res.json(rows);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return res.status(500).json({ error: `Failed to get attendance: ${msg}` });
  }
});

router.put("/api/centypack/labour-attendance", requireAuth, async (req, res) => {
  const businessId = req.business?.id ?? req.user?.businessId;
  if (!businessId) return res.status(400).json({ error: "No business linked to this account" });
  const { date, records } = req.body as { date?: string; records?: { labourerId: string; status: string; notes?: string | null }[] };
  if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    return res.status(400).json({ error: "date (YYYY-MM-DD) is required" });
  }
  if (!Array.isArray(records) || records.length === 0) {
    return res.status(400).json({ error: "records array is required" });
  }
  const VALID = new Set(["present", "absent", "half-day"]);
  for (const r of records) {
    if (!r.labourerId || !VALID.has(r.status)) {
      return res.status(400).json({ error: "Each record needs labourerId and status (present|absent|half-day)" });
    }
  }
  try {
    await storage.upsertLabourAttendance(businessId, date, records);
    return res.json({ ok: true });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return res.status(500).json({ error: `Failed to save attendance: ${msg}` });
  }
});

router.get("/api/centypack/labour-pack-summary", requireAuth, async (req, res) => {
  const businessId = req.business?.id ?? req.user?.businessId;
  if (!businessId) return res.status(400).json({ error: "No business linked to this account" });
  const dateFrom = typeof req.query.dateFrom === "string" ? req.query.dateFrom : undefined;
  const dateTo = typeof req.query.dateTo === "string" ? req.query.dateTo : undefined;
  const labourerId = typeof req.query.labourerId === "string" ? req.query.labourerId : undefined;
  const packSessionId = typeof req.query.packSessionId === "string" ? req.query.packSessionId : undefined;
  try {
    const rows = await storage.getLabourPackSummary(businessId, { dateFrom, dateTo, labourerId, packSessionId });
    return res.json(rows);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return res.status(500).json({ error: `Failed to get pack summary: ${msg}` });
  }
});

router.get("/api/centypack/pack-lines/:packLineId/labourers", requireAuth, async (req, res) => {
  try {
    const rows = await storage.getPackLineLabourers(String(req.params.packLineId));
    return res.json(rows);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return res.status(500).json({ error: `Failed to get pack line labourers: ${msg}` });
  }
});

router.put("/api/centypack/pack-lines/:packLineId/labourers", requireAuth, async (req, res) => {
  const businessId = req.business?.id ?? req.user?.businessId;
  if (!businessId) return res.status(400).json({ error: "No business linked to this account" });
  const { assignments } = req.body as { assignments?: { labourerId: string; cartonCount: number }[] };
  if (!Array.isArray(assignments)) {
    return res.status(400).json({ error: "assignments array is required" });
  }
  try {
    await storage.upsertPackLineLabourers(String(req.params.packLineId), businessId, assignments);
    return res.json({ ok: true });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return res.status(500).json({ error: `Failed to update pack line labourers: ${msg}` });
  }
});

export default router;
