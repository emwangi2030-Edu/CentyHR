import { Router } from "express";
import { requireAuth } from "../middleware/auth";
import { storage } from "../storage";

const router = Router();

router.get("/api/centypack/stock/on-hand", requireAuth, async (req, res) => {
  const businessId = req.business?.id ?? req.user?.businessId;
  if (!businessId) return res.status(400).json({ error: "No business linked to this account" });
  const cropId = typeof req.query.cropId === "string" ? req.query.cropId : undefined;
  const stage = typeof req.query.stage === "string" ? req.query.stage : undefined;
  const warehouseId = typeof req.query.warehouseId === "string" ? req.query.warehouseId : undefined;
  try {
    const rows = await storage.getStockOnHand(businessId, { cropId, stage, warehouseId });
    return res.json({ data: rows });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return res.status(500).json({ error: `Failed to get stock on hand: ${msg}` });
  }
});

router.get("/api/centypack/stock/movements", requireAuth, async (req, res) => {
  const businessId = req.business?.id ?? req.user?.businessId;
  if (!businessId) return res.status(400).json({ error: "No business linked to this account" });
  const txnId = typeof req.query.txnId === "string" ? req.query.txnId : undefined;
  const txnType = typeof req.query.txnType === "string" ? req.query.txnType : undefined;
  const cropId = typeof req.query.cropId === "string" ? req.query.cropId : undefined;
  const stage = typeof req.query.stage === "string" ? req.query.stage : undefined;
  const dateFrom = typeof req.query.dateFrom === "string" ? req.query.dateFrom : undefined;
  const dateTo = typeof req.query.dateTo === "string" ? req.query.dateTo : undefined;
  const limit = Math.min(Number(req.query.limit) || 100, 500);
  const offset = Number(req.query.offset) || 0;
  try {
    const result = await storage.getStockMovements(businessId, { txnId, txnType, cropId, stage, dateFrom, dateTo, limit, offset });
    return res.json({ data: result.rows, total: result.total, limit, offset });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return res.status(500).json({ error: `Failed to get stock movements: ${msg}` });
  }
});

export default router;
