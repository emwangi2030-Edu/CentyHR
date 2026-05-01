import { Router } from "express";
import { requireAuth } from "../middleware/auth";
import { storage } from "../storage";

const router = Router();

const VALID_STATUSES = ["draft", "completed", "cancelled"];

/** GET /api/centypack/pack-sessions */
router.get("/api/centypack/pack-sessions", requireAuth, async (req, res) => {
  const businessId = req.business?.id ?? req.user?.businessId;
  if (!businessId) return res.status(400).json({ error: "No business linked to this account" });
  const search = typeof req.query.search === "string" ? req.query.search.trim() : undefined;
  const cropId = typeof req.query.cropId === "string" ? req.query.cropId : undefined;
  const status = typeof req.query.status === "string" ? req.query.status : undefined;
  const dateFrom = typeof req.query.dateFrom === "string" ? req.query.dateFrom : undefined;
  const dateTo = typeof req.query.dateTo === "string" ? req.query.dateTo : undefined;
  const limit = Math.min(Number(req.query.limit) || 50, 200);
  const offset = Number(req.query.offset) || 0;
  try {
    const { rows, total } = await storage.listCentypackPackSessions(businessId, { search, cropId, status, dateFrom, dateTo, limit, offset });
    return res.json({ data: rows, total, limit, offset });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return res.status(500).json({ error: `Failed to list pack sessions: ${msg}` });
  }
});

/** GET /api/centypack/pack-sessions/next-code */
router.get("/api/centypack/pack-sessions/next-code", requireAuth, async (req, res) => {
  const businessId = req.business?.id ?? req.user?.businessId;
  if (!businessId) return res.status(400).json({ error: "No business linked to this account" });
  try {
    const code = await storage.nextCentypackPackCode(businessId);
    return res.json({ code });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return res.status(500).json({ error: `Failed to generate pack session code: ${msg}` });
  }
});

/** GET /api/centypack/pack-sessions/:id */
router.get("/api/centypack/pack-sessions/:id", requireAuth, async (req, res) => {
  const businessId = req.business?.id ?? req.user?.businessId;
  if (!businessId) return res.status(400).json({ error: "No business linked to this account" });
  try {
    const record = await storage.getCentypackPackSessionById(String(req.params.id), businessId);
    if (!record) return res.status(404).json({ error: "Pack session not found" });
    return res.json({ data: record });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return res.status(500).json({ error: `Failed to get pack session: ${msg}` });
  }
});

/** POST /api/centypack/pack-sessions */
router.post("/api/centypack/pack-sessions", requireAuth, async (req, res) => {
  const user = req.user!;
  if (!["super_admin", "admin", "business_manager"].includes(user.role))
    return res.status(403).json({ error: "Only admins and business managers can create pack sessions" });
  const businessId = req.business?.id ?? user.businessId;
  if (!businessId) return res.status(400).json({ error: "No business linked to this account" });

  const { sessionDate, gradingSessionId, intakeId, cropId, varietyId, notes, status, lines } = req.body as Record<string, unknown>;

  if (!sessionDate || typeof sessionDate !== "string" || !sessionDate.trim())
    return res.status(400).json({ error: "Session date is required" });
  if (!cropId || typeof cropId !== "string")
    return res.status(400).json({ error: "Crop is required" });
  if (status != null && !VALID_STATUSES.includes(String(status)))
    return res.status(400).json({ error: `Status must be one of: ${VALID_STATUSES.join(", ")}` });
  if (!Array.isArray(lines) || lines.length === 0)
    return res.status(400).json({ error: "At least one pack line is required" });

  for (let i = 0; i < lines.length; i++) {
    const line = (lines as Record<string, unknown>[])[i];
    if (line.netWeightKg == null || isNaN(Number(line.netWeightKg)) || Number(line.netWeightKg) <= 0)
      return res.status(400).json({ error: `Line ${i + 1}: net weight must be a positive number` });
  }

  try {
    const sessionCode = await storage.nextCentypackPackCode(businessId);
    const parsedLines = (lines as Record<string, unknown>[]).map(line => ({
      gradeCodeId: line.gradeCodeId ? String(line.gradeCodeId) : null,
      cartonTypeId: line.cartonTypeId ? String(line.cartonTypeId) : null,
      cartonCount: line.cartonCount != null && !isNaN(Number(line.cartonCount)) ? Math.floor(Number(line.cartonCount)) : null,
      netWeightKg: String(Number(line.netWeightKg)),
      notes: typeof line.notes === "string" ? line.notes.trim() || null : null,
      labourers: Array.isArray(line.labourers)
        ? (line.labourers as Record<string, unknown>[])
            .filter(la => la.labourerId)
            .map(la => ({ labourerId: String(la.labourerId), cartonCount: Math.floor(Number(la.cartonCount)) || 0 }))
        : [],
    }));
    const record = await storage.createCentypackPackSession(
      {
        businessId,
        sessionCode,
        sessionDate: String(sessionDate).trim(),
        gradingSessionId: gradingSessionId ? String(gradingSessionId) : null,
        intakeId: intakeId ? String(intakeId) : null,
        cropId: String(cropId),
        varietyId: varietyId ? String(varietyId) : null,
        notes: typeof notes === "string" ? notes.trim() || null : null,
        status: status ? String(status) : "draft",
      },
      parsedLines,
    );
    return res.status(201).json({ data: record });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return res.status(500).json({ error: `Failed to create pack session: ${msg}` });
  }
});

/** PUT /api/centypack/pack-sessions/:id */
router.put("/api/centypack/pack-sessions/:id", requireAuth, async (req, res) => {
  const user = req.user!;
  if (!["super_admin", "admin", "business_manager"].includes(user.role))
    return res.status(403).json({ error: "Only admins and business managers can update pack sessions" });
  const businessId = req.business?.id ?? user.businessId;
  if (!businessId) return res.status(400).json({ error: "No business linked to this account" });

  const { sessionDate, gradingSessionId, intakeId, cropId, varietyId, notes, status, lines } = req.body as Record<string, unknown>;

  if (status != null && !VALID_STATUSES.includes(String(status)))
    return res.status(400).json({ error: `Status must be one of: ${VALID_STATUSES.join(", ")}` });

  let parsedLines: { gradeCodeId: string | null; cartonTypeId: string | null; cartonCount: number | null; netWeightKg: string; notes: string | null }[] | undefined;
  if (lines !== undefined) {
    if (!Array.isArray(lines) || lines.length === 0)
      return res.status(400).json({ error: "At least one pack line is required" });
    for (let i = 0; i < lines.length; i++) {
      const line = (lines as Record<string, unknown>[])[i];
      if (line.netWeightKg == null || isNaN(Number(line.netWeightKg)) || Number(line.netWeightKg) <= 0)
        return res.status(400).json({ error: `Line ${i + 1}: net weight must be a positive number` });
    }
    parsedLines = (lines as Record<string, unknown>[]).map(line => ({
      gradeCodeId: line.gradeCodeId ? String(line.gradeCodeId) : null,
      cartonTypeId: line.cartonTypeId ? String(line.cartonTypeId) : null,
      cartonCount: line.cartonCount != null && !isNaN(Number(line.cartonCount)) ? Math.floor(Number(line.cartonCount)) : null,
      netWeightKg: String(Number(line.netWeightKg)),
      notes: typeof line.notes === "string" ? line.notes.trim() || null : null,
      labourers: Array.isArray(line.labourers)
        ? (line.labourers as Record<string, unknown>[])
            .filter(la => la.labourerId)
            .map(la => ({ labourerId: String(la.labourerId), cartonCount: Math.floor(Number(la.cartonCount)) || 0 }))
        : [],
    }));
  }

  const headerPatch: Record<string, unknown> = {};
  if (sessionDate != null) headerPatch.sessionDate = String(sessionDate).trim();
  if (gradingSessionId !== undefined) headerPatch.gradingSessionId = gradingSessionId ? String(gradingSessionId) : null;
  if (intakeId !== undefined) headerPatch.intakeId = intakeId ? String(intakeId) : null;
  if (cropId != null) headerPatch.cropId = String(cropId);
  if (varietyId !== undefined) headerPatch.varietyId = varietyId ? String(varietyId) : null;
  if (notes !== undefined) headerPatch.notes = typeof notes === "string" ? notes.trim() || null : null;
  if (status != null) headerPatch.status = String(status);

  try {
    const record = await storage.updateCentypackPackSession(String(req.params.id), businessId, headerPatch as any, parsedLines);
    if (!record) return res.status(404).json({ error: "Pack session not found" });
    return res.json({ data: record });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return res.status(500).json({ error: `Failed to update pack session: ${msg}` });
  }
});

export default router;
