import type { ErpCredentials } from "../erpnext/client.js";
import { defaultClient, ErpError } from "../erpnext/client.js";
import * as config from "../config.js";
import {
  zkConnect,
  zkDisconnect,
  zkGetAttendances,
  getWatermark,
  setWatermark,
} from "./zktecoClient.js";

const erp = defaultClient();

export type SyncStats = {
  total_device_logs: number;
  processed: number;
  synced: number;
  skipped: number;
  errors: number;
  error_samples: string[];
  new_watermark: string;
};

// node-zklib doesn't expose the inout state byte.
// Strategy: per employee per calendar day, assign IN/OUT alternately by scan order,
// resuming from the last known log_type already in ERPNext. Using the actual last
// type (rather than a count-based offset) is resilient to silent write failures in
// earlier sync runs — a failed write won't shift every subsequent punch to the wrong type.
function buildLogTypeMap(
  logs: Array<{ deviceUserId: string; recordTime: Date }>,
  existingByDevice: Map<string, Array<{ time: Date; log_type: "IN" | "OUT" }>>
): Map<string, "IN" | "OUT"> {
  const result = new Map<string, "IN" | "OUT">();

  // Group new scans by employee + UTC calendar day
  const groups = new Map<string, Array<{ deviceUserId: string; recordTime: Date }>>();
  for (const l of logs) {
    const dateKey = l.recordTime.toISOString().slice(0, 10);
    const groupKey = `${l.deviceUserId}|${dateKey}`;
    if (!groups.has(groupKey)) groups.set(groupKey, []);
    groups.get(groupKey)!.push(l);
  }

  for (const [groupKey, records] of groups.entries()) {
    const sorted = [...records].sort((a, b) => a.recordTime.getTime() - b.recordTime.getTime());
    const deviceId = sorted[0].deviceUserId;
    const dateKey = groupKey.split("|")[1];

    // Most-recent existing record for this device on this date determines the next type.
    const lastExisting = (existingByDevice.get(deviceId) ?? [])
      .filter(r => r.time.toISOString().slice(0, 10) === dateKey)
      .sort((a, b) => b.time.getTime() - a.time.getTime())[0] ?? null;

    let lastType: "IN" | "OUT" | null = lastExisting?.log_type ?? null;

    for (const rec of sorted) {
      const nextType: "IN" | "OUT" = lastType === "IN" ? "OUT" : "IN";
      result.set(`${rec.deviceUserId}|${rec.recordTime.toISOString()}`, nextType);
      lastType = nextType;
    }
  }

  return result;
}

function toErpDatetime(d: Date): string {
  return d.toISOString().replace("T", " ").slice(0, 19);
}

// Ensure an ERPNext Attendance record exists and is marked Present for each IN punch
// that was successfully synced. This mirrors what the web clock-in endpoint does so
// that fingerprint-only users are never left showing "Absent" in the UI.
async function ensureAttendanceForInPunches(
  creds: ErpCredentials,
  company: string,
  inPunches: Array<{ empName: string; date: string; inTime: string }>,
): Promise<void> {
  if (inPunches.length === 0) return;

  const empNames = [...new Set(inPunches.map(p => p.empName))];
  const dates    = [...new Set(inPunches.map(p => p.date))];

  // Batch-fetch existing Attendance records for the affected employees/dates
  const existingAttRows = (await erp.getList(creds, "Attendance", {
    filters: [
      ["employee", "in", empNames],
      ["attendance_date", "in", dates],
    ],
    fields: ["name", "employee", "attendance_date", "in_time", "docstatus"],
    limit_page_length: 1000,
  })) as Record<string, unknown>[];

  const attMap = new Map<string, { name: string; in_time: string; docstatus: number }>();
  for (const row of existingAttRows) {
    const key = `${row.employee}|${row.attendance_date}`;
    attMap.set(key, {
      name: String(row.name ?? ""),
      in_time: String(row.in_time ?? "").trim(),
      docstatus: Number(row.docstatus ?? 0),
    });
  }

  // Batch-fetch active Shift Assignments so we can populate the shift field
  const shiftRows = (await erp.getList(creds, "Shift Assignment", {
    filters: [
      ["employee", "in", empNames],
      ["start_date", "<=", dates[dates.length - 1]],
      ["docstatus", "=", 1],
    ],
    fields: ["employee", "shift_type", "start_date", "end_date"],
    limit_page_length: 1000,
  })) as Record<string, unknown>[];

  // Build a lookup: empName → most-specific shift for a given date
  function shiftForEmployee(empName: string, date: string): string | null {
    const matches = shiftRows.filter(r => {
      if (String(r.employee) !== empName) return false;
      if (String(r.start_date) > date) return false;
      const end = String(r.end_date ?? "");
      if (end && end < date) return false;
      return true;
    });
    // Prefer the assignment with the latest start_date (most specific)
    matches.sort((a, b) => String(b.start_date).localeCompare(String(a.start_date)));
    return matches[0] ? String(matches[0].shift_type) : null;
  }

  for (const { empName, date, inTime } of inPunches) {
    const key = `${empName}|${date}`;
    const existing = attMap.get(key);
    const shift = shiftForEmployee(empName, date);

    try {
      if (existing) {
        // Don't touch submitted (docstatus=1) records — they're locked in ERPNext
        if (existing.docstatus === 1) continue;
        await erp.updateDoc(creds, "Attendance", existing.name, {
          status: "Present",
          ...(shift ? { shift } : {}),
          // Only set in_time if the record doesn't already have one
          ...(inTime && !existing.in_time ? { in_time: inTime } : {}),
        });
      } else {
        await erp.createDoc(creds, "Attendance", {
          employee: empName,
          company,
          attendance_date: date,
          status: "Present",
          ...(shift ? { shift } : {}),
          in_time: inTime,
        });
      }
    } catch {
      // Non-fatal: a failed Attendance upsert should not abort the checkin sync
    }
  }
}

export async function runZktecoSync(
  creds: ErpCredentials,
  company: string,
  fromDateOverride?: string
): Promise<SyncStats> {
  let zk: unknown = null;
  try {
    zk = await zkConnect(config.ZKTECO_HOST, config.ZKTECO_PORT, config.ZKTECO_TIMEOUT_MS);
  } catch (e) {
    throw new Error(`ZKTeco device unreachable: ${String(e)}`);
  }

  try {
    const logs = await zkGetAttendances(zk);

    let cutoff: Date | null = null;
    if (fromDateOverride) {
      const d = new Date(`${fromDateOverride}T00:00:00Z`);
      if (!Number.isNaN(d.getTime())) cutoff = d;
    } else {
      cutoff = getWatermark(config.ZKTECO_HOST, config.ZKTECO_PORT, company);
    }

    const toProcess = cutoff ? logs.filter((l) => l.recordTime >= cutoff!) : logs;

    const empRows = (await erp.getList(creds, "Employee", {
      filters: [
        ["company", "=", company],
        ["attendance_device_id", "!=", ""],
      ],
      fields: ["name", "employee_name", "attendance_device_id"],
      limit_page_length: 5000,
    })) as Record<string, unknown>[];

    const empMap = new Map<string, { name: string; employee_name: string }>();
    for (const row of empRows) {
      const devId = String(row.attendance_device_id ?? "").trim();
      if (devId) empMap.set(devId, { name: String(row.name ?? ""), employee_name: String(row.employee_name ?? "") });
    }

    let synced = 0;
    let skipped = 0;
    let errors = 0;
    const error_samples: string[] = [];

    // Fetch existing Employee Checkin records for today with their actual log_type and
    // timestamps so buildLogTypeMap can resume from the correct last-known state rather
    // than relying on a count that drifts when writes fail silently.
    const existingByDevice = new Map<string, Array<{ time: Date; log_type: "IN" | "OUT" }>>();
    if (empMap.size > 0) {
      const today = new Date().toISOString().slice(0, 10);
      const existingRows = (await erp.getList(creds, "Employee Checkin", {
        filters: [
          ["employee", "in", [...empMap.values()].map((e) => e.name)],
          ["time", ">=", `${today} 00:00:00`],
          ["time", "<=", `${today} 23:59:59`],
        ],
        fields: ["employee", "time", "log_type"],
        limit_page_length: 1000,
      })) as Record<string, unknown>[];

      // Build reverse map: employee name → deviceId
      const empNameToDeviceId = new Map<string, string>();
      for (const [deviceId, emp] of empMap.entries()) {
        empNameToDeviceId.set(emp.name, deviceId);
      }

      for (const row of existingRows) {
        const empName = String(row.employee ?? "");
        const deviceId = empNameToDeviceId.get(empName);
        if (!deviceId) continue;
        const rawTime = String(row.time ?? "");
        // ERPNext returns "YYYY-MM-DD HH:MM:SS" (stored as UTC); parse as UTC.
        const time = new Date(rawTime.includes("T") ? rawTime : rawTime.replace(" ", "T") + "Z");
        const log_type = String(row.log_type ?? "") as "IN" | "OUT";
        if (log_type !== "IN" && log_type !== "OUT") continue;
        if (!existingByDevice.has(deviceId)) existingByDevice.set(deviceId, []);
        existingByDevice.get(deviceId)!.push({ time, log_type });
      }
    }

    const logTypeMap = buildLogTypeMap(toProcess, existingByDevice);

    // Track IN punches that were successfully synced so we can ensure their
    // Attendance record is marked Present after the checkin loop.
    const syncedInPunches: Array<{ empName: string; date: string; inTime: string }> = [];
    // Deduplicate by empName|date — keep the earliest IN per employee per day
    const inPunchSeen = new Set<string>();

    for (const log of toProcess) {
      const logType = logTypeMap.get(`${log.deviceUserId}|${log.recordTime.toISOString()}`);
      if (!logType) { skipped++; continue; }
      const emp = empMap.get(log.deviceUserId);
      if (!emp) { skipped++; continue; }
      try {
        await erp.createDoc(creds, "Employee Checkin", {
          employee: emp.name,
          employee_name: emp.employee_name,
          log_type: logType,
          time: toErpDatetime(log.recordTime),
          device_id: `zkteco:${config.ZKTECO_HOST}`,
        });
        synced++;

        if (logType === "IN") {
          const date = log.recordTime.toISOString().slice(0, 10);
          const key = `${emp.name}|${date}`;
          if (!inPunchSeen.has(key)) {
            inPunchSeen.add(key);
            syncedInPunches.push({ empName: emp.name, date, inTime: toErpDatetime(log.recordTime) });
          }
        }
      } catch (e) {
        const isDuplicate =
          e instanceof ErpError &&
          (e.status === 409 ||
            (e.status === 417 &&
              JSON.stringify(e.body ?? "").includes("already has a log with the same timestamp")));
        if (isDuplicate) {
          skipped++;
        } else {
          errors++;
          if (error_samples.length < 10) {
            let detail = String(e);
            if (e instanceof ErpError && e.body) {
              const b = e.body as Record<string, unknown>;
              const raw = typeof b._server_messages === "string" ? b._server_messages : null;
              if (raw) {
                try {
                  const msgs = JSON.parse(raw) as Array<{ message?: string } | string>;
                  detail = msgs.map((m) => (typeof m === "string" ? m : (m.message ?? ""))).filter(Boolean).join("; ");
                } catch { /* ignore */ }
              } else if (typeof b.exception === "string") {
                detail = b.exception.split("\n").filter(Boolean).pop() ?? detail;
              } else if (typeof b.message === "string") {
                detail = b.message;
              }
            }
            error_samples.push(`sn=${log.userSn} emp=${emp.name}: ${detail}`);
          }
        }
      }
    }

    // Ensure Attendance records are marked Present for every newly synced IN punch.
    // This mirrors what the web clock-in endpoint does, so fingerprint-only users
    // are never left showing "Absent" due to a missing Attendance record.
    await ensureAttendanceForInPunches(creds, company, syncedInPunches);

    const now = new Date();
    if (!fromDateOverride) setWatermark(config.ZKTECO_HOST, config.ZKTECO_PORT, company, now);

    return {
      total_device_logs: logs.length,
      processed: toProcess.length,
      synced,
      skipped,
      errors,
      error_samples,
      new_watermark: now.toISOString(),
    };
  } finally {
    await zkDisconnect(zk);
  }
}
