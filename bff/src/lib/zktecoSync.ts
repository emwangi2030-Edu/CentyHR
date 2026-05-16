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
// Strategy: per employee per calendar day, assign IN/OUT alternately by scan order.
// The offset parameter accounts for checkins already written to ERPNext in earlier
// sync runs so the sequence continues correctly across multiple runs:
//   existing=0 → 1st new scan = IN, 2nd = OUT, …
//   existing=1 → 1st new scan = OUT, 2nd = IN, …
function buildLogTypeMap(
  logs: Array<{ deviceUserId: string; recordTime: Date }>,
  existingCountByDevice: Map<string, number>
): Map<string, "IN" | "OUT"> {
  const result = new Map<string, "IN" | "OUT">();

  // Group scans by employee + UTC calendar day
  const groups = new Map<string, Array<{ deviceUserId: string; recordTime: Date }>>();
  for (const l of logs) {
    const dateKey = l.recordTime.toISOString().slice(0, 10);
    const groupKey = `${l.deviceUserId}|${dateKey}`;
    if (!groups.has(groupKey)) groups.set(groupKey, []);
    groups.get(groupKey)!.push(l);
  }

  for (const records of groups.values()) {
    const sorted = [...records].sort((a, b) => a.recordTime.getTime() - b.recordTime.getTime());
    const deviceId = sorted[0].deviceUserId;
    const offset = existingCountByDevice.get(deviceId) ?? 0;
    sorted.forEach((rec, idx) => {
      result.set(`${rec.deviceUserId}|${rec.recordTime.toISOString()}`, (offset + idx) % 2 === 0 ? "IN" : "OUT");
    });
  }
  return result;
}

function toErpDatetime(d: Date): string {
  return d.toISOString().replace("T", " ").slice(0, 19);
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

    // Count how many Employee Checkin records already exist today per employee so the
    // alternating IN/OUT sequence continues correctly across multiple sync runs.
    const existingCountByDevice = new Map<string, number>();
    if (empMap.size > 0) {
      const today = new Date().toISOString().slice(0, 10);
      const existingRows = (await erp.getList(creds, "Employee Checkin", {
        filters: [
          ["employee", "in", [...empMap.values()].map((e) => e.name)],
          ["time", ">=", `${today} 00:00:00`],
          ["time", "<=", `${today} 23:59:59`],
        ],
        fields: ["employee"],
        limit_page_length: 1000,
      })) as Record<string, unknown>[];
      const countByEmpName = new Map<string, number>();
      for (const row of existingRows) {
        const n = String(row.employee ?? "");
        countByEmpName.set(n, (countByEmpName.get(n) ?? 0) + 1);
      }
      for (const [deviceId, emp] of empMap.entries()) {
        existingCountByDevice.set(deviceId, countByEmpName.get(emp.name) ?? 0);
      }
    }

    const logTypeMap = buildLogTypeMap(toProcess, existingCountByDevice);

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
              // ERPNext wraps validation messages in _server_messages (JSON array of objects)
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
