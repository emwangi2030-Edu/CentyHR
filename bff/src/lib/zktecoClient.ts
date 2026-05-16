import { createRequire } from "module";
import { readFileSync, writeFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const _require = createRequire(import.meta.url);
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const ZKLib: any = _require("node-zklib");

export type ZKDeviceInfo = {
  userCounts: number;
  logCounts: number;
  logCapacity: number;
};

export type ZKUser = {
  uid: number;
  role: number;
  password: string;
  name: string;
  userId: string;
  cardno: number;
};

// node-zklib decodeRecordData40 returns exactly these three fields + ip
export type ZKAttendance = {
  userSn: number;       // user serial number
  deviceUserId: string; // enrollment ID string (matches attendance_device_id in ERPNext)
  recordTime: Date;     // punch timestamp
  ip: string;
};

export async function zkConnect(host: string, port: number, timeoutMs: number): Promise<unknown> {
  const zk = new ZKLib(host, port, timeoutMs, 5200);
  await zk.createSocket();
  return zk;
}

export async function zkGetInfo(zk: unknown): Promise<ZKDeviceInfo> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return (zk as any).getInfo() as Promise<ZKDeviceInfo>;
}

export async function zkGetUsers(zk: unknown): Promise<ZKUser[]> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const result = await (zk as any).getUsers();
  return (result?.data ?? []) as ZKUser[];
}

export async function zkGetAttendances(zk: unknown): Promise<ZKAttendance[]> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const result = await (zk as any).getAttendances();
  return (result?.data ?? []) as ZKAttendance[];
}

export async function zkDisconnect(zk: unknown): Promise<void> {
  try {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    await (zk as any).disconnect();
  } catch {
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (zk as any).socket?.destroy();
    } catch { /* ignore */ }
  }
}

// ---------------------------------------------------------------------------
// Per-device-per-company sync watermark (stored as a JSON file next to dist/)
// ---------------------------------------------------------------------------

const _dir = dirname(fileURLToPath(import.meta.url));
const WATERMARK_PATH = join(_dir, "..", ".zkteco-watermark.json");

function loadWatermarks(): Record<string, string> {
  try {
    if (existsSync(WATERMARK_PATH)) {
      return JSON.parse(readFileSync(WATERMARK_PATH, "utf8")) as Record<string, string>;
    }
  } catch { /* ignore */ }
  return {};
}

function saveWatermarks(store: Record<string, string>): void {
  try {
    writeFileSync(WATERMARK_PATH, JSON.stringify(store, null, 2), "utf8");
  } catch { /* ignore */ }
}

function watermarkKey(host: string, port: number, company: string): string {
  return `${host}:${port}|${company}`;
}

export function getWatermark(host: string, port: number, company: string): Date | null {
  const v = loadWatermarks()[watermarkKey(host, port, company)];
  if (!v) return null;
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? null : d;
}

export function setWatermark(host: string, port: number, company: string, ts: Date): void {
  const store = loadWatermarks();
  store[watermarkKey(host, port, company)] = ts.toISOString();
  saveWatermarks(store);
}
