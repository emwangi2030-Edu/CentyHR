/**
 * Pay Hub merges this with its own defaults — keep shapes aligned with
 * Pay Hub `docs/HR_CAPABILITIES_STANDARD.md`.
 */

export const CAPABILITIES_VERSION = 1 as const;

export type HrCapabilitiesPayload = {
  version: typeof CAPABILITIES_VERSION;
  people: {
    directory: boolean;
    createEmployee: boolean;
    sendInvite: boolean;
    editEmployee: boolean;
    exitEmployee: boolean;
    orgOptions: boolean;
    lifecycle: boolean;
  };
  assets: {
    registry: boolean;
    assign: boolean;
    requisitions: "none" | "view" | "approve";
    disposals: "hidden" | "view" | "create";
  };
  leaves: {
    applications: boolean;
    submit: boolean;
    approve: boolean;
    balances: boolean;
  };
  attendance: {
    shifts: boolean;
    checkins: boolean;
    daily: boolean;
    teamView: boolean;
  };
  expenses: {
    claims: boolean;
    submit: boolean;
    approve: boolean;
    advances: boolean;
  };
};

/** Optimistic defaults: operators can narrow via HR_CAPABILITIES_JSON. */
export const DEFAULT_BFF_CAPABILITIES: HrCapabilitiesPayload = {
  version: CAPABILITIES_VERSION,
  people: {
    directory: true,
    createEmployee: true,
    sendInvite: true,
    editEmployee: true,
    exitEmployee: true,
    orgOptions: true,
    lifecycle: true,
  },
  assets: {
    registry: true,
    assign: true,
    requisitions: "approve",
    disposals: "create",
  },
  leaves: {
    applications: true,
    submit: true,
    approve: true,
    balances: true,
  },
  attendance: {
    shifts: true,
    checkins: true,
    daily: true,
    teamView: true,
  },
  expenses: {
    claims: true,
    submit: true,
    approve: true,
    advances: true,
  },
};

function isRecord(v: unknown): v is Record<string, unknown> {
  return !!v && typeof v === "object" && !Array.isArray(v);
}

function pickReq(v: unknown): HrCapabilitiesPayload["assets"]["requisitions"] {
  const s = String(v ?? "").toLowerCase();
  if (s === "none" || s === "view" || s === "approve") return s;
  return DEFAULT_BFF_CAPABILITIES.assets.requisitions;
}

function pickDisp(v: unknown): HrCapabilitiesPayload["assets"]["disposals"] {
  const s = String(v ?? "").toLowerCase();
  if (s === "hidden" || s === "view" || s === "create") return s;
  return DEFAULT_BFF_CAPABILITIES.assets.disposals;
}

function deepMergeBoolBlock<T extends Record<string, boolean>>(base: T, patch: Record<string, unknown>): T {
  const out = { ...base } as T;
  for (const k of Object.keys(base)) {
    if (typeof patch[k] === "boolean") (out as Record<string, boolean>)[k] = patch[k] as boolean;
  }
  return out;
}

function deepMergeAssets(
  base: HrCapabilitiesPayload["assets"],
  a: Record<string, unknown>
): HrCapabilitiesPayload["assets"] {
  const out = { ...base };
  if (typeof a.registry === "boolean") out.registry = a.registry;
  if (typeof a.assign === "boolean") out.assign = a.assign;
  if (a.requisitions !== undefined) out.requisitions = pickReq(a.requisitions);
  if (a.disposals !== undefined) out.disposals = pickDisp(a.disposals);
  return out;
}

/** Merge partial patch (from HR_CAPABILITIES_JSON) into defaults. */
export function mergeCapabilitiesPatch(base: HrCapabilitiesPayload, patch: unknown): HrCapabilitiesPayload {
  if (!isRecord(patch)) return base;
  const root = isRecord(patch.data) ? patch.data : patch;
  if (!isRecord(root)) return base;
  let out: HrCapabilitiesPayload = {
    ...base,
    people: { ...base.people },
    assets: { ...base.assets },
    leaves: { ...base.leaves },
    attendance: { ...base.attendance },
    expenses: { ...base.expenses },
  };
  if (isRecord(root.people)) out = { ...out, people: deepMergeBoolBlock(out.people, root.people) };
  if (isRecord(root.assets)) out = { ...out, assets: deepMergeAssets(out.assets, root.assets) };
  if (isRecord(root.leaves)) out = { ...out, leaves: deepMergeBoolBlock(out.leaves, root.leaves) };
  if (isRecord(root.attendance)) out = { ...out, attendance: deepMergeBoolBlock(out.attendance, root.attendance) };
  if (isRecord(root.expenses)) out = { ...out, expenses: deepMergeBoolBlock(out.expenses, root.expenses) };
  return out;
}

export function parseEnvCapabilitiesJson(raw: string): unknown {
  const t = raw.trim();
  if (!t) return null;
  try {
    return JSON.parse(t) as unknown;
  } catch {
    return null;
  }
}

export function buildCapabilitiesForResponse(envJson: string): HrCapabilitiesPayload {
  const patch = parseEnvCapabilitiesJson(envJson);
  let cap: HrCapabilitiesPayload = {
    ...DEFAULT_BFF_CAPABILITIES,
    people: { ...DEFAULT_BFF_CAPABILITIES.people },
    assets: { ...DEFAULT_BFF_CAPABILITIES.assets },
    leaves: { ...DEFAULT_BFF_CAPABILITIES.leaves },
    attendance: { ...DEFAULT_BFF_CAPABILITIES.attendance },
    expenses: { ...DEFAULT_BFF_CAPABILITIES.expenses },
  };
  if (patch) cap = mergeCapabilitiesPatch(cap, patch);
  return cap;
}

