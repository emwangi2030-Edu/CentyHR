import crypto from "crypto";

/** Signed by Pay Hub; `appRole` is optional for older bridge tokens. */
export type BridgePayload = {
  email: string;
  company: string;
  canHr: boolean;
  exp: number;
  displayName?: string;
  /** Pay Hub `user.role` — stage-1 vs stage-2 UX and future BFF rules. */
  appRole?: string;
};

export function verifyBridgeAuth(header: string, secret: string): BridgePayload | null {
  const parts = header.split(".");
  if (parts.length !== 2) return null;
  const [payloadB64, sigHex] = parts;
  const expected = crypto.createHmac("sha256", secret).update(payloadB64).digest("hex");
  let a: Buffer;
  let b: Buffer;
  try {
    a = Buffer.from(sigHex, "hex");
    b = Buffer.from(expected, "hex");
  } catch {
    return null;
  }
  if (a.length !== b.length) return null;
  if (!crypto.timingSafeEqual(a, b)) return null;
  try {
    const payload = JSON.parse(Buffer.from(payloadB64, "base64url").toString()) as BridgePayload;
    if (payload.exp < Date.now() / 1000) return null;
    if (!payload.email || !payload.company) return null;
    return payload;
  } catch {
    return null;
  }
}
