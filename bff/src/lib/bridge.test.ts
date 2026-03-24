import assert from "node:assert/strict";
import crypto from "node:crypto";
import test from "node:test";
import { verifyBridgeAuth, type BridgePayload } from "./bridge.js";

function signTestToken(secret: string, payload: BridgePayload): string {
  const payloadB64 = Buffer.from(JSON.stringify(payload)).toString("base64url");
  const sig = crypto.createHmac("sha256", secret).update(payloadB64).digest("hex");
  return `${payloadB64}.${sig}`;
}

test("bridge verifies payload with appRole", () => {
  const secret = "test-secret-at-least-32-chars-long!!";
  const exp = Math.floor(Date.now() / 1000) + 120;
  const token = signTestToken(secret, {
    email: "mgr@example.com",
    company: "Centy Demo",
    canHr: false,
    exp,
    appRole: "approver",
  });
  const v = verifyBridgeAuth(token, secret);
  assert.ok(v);
  assert.equal(v!.appRole, "approver");
  assert.equal(v!.canHr, false);
});

test("bridge verifies legacy token without appRole", () => {
  const secret = "another-test-secret-key-32chars-min";
  const exp = Math.floor(Date.now() / 1000) + 120;
  const token = signTestToken(secret, {
    email: "hr@example.com",
    company: "Centy Demo",
    canHr: true,
    exp,
  });
  const v = verifyBridgeAuth(token, secret);
  assert.ok(v);
  assert.equal(v!.appRole, undefined);
});

test("bridge rejects tampered appRole", () => {
  const secret = "tamper-test-secret-key-32chars-min!";
  const exp = Math.floor(Date.now() / 1000) + 120;
  let payload: BridgePayload = {
    email: "a@b.com",
    company: "Co",
    canHr: false,
    exp,
    appRole: "approver",
  };
  const token = signTestToken(secret, payload);
  const parts = token.split(".");
  const tampered = JSON.parse(Buffer.from(parts[0]!, "base64url").toString()) as BridgePayload;
  tampered.appRole = "super_admin";
  const badB64 = Buffer.from(JSON.stringify(tampered)).toString("base64url");
  const badToken = `${badB64}.${parts[1]}`;
  assert.equal(verifyBridgeAuth(badToken, secret), null);
});
