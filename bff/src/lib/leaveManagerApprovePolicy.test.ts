import assert from "node:assert/strict";
import test from "node:test";
import { leaveManagerBlockedByDayCeiling, leaveManagerDayCeilingMessage } from "./leaveManagerApprovePolicy.js";

test("leave ceiling: HR bypass", () => {
  assert.equal(leaveManagerBlockedByDayCeiling(99, 5, true), false);
});

test("leave ceiling: no max configured", () => {
  assert.equal(leaveManagerBlockedByDayCeiling(99, null, false), false);
});

test("leave ceiling: manager within threshold", () => {
  assert.equal(leaveManagerBlockedByDayCeiling(5, 10, false), false);
});

test("leave ceiling: manager above threshold", () => {
  assert.equal(leaveManagerBlockedByDayCeiling(11, 10, false), true);
});

test("leave ceiling: non-numeric days does not block", () => {
  assert.equal(leaveManagerBlockedByDayCeiling("x", 10, false), false);
});

test("leave ceiling message references env", () => {
  assert.ok(leaveManagerDayCeilingMessage(7).includes("LEAVE_MANAGER_APPROVE_MAX_DAYS"));
});
