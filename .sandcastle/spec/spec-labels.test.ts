import { test } from "node:test";
import assert from "node:assert/strict";
import { STRENGTHS } from "../shared/candidate-labels";
import { autoImplement, failurePlan, staleMarkArgs } from "./spec-labels";

test("autoImplement is true only for a strong request", () => {
  assert.equal(autoImplement(["request"], "strong", "OWNER"), true);
  for (const s of STRENGTHS.filter((s) => s !== "strong")) assert.equal(autoImplement(["request"], s, "OWNER"), false);
});

test("autoImplement is false for a strong issue that is not a request, or is already queued", () => {
  assert.equal(autoImplement(["agent:candidate"], "strong", "OWNER"), false);
  assert.equal(autoImplement(["request", "agent:implement"], "strong", "OWNER"), false);
});

test("autoImplement is false for a strong request from someone outside the repository", () => {
  for (const who of ["NONE", "CONTRIBUTOR", "FIRST_TIMER"] as const) assert.equal(autoImplement(["request"], "strong", who), false);
});

test("a first failure retries: marks agent:recovered and takes agent:spec off so the PAT can re-add it", () => {
  const plan = failurePlan(["agent:spec", "agent:candidate"]);
  assert.equal(plan.action, "retry");
  assert.equal(plan.createLabel.name, "agent:recovered");
  assert.deepEqual(plan.edit, ["--add-label", "agent:recovered", "--remove-label", "agent:spec"]);
});

test("a failure on a recovered issue gives up: labels it agent:blocked and clears the spec and recovery marks", () => {
  const plan = failurePlan(["agent:spec", "agent:recovered"]);
  assert.equal(plan.action, "give-up");
  assert.equal(plan.createLabel.name, "agent:blocked");
  assert.deepEqual(plan.edit, ["--remove-label", "agent:spec", "--remove-label", "agent:recovered", "--add-label", "agent:blocked"]);
});

test("a success clears only the recovery and block marks the issue carries", () => {
  assert.deepEqual(staleMarkArgs(["agent:spec"]), []);
  assert.deepEqual(staleMarkArgs(["agent:recovered", "agent:blocked"]), ["--remove-label", "agent:recovered", "--remove-label", "agent:blocked"]);
});
