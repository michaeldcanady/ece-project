import { test } from "node:test";
import assert from "node:assert/strict";
import { STRENGTHS } from "../shared/candidate-labels";
import { autoImplement } from "./spec-labels";

test("autoImplement is true only for a strong request", () => {
  assert.equal(autoImplement(["request"], "strong", "OWNER"), true);
  for (const s of STRENGTHS.filter((s) => s !== "strong")) assert.equal(autoImplement(["request"], s, "OWNER"), false);
});

test("autoImplement is false for a strong issue that is not a request, or is already queued", () => {
  assert.equal(autoImplement(["agent:candidate"], "strong", "OWNER"), false);
  assert.equal(autoImplement(["request", "agent:implement"], "strong", "OWNER"), false);
});

test("autoImplement is false for a strong request from someone outside the repository", () => {
  for (const who of ["NONE", "CONTRIBUTOR", "FIRST_TIMER"]) assert.equal(autoImplement(["request"], "strong", who), false);
});
