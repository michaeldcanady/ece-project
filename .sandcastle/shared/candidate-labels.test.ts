import { test } from "node:test";
import assert from "node:assert/strict";
import { candidateLabelChange, specEditArgs, STRENGTHS, strengthLabel } from "./candidate-labels";

test("an unlabelled issue gains agent:candidate and its strength", () => {
  assert.deepEqual(candidateLabelChange("explore", []), {
    add: ["agent:candidate", "strength:explore"],
    remove: [],
  });
});

test("a stale strength is removed; unrelated labels are not its concern", () => {
  const change = candidateLabelChange("explore", ["strength:strong", "agent:spec"]);
  assert.deepEqual(change.remove, ["strength:strong"]);
});

test("an issue already right needs nothing removed", () => {
  const change = candidateLabelChange("explore", ["agent:candidate", "strength:explore"]);
  assert.deepEqual(change.remove, []);
});

test("every strength maps to strength:<value>", () => {
  for (const s of STRENGTHS) assert.equal(strengthLabel(s), `strength:${s}`);
});

test("a spec edit sets the body, drops agent:spec and a stale strength, and adds the candidate labels", () => {
  assert.deepEqual(specEditArgs("7", "b.md", "explore", ["agent:spec", "strength:strong"]), [
    "issue", "edit", "7", "--body-file", "b.md",
    "--remove-label", "agent:spec",
    "--add-label", "agent:candidate", "--add-label", "strength:explore",
    "--remove-label", "strength:strong",
  ]);
});
