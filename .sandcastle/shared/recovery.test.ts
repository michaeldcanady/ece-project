import { test } from "node:test";
import assert from "node:assert/strict";
import { failureComment, recoveryAction, redact } from "./recovery";

test("recoveryAction retries an issue that has not been recovered and gives up on one that has", () => {
  assert.equal(recoveryAction(["agent:implement"]), "retry");
  assert.equal(recoveryAction(["agent:implement", "agent:recovered"]), "give-up");
});

test("redact removes every occurrence of each secret and ignores an empty one", () => {
  assert.equal(redact("x-access-token:abc@github abc", ["abc", ""]), "x-access-token:***@github ***");
});

test("failureComment says what happens next and fences the reason with a fence the reason cannot close", () => {
  assert.match(failureComment("retry", "boom"), /runs once more/);
  assert.match(failureComment("give-up", "boom"), /is blocked/);
  const comment = failureComment("retry", "has ``` inside");
  assert.ok(comment.includes("````\nhas ``` inside\n````"));
});
