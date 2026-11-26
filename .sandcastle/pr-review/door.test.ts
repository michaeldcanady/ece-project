import { test } from "node:test";
import assert from "node:assert/strict";
import { DOORS, doorSchema, renderDoor } from "./door";
import { reviewSchema } from "./review-plan";

test("doorSchema accepts each value of DOORS", () => {
  for (const kind of DOORS) assert.equal(doorSchema.safeParse({ kind, reason: "Because." }).success, true);
});

test("doorSchema rejects an unknown kind and an empty reason", () => {
  assert.equal(doorSchema.safeParse({ kind: "revolving", reason: "x" }).success, false);
  assert.equal(doorSchema.safeParse({ kind: "one-way", reason: "" }).success, false);
});

test("reviewSchema fails on a review with no door", () => {
  assert.equal(reviewSchema.safeParse({ summary: "ok", comments: [], resolved: [] }).success, false);
});

test("renderDoor is a function of its input only", () => {
  const door = { kind: "one-way", reason: "Changes the Kafka key." } as const;
  assert.equal(renderDoor(door), "**Door: one-way.** Changes the Kafka key.");
  assert.equal(renderDoor(door), renderDoor({ ...door }));
});
