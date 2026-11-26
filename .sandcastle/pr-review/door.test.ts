import { test } from "node:test";
import assert from "node:assert/strict";
import { DOORS, doorLabels, doorSchema, ensureDoorLabels } from "./door";
import { reviewSchema } from "./review-plan";

test("doorSchema accepts each value of DOORS and rejects anything else", () => {
  for (const door of DOORS) assert.equal(doorSchema.safeParse(door).success, true);
  assert.equal(doorSchema.safeParse("revolving").success, false);
});

test("reviewSchema fails on a review with no door", () => {
  assert.equal(reviewSchema.safeParse({ summary: "ok", comments: [], resolved: [] }).success, false);
});

test("doorLabels adds the label for the door and removes the other one", () => {
  assert.deepEqual(doorLabels("one-way"), { add: "door:one-way", remove: "door:two-way" });
  assert.deepEqual(doorLabels("two-way"), { add: "door:two-way", remove: "door:one-way" });
});

test("ensureDoorLabels creates only the door labels the repo lacks", () => {
  const calls: string[][] = [];
  ensureDoorLabels((args) => {
    calls.push(args);
    return args[1] === "list" ? "bug\ndoor:two-way\n" : "";
  });
  assert.deepEqual(calls.filter((c) => c[1] === "create"), [["label", "create", "door:one-way"]]);
});
