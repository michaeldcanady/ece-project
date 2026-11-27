import { test } from "node:test";
import assert from "node:assert/strict";
import { nextIssue, orderQueue, type QueuedIssue } from "./queue";

const issue = (number: number, createdAt: string, ...labels: string[]): QueuedIssue => ({
  number,
  createdAt,
  labels: labels.map((name) => ({ name })),
});

const numbers = (issues: QueuedIssue[]): number[] => orderQueue(issues).map((i) => i.number);

test("nextIssue is undefined for an empty queue", () => {
  assert.equal(nextIssue([]), undefined);
});

test("the oldest issue goes first when nothing else differs", () => {
  assert.deepEqual(numbers([issue(2, "2026-02-01T00:00:00Z"), issue(1, "2026-03-01T00:00:00Z")]), [2, 1]);
});

test("a human request goes before an older scan candidate", () => {
  assert.deepEqual(numbers([issue(1, "2026-01-01T00:00:00Z"), issue(2, "2026-02-01T00:00:00Z", "request")]), [2, 1]);
});

test("priority:high goes before a request", () => {
  const queue = [
    issue(1, "2026-01-01T00:00:00Z", "request"),
    issue(2, "2026-03-01T00:00:00Z", "priority:high"),
    issue(3, "2026-02-01T00:00:00Z"),
  ];
  assert.deepEqual(numbers(queue), [2, 1, 3]);
});

test("a request with priority:high beats a high-priority scan candidate", () => {
  const queue = [
    issue(1, "2026-01-01T00:00:00Z", "priority:high"),
    issue(2, "2026-02-01T00:00:00Z", "priority:high", "request"),
  ];
  assert.deepEqual(numbers(queue), [2, 1]);
});

test("the issue number breaks a tie, and the input is not reordered", () => {
  const queue = [issue(5, "2026-01-01T00:00:00Z"), issue(4, "2026-01-01T00:00:00Z")];
  assert.deepEqual(numbers(queue), [4, 5]);
  assert.deepEqual(queue.map((i) => i.number), [5, 4]);
});
