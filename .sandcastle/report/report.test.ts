import { test } from "node:test";
import assert from "node:assert/strict";
import { buildReport, factoryIssue, recovery, type Issue, type Pr } from "./report";

const now = new Date("2026-11-30T07:00:00Z");
const since = new Date("2026-11-23T07:00:00Z");
const recent = "2026-11-28T00:00:00Z";
const old = "2026-11-01T00:00:00Z";

const issue = (number: number, labels: string[], over: Partial<Issue> = {}): Issue => ({
  number,
  state: "OPEN",
  labels,
  createdAt: recent,
  updatedAt: recent,
  ...over,
});
const pr = (number: number, branch: string, state: Pr["state"], labels: string[] = [], createdAt = recent): Pr => ({
  number,
  state,
  headRefName: branch,
  labels,
  createdAt,
});

test("factoryIssue reads the issue number from an agent branch only", () => {
  assert.equal(factoryIssue(pr(1, "agent/issue-42-fix", "OPEN")), 42);
  assert.equal(factoryIssue(pr(1, "feature/issue-42", "OPEN")), undefined);
});

test("counts human and scan issues filed in the window, and ignores older ones", () => {
  const report = buildReport(
    [
      issue(1, ["request"]),
      issue(2, ["request", "agent:candidate"]),
      issue(3, ["agent:candidate"]),
      issue(4, ["agent:candidate"], { createdAt: old }),
    ],
    [],
    now,
  );
  assert.match(report, /By humans \(label `request`\): 2/);
  assert.match(report, /By the nightly scan .*: 1/);
});

test("splits Factory PRs by outcome and skips other branches and old PRs", () => {
  const report = buildReport(
    [],
    [
      pr(10, "agent/issue-1", "MERGED", ["door:two-way"]),
      pr(11, "agent/issue-2", "CLOSED"),
      pr(12, "agent/issue-3", "OPEN", ["door:one-way"]),
      pr(13, "human/work", "MERGED"),
      pr(14, "agent/issue-4", "MERGED", [], old),
    ],
    now,
  );
  assert.match(report, /Opened \(branch `agent\/issue-\*`\): 3/);
  assert.match(report, /Merged: 1/);
  assert.match(report, /Closed unmerged: 1/);
  assert.match(report, /Still open: 1/);
  assert.match(report, /`door:one-way`: 1 of 3 \(33%\)/);
});

test("lists open blocked issues and not closed ones", () => {
  const report = buildReport(
    [issue(5, ["agent:blocked"]), issue(6, ["agent:blocked"], { state: "CLOSED" })],
    [],
    now,
  );
  assert.match(report, /`agent:blocked`: 1 \(#5\)/);
});

test("recovery counts a merged or open PR as recovered and a PR closed unmerged as other", () => {
  const issues = [
    issue(1, ["agent:recovered"]),
    issue(2, ["agent:recovered", "agent:blocked"]),
    issue(3, ["agent:recovered"]),
    issue(4, ["agent:recovered"], { updatedAt: old }),
  ];
  const prs = [pr(10, "agent/issue-1-retry", "MERGED"), pr(11, "agent/issue-3", "CLOSED")];
  assert.deepEqual(recovery(issues, prs, since), { withPr: 1, blocked: 1, other: 1 });
});

test("an empty week reports zeros and n/a, not NaN", () => {
  const report = buildReport([], [], now);
  assert.match(report, /Recovery rate: n\/a/);
  assert.match(report, /\(n\/a\)/);
  assert.doesNotMatch(report, /NaN/);
});
