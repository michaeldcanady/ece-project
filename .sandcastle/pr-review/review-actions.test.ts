import { test } from "node:test";
import assert from "node:assert/strict";
import { alreadyReviewed, commentableLines, planActions } from "./review-actions";
import { REVIEW_MARKER, type ReviewContext } from "./review-context";
import type { Review } from "./review-plan";

// New side: hunk one covers lines 3-6 (3 context, 4 added, 5 context-after-removal, 6 context),
// hunk two covers lines 20-21. The removed line has no new-side number.
const PATCH = [
  "@@ -3,4 +3,4 @@ fn",
  " three",
  "+four",
  "-old",
  " five",
  " six",
  "@@ -19,2 +20,2 @@",
  " twenty",
  "+twentyone",
].join("\n");

const context = (over: Partial<ReviewContext> = {}): ReviewContext => ({
  pr: 7,
  baseSha: "base",
  headSha: "head1",
  files: [{ path: "a.ts", patch: PATCH }],
  openThreads: [{ threadId: "T1", firstCommentId: 11, path: "a.ts", line: 4, comments: [] }],
  reviewedShas: [],
  ...over,
});

const review = (over: Partial<Review> = {}): Review => ({ summary: "Looks fine.", comments: [], resolved: [], ...over });

test("commentableLines returns new-side added and context lines, not removed lines or the gap between hunks", () => {
  assert.deepEqual([...commentableLines(PATCH)], [3, 4, 5, 6, 20, 21]);
});

test("a comment on a commentable line stays inline with the marker appended", () => {
  const actions = planActions(review({ comments: [{ path: "a.ts", line: 4, body: "nit" }] }), context());
  assert.deepEqual(actions.inline, [{ path: "a.ts", line: 4, body: `nit\n\n${REVIEW_MARKER}` }]);
});

test("a comment off the diff, or on an unknown path, moves into the body as path:line", () => {
  const actions = planActions(
    review({
      comments: [
        { path: "a.ts", line: 10, body: "between hunks" },
        { path: "b.ts", line: 1, body: "not in the PR" },
      ],
    }),
    context(),
  );
  assert.deepEqual(actions.inline, []);
  assert.match(actions.body, /`a\.ts:10`: between hunks/);
  assert.match(actions.body, /`b\.ts:1`: not in the PR/);
});

test("resolving a thread outside openThreads is dropped; one inside carries its firstCommentId", () => {
  const actions = planActions(
    review({ resolved: [{ threadId: "human", reply: "x" }, { threadId: "T1", reply: "Fixed." }] }),
    context(),
  );
  assert.deepEqual(actions.resolutions, [{ threadId: "T1", firstCommentId: 11, reply: `Fixed.\n\n${REVIEW_MARKER}` }]);
});

test("the body carries the marker and the head sha, and alreadyReviewed sees that sha", () => {
  const actions = planActions(review(), context());
  assert.equal(actions.commitId, "head1");
  assert.ok(actions.body.includes(REVIEW_MARKER));
  assert.ok(actions.body.includes("sha=head1"));
  assert.equal(alreadyReviewed(context()), false);
  assert.equal(alreadyReviewed(context({ reviewedShas: ["head1"] })), true);
});

test("planActions is a function of its inputs only", () => {
  const r = review({ comments: [{ path: "a.ts", line: 21, body: "x" }], resolved: [{ threadId: "T1", reply: "ok" }] });
  assert.deepEqual(planActions(r, context()), planActions(r, context()));
});
