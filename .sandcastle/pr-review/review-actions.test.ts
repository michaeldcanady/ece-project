import { test } from "node:test";
import assert from "node:assert/strict";
import {
  alreadyReviewed,
  botRepliedLast,
  commentableLines,
  diffDigest,
  ownedOpenThreads,
  planActions,
  reviewedDiffsOf,
  reviewedShasOf,
  type RawThread,
} from "./review-actions";
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
  reviewedDiffs: [],
  ...over,
});

const review = (over: Partial<Review> = {}): Review => ({ summary: "Looks fine.", door: "two-way", comments: [], resolved: [], ...over });

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

test("more than 25 comments are cut to 25 and the rest are noted in the body", () => {
  const comments = Array.from({ length: 27 }, (_, i) => ({ path: "a.ts", line: 4, body: `c${i}` }));
  const actions = planActions(review({ comments }), context());
  assert.equal(actions.inline.length, 25);
  assert.match(actions.body, /2 further comments/);
});

const raw = (over: Partial<RawThread> = {}): RawThread => ({
  id: "T",
  isResolved: false,
  path: "a.ts",
  line: 1,
  comments: [{ databaseId: 1, author: "github-actions", authorAssociation: "NONE", body: `x ${REVIEW_MARKER}` }],
  ...over,
});

test("ownedOpenThreads keeps only unresolved threads the bot opened with the marker", () => {
  const human = { databaseId: 2, author: "mallory", authorAssociation: "NONE", body: `pasted ${REVIEW_MARKER}` };
  const kept = ownedOpenThreads(
    [raw(), raw({ id: "R", isResolved: true }), raw({ id: "H", comments: [human] }), raw({ id: "U", comments: [{ ...human, author: "github-actions", body: "no marker" }] })],
    "alice",
  );
  assert.deepEqual(kept.map((t) => t.threadId), ["T"]);
});

test("ownedOpenThreads leaves out replies from strangers but keeps the bot, the PR author and collaborators", () => {
  const reply = (author: string, authorAssociation: string) => ({ databaseId: 9, author, authorAssociation, body: author });
  const [thread] = ownedOpenThreads(
    [raw({ comments: [...raw().comments, reply("alice", "NONE"), reply("bob", "COLLABORATOR"), reply("mallory", "NONE")] })],
    "alice",
  );
  assert.deepEqual(thread?.comments.map((c) => c.author), ["github-actions", "alice", "bob"]);
});

test("reviewedShasOf reads sha only from marked reviews by the bot", () => {
  const body = `ok\n\n${REVIEW_MARKER}\nsha=abc123`;
  assert.deepEqual(
    reviewedShasOf([
      { author: "github-actions[bot]", body },
      { author: "mallory", body: `${REVIEW_MARKER}\nsha=dead` },
      { author: "github-actions[bot]", body: "sha=beef" },
    ]),
    ["abc123"],
  );
});

test("botRepliedLast is true only when a marked bot reply follows the first comment", () => {
  const bot = { author: "github-actions[bot]", body: `done ${REVIEW_MARKER}` };
  const human = { author: "alice", body: "why?" };
  assert.equal(botRepliedLast([bot]), false);
  assert.equal(botRepliedLast([bot, bot]), true);
  assert.equal(botRepliedLast([bot, bot, human]), false);
  assert.equal(botRepliedLast([bot, { author: "github-actions[bot]", body: "unmarked" }]), false);
});

test("the door becomes a label to add and the opposite one to remove, and never enters the body", () => {
  const one = planActions(review({ door: "one-way" }), context());
  assert.deepEqual(one.door, { add: "door:one-way", remove: "door:two-way" });
  assert.doesNotMatch(one.body, /door/i);
  assert.deepEqual(planActions(review(), context()).door, { add: "door:two-way", remove: "door:one-way" });
});

test("a push that leaves the diff unchanged is already reviewed, even under a new head sha", () => {
  const digest = diffDigest(context().files);
  assert.notEqual(digest, null);
  assert.equal(alreadyReviewed(context({ headSha: "merge-of-main", reviewedDiffs: [String(digest)] })), true);
  assert.equal(alreadyReviewed(context({ headSha: "merge-of-main", reviewedDiffs: ["0000"] })), false);
});

test("a changed patch changes the digest, and file order does not", () => {
  const a = { path: "a.ts", patch: PATCH };
  const b = { path: "b.ts", patch: PATCH };
  assert.equal(diffDigest([a, b]), diffDigest([b, a]));
  assert.notEqual(diffDigest([a]), diffDigest([{ ...a, patch: `${PATCH}\n+extra` }]));
});

test("a file with no patch gives no digest, so it is never skipped by diff", () => {
  assert.equal(diffDigest([{ path: "img.png", patch: "" }]), null);
  assert.equal(alreadyReviewed(context({ files: [{ path: "img.png", patch: "" }], reviewedDiffs: ["abc"] })), false);
});

test("the body carries the diff digest and reviewedDiffsOf reads it only from marked bot reviews", () => {
  const body = planActions(review(), context()).body;
  const digest = diffDigest(context().files);
  assert.ok(body.includes(`diff=${digest}`));
  assert.deepEqual(
    reviewedDiffsOf([
      { author: "github-actions[bot]", body },
      { author: "mallory", body: `${REVIEW_MARKER}\ndiff=dead` },
      { author: "github-actions[bot]", body: "diff=beef" },
    ]),
    [String(digest)],
  );
});
