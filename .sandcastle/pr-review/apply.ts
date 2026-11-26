import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { reviewContextSchema } from "./review-context";
import { reviewSchema } from "./review-plan";
import { botRepliedLast, planActions, reviewedShasOf } from "./review-actions";
import { gh, readReviews, readThreadComments } from "./github-reads";
import { required } from "../shared/common";

/**
 * The GitHub adapter on the way out, and the only step with write access. Both artifacts crossed a job boundary, so
 * they are parsed again. Replies to and resolves the threads the planner allows, then posts one COMMENT review (never
 * an approval or a request for changes). The review goes last because it marks the head as reviewed: if it exists,
 * everything before it succeeded, so a rerun cannot repeat or skip any write.
 */

const reviewFile = path.join(required("REVIEW_DIR"), "review.json");
if (!fs.existsSync(reviewFile)) {
  console.log("No review.json: this head was already reviewed.");
  process.exit(0);
}

const repo = required("GH_REPO");
const context = reviewContextSchema.parse(
  JSON.parse(fs.readFileSync(path.join(required("CONTEXT_DIR"), "context.json"), "utf8")),
);
const review = reviewSchema.parse(JSON.parse(fs.readFileSync(reviewFile, "utf8")));

const live = gh(["pr", "view", String(context.pr), "--json", "headRefOid", "--jq", ".headRefOid"]).trim();
if (live !== context.headSha) {
  console.log(`PR head is now ${live}, not ${context.headSha}; a newer run supersedes this one.`);
  process.exit(0);
}

// The snapshot predates this job, so a rerun of only this job must look at the reviews as they are now.
if (reviewedShasOf(readReviews(repo, context.pr)).includes(context.headSha)) {
  console.log(`Head ${context.headSha} already has a review; posting nothing.`);
  process.exit(0);
}

const actions = planActions(review, context);

const RESOLVE = `mutation($id: ID!) { resolveReviewThread(input: {threadId: $id}) { thread { id } } }`;
for (const r of actions.resolutions) {
  // Reply first, so a thread is never resolved without its explanation; look at the thread as it is now, so a retry
  // after a later failure does not reply twice. Resolving is harmless to repeat.
  if (!botRepliedLast(readThreadComments(r.threadId))) {
    gh(["api", `repos/${repo}/pulls/${context.pr}/comments/${r.firstCommentId}/replies`, "--method", "POST", "-f", `body=${r.reply}`]);
  }
  gh(["api", "graphql", "-f", `query=${RESOLVE}`, "-F", `id=${r.threadId}`]);
  console.log(`Resolved thread ${r.threadId}.`);
}

const payload = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "pr-review-")), "review.json");
fs.writeFileSync(
  payload,
  JSON.stringify({
    commit_id: actions.commitId,
    event: "COMMENT",
    body: actions.body,
    comments: actions.inline.map((c) => ({ path: c.path, line: c.line, side: "RIGHT", body: c.body })),
  }),
);
gh(["api", `repos/${repo}/pulls/${context.pr}/reviews`, "--method", "POST", "--input", payload]);
console.log(`Posted a review with ${actions.inline.length} inline comments.`);
