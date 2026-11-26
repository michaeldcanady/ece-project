import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { execFileSync } from "node:child_process";
import { reviewContextSchema } from "./review-context";
import { reviewSchema } from "./review-plan";
import { z } from "zod";
import { planActions, reviewedShasOf } from "./review-actions";
import { required } from "../shared/common";

/**
 * The GitHub adapter on the way out, and the only step with write access. Both artifacts crossed a job boundary, so
 * they are parsed again. Resolves and replies to the threads the planner allows, then posts one COMMENT review (never
 * an approval or a request for changes). The review goes last because it marks the head as reviewed: if it exists,
 * everything before it succeeded, so a rerun cannot repeat or skip any write.
 */

const gh = (args: string[]): string =>
  execFileSync("gh", args, { encoding: "utf8", stdio: ["ignore", "pipe", "inherit"] });

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
const reviewsSchema = z.array(z.array(z.object({ body: z.string().nullable(), user: z.object({ login: z.string() }).nullable() })));
const liveReviews = reviewsSchema
  .parse(JSON.parse(gh(["api", "--paginate", "--slurp", `repos/${repo}/pulls/${context.pr}/reviews`])))
  .flat();
if (reviewedShasOf(liveReviews.map((r) => ({ author: r.user?.login ?? "ghost", body: r.body ?? "" }))).includes(context.headSha)) {
  console.log(`Head ${context.headSha} already has a review; posting nothing.`);
  process.exit(0);
}

const actions = planActions(review, context);

const RESOLVE = `mutation($id: ID!) { resolveReviewThread(input: {threadId: $id}) { thread { id } } }`;
for (const r of actions.resolutions) {
  // Resolve first: a failure here leaves no reply behind for a rerun to repeat.
  gh(["api", "graphql", "-f", `query=${RESOLVE}`, "-F", `id=${r.threadId}`]);
  if (r.reply !== null) {
    gh(["api", `repos/${repo}/pulls/${context.pr}/comments/${r.firstCommentId}/replies`, "--method", "POST", "-f", `body=${r.reply}`]);
  }
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
