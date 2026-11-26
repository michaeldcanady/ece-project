import { REVIEW_MARKER, type ReviewContext } from "./review-context";
import type { Review } from "./review-plan";

/**
 * The review planner. Pure: no I/O. Given the PR snapshot and what the agent said, it decides exactly which GitHub
 * writes are allowed, so neither the prompt nor apply.ts has to.
 */

export interface InlineComment {
  readonly path: string;
  readonly line: number;
  readonly body: string;
}

export interface Resolution {
  readonly threadId: string;
  readonly firstCommentId: number;
  readonly reply: string;
}

export interface ReviewActions {
  /** The head sha the review is made against. */
  readonly commitId: string;
  /** Summary, comments that could not be anchored, REVIEW_MARKER and `sha=<headSha>`. */
  readonly body: string;
  /** Every one lands on a commentable RIGHT-side line. */
  readonly inline: readonly InlineComment[];
  /** Only threads in context.openThreads. */
  readonly resolutions: readonly Resolution[];
}

const HUNK = /^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@/;

/** The new-side line numbers a review comment can attach to: the added and context lines of each hunk. */
export const commentableLines = (patch: string): ReadonlySet<number> => {
  const lines = new Set<number>();
  let next = 0; // next new-side line number
  let remaining = 0; // new-side lines left in the current hunk
  for (const text of patch.split("\n")) {
    const header = HUNK.exec(text);
    if (header !== null) {
      next = Number(header[1]);
      remaining = header[2] === undefined ? 1 : Number(header[2]);
      continue;
    }
    if (remaining === 0 || text.startsWith("-") || text.startsWith("\\")) continue;
    lines.add(next);
    next += 1;
    remaining -= 1;
  }
  return lines;
};

export const alreadyReviewed = (context: ReviewContext): boolean => context.reviewedShas.includes(context.headSha);

const marked = (text: string): string => `${text}\n\n${REVIEW_MARKER}`;

export const planActions = (review: Review, context: ReviewContext): ReviewActions => {
  const commentable = new Map(context.files.map((f) => [f.path, commentableLines(f.patch)]));
  const inline: InlineComment[] = [];
  const unanchored: string[] = [];
  for (const c of review.comments) {
    if (commentable.get(c.path)?.has(c.line) === true) {
      inline.push({ path: c.path, line: c.line, body: marked(c.body) });
    } else {
      unanchored.push(`- \`${c.path}:${c.line}\`: ${c.body}`);
    }
  }

  const threads = new Map(context.openThreads.map((t) => [t.threadId, t]));
  const resolutions: Resolution[] = [];
  for (const r of review.resolved) {
    const thread = threads.get(r.threadId);
    if (thread === undefined) continue;
    threads.delete(r.threadId); // resolve each thread once
    resolutions.push({ threadId: thread.threadId, firstCommentId: thread.firstCommentId, reply: marked(r.reply) });
  }

  const parts = [review.summary];
  if (unanchored.length > 0) parts.push(unanchored.join("\n"));
  parts.push(`${REVIEW_MARKER}\nsha=${context.headSha}`);
  return { commitId: context.headSha, body: parts.join("\n\n"), inline, resolutions };
};
