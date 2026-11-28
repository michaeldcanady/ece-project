import { createHash } from "node:crypto";
import { doorLabels } from "./door";
import { BOT_LOGIN, MAX_COMMENTS, REVIEW_MARKER, type PriorThread, type ReviewContext } from "./review-context";
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
  /** The door label to add to the PR, and the opposite one to remove. */
  readonly door: { readonly add: string; readonly remove: string };
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

/**
 * A digest of what the PR changes, so a push that leaves the diff as it was (a merge of main, a rebase, an amended
 * message) is recognised without a model run. Null when a file has no patch (binary or too large): its content is
 * unknown, so the digest cannot vouch for it.
 */
export const diffDigest = (files: ReviewContext["files"]): string | null => {
  if (files.some((f) => f.patch === "")) return null;
  const canonical = [...files].sort((a, b) => a.path.localeCompare(b.path)).map((f) => [f.path, f.patch]);
  return createHash("sha256").update(JSON.stringify(canonical)).digest("hex").slice(0, 16);
};

export const alreadyReviewed = (context: ReviewContext): boolean => {
  if (context.reviewedShas.includes(context.headSha)) return true;
  const digest = diffDigest(context.files);
  return digest !== null && context.reviewedDiffs.includes(digest);
};

const isBot = (login: string): boolean => login.replace(/\[bot\]$/, "") === BOT_LOGIN;

/** A review thread as GitHub reports it, before the planner's ownership rules are applied. */
export interface RawThread {
  readonly id: string;
  readonly isResolved: boolean;
  readonly path: string;
  readonly line: number | null;
  readonly comments: readonly {
    readonly databaseId: number;
    readonly author: string;
    readonly authorAssociation: string;
    readonly body: string;
  }[];
}

const TRUSTED_ASSOCIATIONS: ReadonlySet<string> = new Set(["OWNER", "MEMBER", "COLLABORATOR"]);

/**
 * The threads the reviewer owns: unresolved, opened by the bot with REVIEW_MARKER (so a pasted marker in a human's
 * comment does not count). Their comments are what reaches the model, so only the bot, the PR author and people with
 * write-level association are kept; anyone else's reply is left out.
 */
export const ownedOpenThreads = (threads: readonly RawThread[], prAuthor: string): PriorThread[] =>
  threads.flatMap((t) => {
    const first = t.comments[0];
    if (t.isResolved || first === undefined || !isBot(first.author) || !first.body.includes(REVIEW_MARKER)) return [];
    const trusted = t.comments.filter(
      (c) => isBot(c.author) || c.author === prAuthor || TRUSTED_ASSOCIATIONS.has(c.authorAssociation),
    );
    return [
      {
        threadId: t.id,
        firstCommentId: first.databaseId,
        path: t.path,
        line: t.line,
        comments: trusted.map((c) => ({ author: c.author, body: c.body })),
      },
    ];
  });

/** True when the bot's marked reply already ends the thread (its first comment alone is not a reply). */
export const botRepliedLast = (comments: readonly { readonly author: string; readonly body: string }[]): boolean => {
  const last = comments.length > 1 ? comments.at(-1) : undefined;
  return last !== undefined && isBot(last.author) && last.body.includes(REVIEW_MARKER);
};

/** The head shas named in reviews the bot itself posted (`sha=<sha>` beside REVIEW_MARKER). */
export const reviewedShasOf = (reviews: readonly { readonly author: string; readonly body: string }[]): string[] =>
  reviews.flatMap((r) => {
    const sha = isBot(r.author) && r.body.includes(REVIEW_MARKER) ? /^sha=([0-9a-f]+)$/m.exec(r.body)?.[1] : undefined;
    return sha === undefined ? [] : [sha];
  });

/** The diff digests named in reviews the bot itself posted (`diff=<digest>` beside REVIEW_MARKER). */
export const reviewedDiffsOf = (reviews: readonly { readonly author: string; readonly body: string }[]): string[] =>
  reviews.flatMap((r) => {
    const digest = isBot(r.author) && r.body.includes(REVIEW_MARKER) ? /^diff=([0-9a-f]+)$/m.exec(r.body)?.[1] : undefined;
    return digest === undefined ? [] : [digest];
  });

const marked = (text: string): string => `${text}\n\n${REVIEW_MARKER}`;

export const planActions = (review: Review, context: ReviewContext): ReviewActions => {
  const commentable = new Map(context.files.map((f) => [f.path, commentableLines(f.patch)]));
  const inline: InlineComment[] = [];
  const unanchored: string[] = [];
  for (const c of review.comments.slice(0, MAX_COMMENTS)) {
    if (commentable.get(c.path)?.has(c.line) === true) {
      inline.push({ path: c.path, line: c.line, body: marked(c.body) });
    } else {
      unanchored.push(`- \`${c.path}:${c.line}\`: ${c.body}`);
    }
  }

  const omitted = review.comments.length - MAX_COMMENTS;
  if (omitted > 0) unanchored.push(`- ${omitted} further comments were left out to keep the review short.`);

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
  const digest = diffDigest(context.files);
  parts.push(`${REVIEW_MARKER}\nsha=${context.headSha}${digest === null ? "" : `\ndiff=${digest}`}`);
  return { commitId: context.headSha, body: parts.join("\n\n"), inline, resolutions, door: doorLabels(review.door) };
};
