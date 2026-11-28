import { z } from "zod";

/** Appended to every comment the reviewer writes, so the next run recognises its own threads (the author is a shared bot). */
export const REVIEW_MARKER = "<!-- sandcastle-pr-review -->";

/** The login GitHub reports for the Actions bot, with or without the `[bot]` suffix the REST API adds. */
export const BOT_LOGIN = "github-actions";

/** Most comments a review posts; the planner notes the rest in the body. */
export const MAX_COMMENTS = 25;

export const changedFileSchema = z.object({
  path: z.string(),
  /** The unified diff of the file; "" for a binary or too-large file. */
  patch: z.string(),
});

export const priorThreadSchema = z.object({
  /** GraphQL node id, for resolveReviewThread. */
  threadId: z.string(),
  /** REST id of the first comment, for the replies endpoint. */
  firstCommentId: z.number().int(),
  path: z.string(),
  line: z.number().int().nullable(),
  comments: z.array(z.object({ author: z.string(), body: z.string() })),
});

/** The PR snapshot: what fetch.ts read from GitHub, parsed once at the boundary. */
export const reviewContextSchema = z.object({
  pr: z.number().int(),
  baseSha: z.string(),
  headSha: z.string(),
  files: z.array(changedFileSchema),
  /** Unresolved threads whose first comment carries REVIEW_MARKER. */
  openThreads: z.array(priorThreadSchema),
  /** Head shas already named in a marked review body. */
  reviewedShas: z.array(z.string()),
  /** Diff digests (see diffDigest) named in a marked review body. */
  reviewedDiffs: z.array(z.string()),
});

export type ReviewContext = z.infer<typeof reviewContextSchema>;
export type PriorThread = z.infer<typeof priorThreadSchema>;
