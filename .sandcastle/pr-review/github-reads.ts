import { execFileSync } from "node:child_process";
import { z } from "zod";

/** Reads from GitHub that both fetch.ts (the snapshot) and apply.ts (a live look before writing) need. */

export const gh = (args: string[]): string =>
  execFileSync("gh", args, { encoding: "utf8", stdio: ["ignore", "pipe", "inherit"], maxBuffer: 256 * 1024 * 1024 });

const reviewsSchema = z.array(
  z.array(z.object({ body: z.string().nullable(), user: z.object({ login: z.string() }).nullable() })),
);

export interface Authored {
  readonly author: string;
  readonly body: string;
}

/** Every review on the PR as {author, body}. */
export const readReviews = (repo: string, pr: number): Authored[] =>
  reviewsSchema
    .parse(JSON.parse(gh(["api", "--paginate", "--slurp", `repos/${repo}/pulls/${pr}/reviews`])))
    .flat()
    .map((r) => ({ author: r.user?.login ?? "ghost", body: r.body ?? "" }));

const threadSchema = z.object({
  data: z.object({
    node: z.object({
      comments: z.object({
        nodes: z.array(z.object({ author: z.object({ login: z.string() }).nullable(), body: z.string() })),
      }),
    }),
  }),
});

const THREAD_QUERY = `query($id: ID!) { node(id: $id) { ... on PullRequestReviewThread {
  comments(last: 50) { nodes { author { login } body } } } } }`;

/** The comments on a review thread as they are now (the last 50). */
export const readThreadComments = (threadId: string): Authored[] =>
  threadSchema
    .parse(JSON.parse(gh(["api", "graphql", "-f", `query=${THREAD_QUERY}`, "-f", `id=${threadId}`])))
    .data.node.comments.nodes.map((c) => ({ author: c.author?.login ?? "ghost", body: c.body }));
