import { execFileSync } from "node:child_process";
import { z } from "zod";
import { fail, required, writeJson } from "../shared/common";
import { REVIEW_MARKER, reviewContextSchema, type ReviewContext } from "./review-context";

/**
 * The GitHub boundary on the way in: the only place raw GitHub JSON is read. Writes context.json to OUTPUT_DIR.
 * PR text is untrusted, so it goes to a file and is never put into shell or workflow expressions.
 */

const gh = (args: string[]): string =>
  execFileSync("gh", args, { encoding: "utf8", stdio: ["ignore", "pipe", "inherit"], maxBuffer: 256 * 1024 * 1024 });

const filesSchema = z.array(z.array(z.object({ filename: z.string(), patch: z.string().optional() })));
const reviewsSchema = z.array(z.array(z.object({ body: z.string().nullable() })));
const threadsSchema = z.object({
  data: z.object({
    repository: z.object({
      pullRequest: z.object({
        reviewThreads: z.object({
          nodes: z.array(
            z.object({
              id: z.string(),
              isResolved: z.boolean(),
              path: z.string(),
              line: z.number().int().nullable(),
              comments: z.object({
                nodes: z.array(
                  z.object({
                    databaseId: z.number().int(),
                    author: z.object({ login: z.string() }).nullable(),
                    body: z.string(),
                  }),
                ),
              }),
            }),
          ),
        }),
      }),
    }),
  }),
});

const QUERY = `query($owner: String!, $name: String!, $number: Int!) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      reviewThreads(first: 100) {
        nodes { id isResolved path line comments(first: 50) { nodes { databaseId author { login } body } } }
      }
    }
  }
}`;

try {
  const repo = required("GH_REPO");
  const [owner = "", name = ""] = repo.split("/");
  const pr = Number(required("PR_NUMBER"));

  const files = filesSchema.parse(JSON.parse(gh(["api", "--paginate", "--slurp", `repos/${repo}/pulls/${pr}/files`]))).flat();
  const reviews = reviewsSchema.parse(JSON.parse(gh(["api", "--paginate", "--slurp", `repos/${repo}/pulls/${pr}/reviews`]))).flat();
  const threads = threadsSchema.parse(
    JSON.parse(
      gh(["api", "graphql", "-f", `query=${QUERY}`, "-F", `owner=${owner}`, "-F", `name=${name}`, "-F", `number=${pr}`]),
    ),
  ).data.repository.pullRequest.reviewThreads.nodes;

  const openThreads = threads.flatMap((t) => {
    const first = t.comments.nodes[0];
    if (t.isResolved || first === undefined || !first.body.includes(REVIEW_MARKER)) return [];
    return [
      {
        threadId: t.id,
        firstCommentId: first.databaseId,
        path: t.path,
        line: t.line,
        comments: t.comments.nodes.map((c) => ({ author: c.author?.login ?? "ghost", body: c.body })),
      },
    ];
  });

  const reviewedShas = reviews.flatMap((r) => {
    const body = r.body ?? "";
    const sha = body.includes(REVIEW_MARKER) ? /^sha=([0-9a-f]+)$/m.exec(body)?.[1] : undefined;
    return sha === undefined ? [] : [sha];
  });

  const context: ReviewContext = reviewContextSchema.parse({
    pr,
    baseSha: required("BASE_SHA"),
    headSha: required("HEAD_SHA"),
    files: files.map((f) => ({ path: f.filename, patch: f.patch ?? "" })),
    openThreads,
    reviewedShas,
  });
  writeJson("context.json", context);
  console.log(`Context written: ${files.length} files, ${openThreads.length} open threads.`);
} catch (error) {
  fail(error instanceof Error ? error.message : String(error));
}
