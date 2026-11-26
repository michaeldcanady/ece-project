import { z } from "zod";
import { fail, required, writeJson } from "../shared/common";
import { gh, readReviews } from "./github-reads";
import { ownedOpenThreads, reviewedShasOf } from "./review-actions";
import { reviewContextSchema, type ReviewContext } from "./review-context";

/**
 * The GitHub boundary on the way in: with github-reads.ts, the only place raw GitHub JSON is read. Writes context.json to OUTPUT_DIR.
 * PR text is untrusted, so it goes to a file and is never put into shell or workflow expressions.
 */

const filesSchema = z.array(z.array(z.object({ filename: z.string(), patch: z.string().optional() })));
const threadsSchema = z.object({
  data: z.object({
    repository: z.object({
      pullRequest: z.object({
        author: z.object({ login: z.string() }).nullable(),
        reviewThreads: z.object({
          pageInfo: z.object({ hasNextPage: z.boolean() }),
          nodes: z.array(
            z.object({
              id: z.string(),
              isResolved: z.boolean(),
              path: z.string(),
              line: z.number().int().nullable(),
              comments: z.object({
                pageInfo: z.object({ hasNextPage: z.boolean() }),
                nodes: z.array(
                  z.object({
                    authorAssociation: z.string(),
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
      author { login }
      reviewThreads(first: 100) {
        pageInfo { hasNextPage }
        nodes {
          id isResolved path line
          comments(first: 50) { pageInfo { hasNextPage } nodes { databaseId authorAssociation author { login } body } }
        }
      }
    }
  }
}`;

try {
  const repo = required("GH_REPO");
  const [owner = "", name = ""] = repo.split("/");
  const pr = Number(required("PR_NUMBER"));

  const files = filesSchema.parse(JSON.parse(gh(["api", "--paginate", "--slurp", `repos/${repo}/pulls/${pr}/files`]))).flat();
  const pullRequest = threadsSchema.parse(
    JSON.parse(
      gh(["api", "graphql", "-f", `query=${QUERY}`, "-f", `owner=${owner}`, "-f", `name=${name}`, "-F", `number=${pr}`]),
    ),
  ).data.repository.pullRequest;
  const threads = pullRequest.reviewThreads;
  if (threads.pageInfo.hasNextPage || threads.nodes.some((t) => t.comments.pageInfo.hasNextPage)) {
    console.warn("Warning: more review threads or comments than one page; the extra ones are not considered.");
  }

  const openThreads = ownedOpenThreads(
    threads.nodes.map((t) => ({
      id: t.id,
      isResolved: t.isResolved,
      path: t.path,
      line: t.line,
      comments: t.comments.nodes.map((c) => ({
        databaseId: c.databaseId,
        author: c.author?.login ?? "ghost",
        authorAssociation: c.authorAssociation,
        body: c.body,
      })),
    })),
    pullRequest.author?.login ?? "",
  );
  const reviewedShas = reviewedShasOf(readReviews(repo, pr));

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
