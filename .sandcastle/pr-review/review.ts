import * as fs from "node:fs";
import * as path from "node:path";
import * as sandcastle from "@ai-hero/sandcastle";
import { noSandbox } from "@ai-hero/sandcastle/sandboxes/no-sandbox";
import { claudeReviewAgent, exec, fail, required, writeJson } from "../shared/common";
import { runWithExtraction } from "../shared/run-with-extraction";
import { alreadyReviewed } from "./review-actions";
import { reviewContextSchema, type ReviewContext } from "./review-context";
import { reviewSchema } from "./review-plan";

/**
 * Review a PR's diff and judge its earlier threads. Read-only: writes review.json to OUTPUT_DIR (nothing when this
 * head was already reviewed). apply.ts, in a separate job, is the only part that touches GitHub.
 */

const here = (file: string) => path.join(import.meta.dirname, file);

const head = (): string => exec("git", ["rev-parse", "HEAD"]).output.trim();

const renderThreads = (context: ReviewContext): string =>
  context.openThreads.length === 0
    ? "(none)"
    : context.openThreads
        .map((t) => {
          const thread = t.comments.map((c) => `  ${c.author}: ${c.body}`).join("\n");
          return `Thread ${t.threadId} on ${t.path}:${t.line ?? "?"}\n${thread}`;
        })
        .join("\n\n");

try {
  const context = reviewContextSchema.parse(
    JSON.parse(fs.readFileSync(path.join(required("CONTEXT_DIR"), "context.json"), "utf8")),
  );
  if (alreadyReviewed(context)) {
    console.log(`Head ${context.headSha} was already reviewed; nothing to do.`);
  } else {
    const before = head();
    const review = await runWithExtraction({
      name: `pr-review-${context.pr}`,
      agent: claudeReviewAgent(),
      sandbox: noSandbox(),
      logging: { type: "stdout" },
      maxIterations: 1,
      idleTimeoutSeconds: 900,
      promptFile: here("prompt.md"),
      promptArgs: {
        BASE_SHA: context.baseSha,
        HEAD_SHA: context.headSha,
        OPEN_THREADS: renderThreads(context),
      },
      output: sandcastle.Output.object({ tag: "output", schema: reviewSchema }),
      extractionPrompt: fs.readFileSync(here("extraction.md"), "utf8"),
      maxRetries: 1,
    });

    if (exec("git", ["status", "--porcelain"]).output.trim() !== "" || head() !== before) {
      fail("The reviewer changed the working tree or HEAD; it must be read-only.");
    }
    writeJson("review.json", review.output);
    console.log("Review written.");
  }
} catch (error) {
  fail(error instanceof Error ? error.message : String(error));
}
