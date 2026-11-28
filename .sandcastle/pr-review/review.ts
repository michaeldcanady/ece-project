import * as fs from "node:fs";
import * as path from "node:path";
import * as sandcastle from "@ai-hero/sandcastle";
import { noSandbox } from "@ai-hero/sandcastle/sandboxes/no-sandbox";
import { claudeAgent, exec, fail, required, writeJson, writingStyle } from "../shared/common";
import { runStructured } from "../shared/run-structured";
import { alreadyReviewed } from "./review-actions";
import { reviewContextSchema, type ReviewContext } from "./review-context";
import { reviewSchema } from "./review-plan";

/**
 * Review a PR's diff and judge its earlier threads. Read-only: writes review.json to OUTPUT_DIR (nothing when this
 * head was already reviewed). apply.ts, in a separate job, is the only part that touches GitHub.
 */

const here = (file: string) => path.join(import.meta.dirname, file);

const head = (): string => exec("git", ["rev-parse", "HEAD"]).output.trim();

/** The repo's standards, inlined so the reviewer cannot skip reading them. `CLAUDE.md` is loaded by Claude Code itself. */
const standards = (): string => {
  const rules = fs.existsSync(".claude/rules")
    ? fs.readdirSync(".claude/rules").filter((f) => f.endsWith(".md")).sort().map((f) => `.claude/rules/${f}`)
    : [];
  return ["GLOSSARY.md", "CODING_STANDARDS.md", ...rules]
    .filter((f) => fs.existsSync(f))
    .map((f) => `## ${f}\n\n${fs.readFileSync(f, "utf8").trim()}`)
    .join("\n\n");
};

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
    // The first review of a PR is the strongest; later pushes are checked against threads the first one opened.
    const role = context.reviewedShas.length === 0 ? "prReviewFirst" : "prReviewFollowup";
    const before = head();
    const review = await runStructured({
      name: `pr-review-${context.pr}`,
      agent: claudeAgent(role),
      sandbox: noSandbox(),
      logging: { type: "stdout" },
      maxIterations: 1,
      idleTimeoutSeconds: 900,
      promptFile: here("prompt.md"),
      promptArgs: {
        BASE_SHA: context.baseSha,
        HEAD_SHA: context.headSha,
        OPEN_THREADS: renderThreads(context),
        WRITING_STYLE: writingStyle(),
        STANDARDS: standards(),
        OUTPUT_SHAPE: fs.readFileSync(here("output-shape.md"), "utf8"),
      },
      output: sandcastle.Output.object({ tag: "output", schema: reviewSchema }),
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
