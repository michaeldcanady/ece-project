import * as fs from "node:fs";
import * as path from "node:path";
import * as sandcastle from "@ai-hero/sandcastle";
import { noSandbox } from "@ai-hero/sandcastle/sandboxes/no-sandbox";
import {
  claudeAgent,
  exec,
  fail,
  IMPLEMENT_REVIEW_PASS,
  outputDir,
  required,
  writeText,
  writingStyle,
} from "../shared/common";

/**
 * Implement an issue, gate on `mise run check`, review, gate again.
 * Commits land on the checked-out branch; this script never pushes and holds
 * no GitHub credentials. The workflow pushes and opens the PR only if this
 * exits 0. On failure it writes failure_reason.txt and exits non-zero.
 */

const ISSUE_FILE = required("ISSUE_FILE"); // JSON from `gh issue view --json title,body,comments`
const ISSUE_NUMBER = required("ISSUE_NUMBER");
const BRANCH = required("BRANCH");
// One agent iteration per pass; the recovery attempt is the next workflow run, which reads the failure comment.
const ITERATIONS_PER_PASS = 1;

const here = (file: string) => path.join(import.meta.dirname, file);

const check = (): { ok: boolean; output: string } => {
  const r = exec("mise", ["run", "check"]);
  return { ok: r.code === 0, output: r.output };
};

const tail = (text: string, lines = 60): string => text.split("\n").slice(-lines).join("\n");

const commitsAhead = (): number =>
  Number(exec("git", ["rev-list", "--count", "origin/main..HEAD"]).output.trim());

// The push uses a token that can reach the workflows, so the agent may not change the machinery that runs it.
const PROTECTED = [".github/", ".sandcastle/", "CLAUDE.md", "CODING_STANDARDS.md", ".claude/", ".githooks/"];

const guardProtected = (who: string): void => {
  const touched = exec("git", ["diff", "--name-only", "origin/main...HEAD"])
    .output.split("\n")
    .filter((f) => PROTECTED.some((p) => f === p || f.startsWith(p)));
  if (touched.length > 0) fail(`${who} changed protected files:\n${touched.join("\n")}`);
};

try {
  const issue = JSON.parse(fs.readFileSync(ISSUE_FILE, "utf8")) as {
    title: string;
    body: string;
    comments: { body: string }[];
  };
  const issueContext = [
    `# ${issue.title}`,
    issue.body,
    ...issue.comments.map((c, i) => `## Comment ${i + 1}\n${c.body}`),
  ].join("\n\n");

  const implementNotesFile = path.join(outputDir(), "implement_notes.md");
  const reviewNotesFile = path.join(outputDir(), "review_notes.md");

  const common = {
    agent: claudeAgent("implement"),
    sandbox: noSandbox(),
    logging: { type: "stdout" } as const,
    idleTimeoutSeconds: 600,
  };

  await sandcastle.run({
    ...common,
    name: `implement-#${ISSUE_NUMBER}`,
    // One attempt. A failed run gets one recovery run (see implement.yml), not more iterations here.
    maxIterations: ITERATIONS_PER_PASS,
    promptFile: here("prompt.md"),
    promptArgs: {
      ISSUE_NUMBER,
      BRANCH,
      ISSUE_CONTEXT: issueContext,
      NOTES_FILE: implementNotesFile,
      WRITING_STYLE: writingStyle(),
    },
  });

  if (commitsAhead() === 0) fail("The implement agent made no commits.");

  guardProtected("The implement agent");

  const first = check();
  if (!first.ok) fail(`mise run check failed after implementation:\n\n${tail(first.output)}`);

  if (IMPLEMENT_REVIEW_PASS) {
    await sandcastle.run({
      ...common,
      agent: claudeAgent("implementReview"),
      name: `review-#${ISSUE_NUMBER}`,
      maxIterations: ITERATIONS_PER_PASS,
      promptFile: here("review-prompt.md"),
      promptArgs: { ISSUE_NUMBER, BRANCH, ISSUE_CONTEXT: issueContext, NOTES_FILE: reviewNotesFile, WRITING_STYLE: writingStyle() },
    });

    guardProtected("The review pass");

    const second = check();
    if (!second.ok) fail(`mise run check failed after review:\n\n${tail(second.output)}`);
  }

  const readNotes = (file: string, none: string): string => {
    const text = fs.existsSync(file) ? fs.readFileSync(file, "utf8").trim() : "";
    return text === "" ? none : text;
  };
  writeText(
    "pr_body.md",
    [
      `Closes #${ISSUE_NUMBER}`,
      "",
      IMPLEMENT_REVIEW_PASS
        ? "Implemented by the nightly agent from the issue's spec, then reviewed by a second agent pass."
        : "Implemented by the nightly agent from the issue's spec.",
      "",
      "## Verification",
      IMPLEMENT_REVIEW_PASS
        ? "- `mise run check` (lint, strong-types check, tests) passed after implementation and again after review."
        : "- `mise run check` (lint, strong-types check, tests) passed after implementation.",
      "- The DAG tests need the sibling Airflow container, which does not exist on the runner, so they were **skipped**. Run them in the dev container before merging.",
      "",
      "## Implementation notes",
      readNotes(implementNotesFile, "(none: the agent made no choices beyond the issue and changed no unlisted files)"),
      ...(IMPLEMENT_REVIEW_PASS ? ["", "## Review pass", readNotes(reviewNotesFile, "(the review pass wrote no notes)")] : []),
    ].join("\n"),
  );
} catch (error) {
  fail(error instanceof Error ? error.message : String(error));
}
