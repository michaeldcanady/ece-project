import * as fs from "node:fs";
import * as path from "node:path";
import * as sandcastle from "@ai-hero/sandcastle";
import { noSandbox } from "@ai-hero/sandcastle/sandboxes/no-sandbox";
import {
  claudeAgent,
  exec,
  fail,
  outputDir,
  required,
  writeText,
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
const MAX_ITERATIONS = 5;

const here = (file: string) => path.join(import.meta.dirname, file);

const check = (): { ok: boolean; output: string } => {
  const r = exec("mise", ["run", "check"]);
  return { ok: r.code === 0, output: r.output };
};

const tail = (text: string, lines = 60): string => text.split("\n").slice(-lines).join("\n");

const commitsAhead = (): number =>
  Number(exec("git", ["rev-list", "--count", "origin/main..HEAD"]).output.trim());

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

  const common = {
    agent: claudeAgent(),
    sandbox: noSandbox(),
    logging: { type: "stdout" } as const,
    idleTimeoutSeconds: 600,
  };

  await sandcastle.run({
    ...common,
    name: `implement-#${ISSUE_NUMBER}`,
    maxIterations: MAX_ITERATIONS,
    promptFile: here("prompt.md"),
    promptArgs: { ISSUE_NUMBER, BRANCH, ISSUE_CONTEXT: issueContext },
  });

  if (commitsAhead() === 0) fail("The implement agent made no commits.");

  const first = check();
  if (!first.ok) fail(`mise run check failed after implementation:\n\n${tail(first.output)}`);

  const notesFile = path.join(outputDir(), "review_notes.md");
  await sandcastle.run({
    ...common,
    name: `review-#${ISSUE_NUMBER}`,
    maxIterations: 1,
    promptFile: here("review-prompt.md"),
    promptArgs: { ISSUE_NUMBER, BRANCH, ISSUE_CONTEXT: issueContext, NOTES_FILE: notesFile },
  });

  const second = check();
  if (!second.ok) fail(`mise run check failed after review:\n\n${tail(second.output)}`);

  const notes = fs.existsSync(notesFile) ? fs.readFileSync(notesFile, "utf8") : "(the review pass wrote no notes)";
  writeText(
    "pr_body.md",
    [
      `Closes #${ISSUE_NUMBER}`,
      "",
      "Implemented by the nightly agent from the issue's spec, then reviewed by a second agent pass.",
      "",
      "## Verification",
      "- `mise run check` (lint, strong-types check, tests) passed after implementation and again after review.",
      "- The DAG tests need the sibling Airflow container, which does not exist on the runner, so they were **skipped**. Run them in the dev container before merging.",
      "",
      "## Review pass",
      notes,
    ].join("\n"),
  );
} catch (error) {
  fail(error instanceof Error ? error.message : String(error));
}
