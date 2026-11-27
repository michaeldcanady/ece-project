import * as fs from "node:fs";
import * as path from "node:path";
import * as sandcastle from "@ai-hero/sandcastle";
import { noSandbox } from "@ai-hero/sandcastle/sandboxes/no-sandbox";
import { claudeAgent, fail, required, writeJson, writingStyle } from "../shared/common";
import { runWithExtraction } from "../shared/run-with-extraction";
import { specSchema } from "./spec-plan";

/**
 * Expand a thin issue (a backlog note) into a full spec. Read-only: writes
 * spec.json to OUTPUT_DIR. apply.ts, in a separate job, replaces the body.
 */

const here = (file: string) => path.join(import.meta.dirname, file);

try {
  const issue = JSON.parse(fs.readFileSync(required("ISSUE_FILE"), "utf8")) as {
    title: string;
    body: string;
    comments: { body: string }[];
  };
  const note = [issue.body, ...issue.comments.map((c, i) => `Comment ${i + 1}:\n${c.body}`)].join("\n\n");

  const spec = await runWithExtraction({
    name: "spec",
    agent: claudeAgent(),
    sandbox: noSandbox(),
    logging: { type: "stdout" },
    maxIterations: 1,
    idleTimeoutSeconds: 900,
    promptFile: here("prompt.md"),
    promptArgs: {
      ISSUE_TITLE: issue.title,
      ISSUE_NOTE: note,
      WRITING_STYLE: writingStyle(),
      STRENGTH_RUBRIC: fs.readFileSync(here("../shared/strength-rubric.md"), "utf8"),
      SPEC_FORMAT: fs.readFileSync(here("../shared/spec-format.md"), "utf8"),
    },
    output: sandcastle.Output.object({ tag: "output", schema: specSchema }),
    extractionPrompt: fs.readFileSync(here("extraction.md"), "utf8"),
    maxRetries: 1,
  });

  writeJson("spec.json", spec.output);
  console.log("Spec written.");
} catch (error) {
  fail(error instanceof Error ? error.message : String(error));
}
