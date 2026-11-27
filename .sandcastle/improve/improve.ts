import * as fs from "node:fs";
import * as path from "node:path";
import * as sandcastle from "@ai-hero/sandcastle";
import { noSandbox } from "@ai-hero/sandcastle/sandboxes/no-sandbox";
import { claudeAgent, fail, required, writeJson } from "../shared/common";
import { alreadyFiledList } from "../shared/candidate-issue";
import { runWithExtraction } from "../shared/run-with-extraction";
import { existingSchema, planSchema } from "./plan";

/**
 * Scan and spec. Read-only: writes plan.json to OUTPUT_DIR and nothing else.
 * apply.ts, in a separate job, is the only thing that touches GitHub. The
 * GitHub Actions runner is the sandbox, so no container is needed.
 */

const here = (file: string) => path.join(import.meta.dirname, file);

try {
  const existing = existingSchema.parse(
    JSON.parse(fs.readFileSync(required("EXISTING_FILE"), "utf8")),
  );
  const alreadyFiled = alreadyFiledList(existing);

  const plan = await runWithExtraction({
    name: "improve-scan",
    agent: claudeAgent(),
    sandbox: noSandbox(),
    logging: { type: "stdout" },
    maxIterations: 1,
    idleTimeoutSeconds: 900,
    promptFile: here("prompt.md"),
    promptArgs: {
      ALREADY_FILED: alreadyFiled,
      STRENGTH_RUBRIC: fs.readFileSync(path.join(import.meta.dirname, "../shared/strength-rubric.md"), "utf8"),
      SPEC_FORMAT: fs.readFileSync(path.join(import.meta.dirname, "../shared/spec-format.md"), "utf8"),
    },
    output: sandcastle.Output.object({ tag: "output", schema: planSchema }),
    extractionPrompt: fs.readFileSync(here("extraction.md"), "utf8"),
    maxRetries: 1,
  });

  writeJson("plan.json", plan.output);
  console.log(`Plan written: ${plan.output.candidates.length} candidate(s).`);
} catch (error) {
  fail(error instanceof Error ? error.message : String(error));
}
