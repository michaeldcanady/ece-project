import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import { execFileSync } from "node:child_process";
import { planSchema, STRENGTHS } from "./plan";
import { required } from "../shared/common";

/**
 * Apply the plan: file one issue per candidate. The only step with GitHub
 * write access; the agent never sees a GitHub token. The plan is validated
 * again here because it crossed a job boundary as an artifact.
 */

const dryRun = process.env.DRY_RUN === "true";
const summary = process.env.GITHUB_STEP_SUMMARY;

const gh = (args: string[]): string =>
  execFileSync("gh", args, { encoding: "utf8", stdio: ["ignore", "pipe", "inherit"] });

const plan = planSchema.parse(
  JSON.parse(fs.readFileSync(path.join(required("PLAN_DIR"), "plan.json"), "utf8")),
);

const note = (line: string): void => {
  console.log(line);
  if (summary) fs.appendFileSync(summary, `${line}\n`);
};

if (plan.candidates.length === 0) {
  note("No candidate worth filing tonight.");
  process.exit(0);
}

if (!dryRun) {
  gh(["label", "create", "agent:candidate", "--force", "--description", "Architecture candidate filed by the nightly run"]);
  for (const s of STRENGTHS) {
    gh(["label", "create", `strength:${s}`, "--force"]);
  }
}

for (const c of plan.candidates) {
  if (c.strength === "speculative") continue;
  if (dryRun) {
    note(`### [dry run] ${c.title} (strength:${c.strength})\n\n${c.body}\n`);
    continue;
  }
  const file = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "issue-")), "body.md");
  fs.writeFileSync(file, `${c.body}\n\n---\nFiled by the nightly improve run. Add the \`agent:implement\` label to have it implemented.\n`);
  const url = gh([
    "issue", "create",
    "--title", c.title,
    "--body-file", file,
    "--label", "agent:candidate",
    "--label", `strength:${c.strength}`,
  ]).trim();
  note(`Filed ${url}`);
}
