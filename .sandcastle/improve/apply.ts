import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import { execFileSync } from "node:child_process";
import { planSchema } from "./plan";
import { CANDIDATE_LABEL, ensureCandidateLabels, strengthLabel } from "../shared/candidate-labels";
import { filedBody } from "../shared/candidate-issue";
import { required } from "../shared/common";

/**
 * Apply the plan: file one issue per candidate. The only step with GitHub
 * write access; the agent never sees a GitHub token. The plan is validated
 * again here because it crossed a job boundary as an artifact.
 */

const dryRun = process.env.DRY_RUN === "true";
const summary = process.env.GITHUB_STEP_SUMMARY;

const gh = (args: string[], token?: string): string =>
  execFileSync("gh", args, {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "inherit"],
    env: token === undefined ? process.env : { ...process.env, GH_TOKEN: token },
  });

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

if (!dryRun) ensureCandidateLabels(gh);

for (const c of plan.candidates) {
  if (c.strength === "speculative") continue;
  if (dryRun) {
    const auto = c.strength === "strong" ? " and would be labeled agent:implement" : "";
    note(`### [dry run] ${c.title} (strength:${c.strength}${auto})\n\n${c.body}\n`);
    continue;
  }
  const file = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "issue-")), "body.md");
  fs.writeFileSync(file, filedBody(c.body, c.strength));
  const url = gh([
    "issue", "create",
    "--title", c.title,
    "--body-file", file,
    "--label", CANDIDATE_LABEL,
    "--label", strengthLabel(c.strength),
  ]).trim();
  note(`Filed ${url}`);

  // A strong candidate goes straight to implementation; anything less waits for a human. The label is added with
  // the PAT, in its own call: an event caused by GITHUB_TOKEN would not start implement.yml.
  if (c.strength === "strong") {
    gh(["issue", "edit", url, "--add-label", "agent:implement"], required("AGENT_PAT"));
    note(`Labeled ${url} agent:implement`);
  }
}
