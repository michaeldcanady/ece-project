import { z } from "zod";
import { failurePlan } from "./spec-labels";
import { gh } from "../shared/gh";
import { SPEC_LABEL, labelNames } from "../shared/labels";
import { required } from "../shared/common";

/**
 * After a failed or cancelled spec run: retry once, then stop. The first failure marks the issue
 * `agent:recovered` and re-adds `agent:spec` with the PAT (a label added with GITHUB_TOKEN starts no workflow);
 * a failure on an already recovered issue removes `agent:spec`, labels it `agent:blocked` and leaves the body unchanged.
 */

const issue = required("ISSUE_NUMBER");
const runUrl = `${required("GITHUB_SERVER_URL")}/${required("GH_REPO")}/actions/runs/${required("GITHUB_RUN_ID")}`;

// Read the labels now, not from the fetch artifact: the issue may have changed since the run began.
const labels = z
  .object({ labels: labelNames })
  .parse(JSON.parse(gh(["issue", "view", issue, "--json", "labels"]))).labels;

const plan = failurePlan(labels);
// A label that does not exist makes `gh issue edit` fail and skips the rest of this handler, so create it first
// (`--force`: without it, `gh label create` fails when the label already exists).
const { name, description, color } = plan.createLabel;
gh(["label", "create", name, "--force", "--description", description, "--color", color]);

if (plan.action === "retry") {
  const pat = required("AGENT_PAT");
  gh(["issue", "comment", issue, "--body", `The spec run failed, so the issue is unchanged. Retrying once. See the workflow log: ${runUrl}`]);
  // Remove first: adding a label the issue already has raises no event.
  gh(["issue", "edit", issue, ...plan.edit]);
  gh(["issue", "edit", issue, "--add-label", SPEC_LABEL], pat);
  console.log(`Issue #${issue}: retrying the spec run once.`);
} else {
  // The edit also clears the recovery mark: a human who re-adds `agent:spec` starts with a fresh attempt.
  gh(["issue", "edit", issue, ...plan.edit]);
  gh(["issue", "comment", issue, "--body", `The spec run failed again, so the issue is unchanged and needs a human. See the workflow log: ${runUrl}`]);
  console.log(`Issue #${issue}: the retry is spent; left unchanged.`);
}
