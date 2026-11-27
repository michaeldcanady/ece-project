import * as fs from "node:fs";
import { z } from "zod";
import { required } from "../shared/common";
import { gh } from "../shared/gh";
import { BLOCKED_LABEL, IMPLEMENT_LABEL, RECOVERED_LABEL, labelNames } from "../shared/labels";
import { failureComment, recoveryAction, redact } from "../shared/recovery";

/**
 * After a failed, cancelled or timed-out implement run: the first failure comments the reason, then marks the issue
 * `agent:recovered` and re-queues it in one call (a cut-off run cannot leave one label without the other); a failure
 * on an already recovered issue labels it `agent:blocked`. GH_TOKEN is the PAT, so the label starts the next run.
 */

const issue = required("ISSUE_NUMBER");
const pat = required("AGENT_PAT");
const reasonFile = required("REASON_FILE");
const runUrl = `${required("GITHUB_SERVER_URL")}/${required("GH_REPO")}/actions/runs/${required("GITHUB_RUN_ID")}`;

const saved = fs.existsSync(reasonFile) ? fs.readFileSync(reasonFile, "utf8").trim() : "";
const reason = redact(
  saved === "" ? `The run ended before it could say why (failed, cancelled or timed out). See the workflow log: ${runUrl}` : saved,
  [pat],
);

const labels = z
  .object({ labels: labelNames })
  .parse(JSON.parse(gh(["issue", "view", issue, "--json", "labels"]))).labels;
const action = recoveryAction(labels);

gh(["issue", "edit", issue, "--remove-label", "agent:in-progress"]);
if (action === "give-up") gh(["issue", "edit", issue, "--add-label", BLOCKED_LABEL]);
gh(["issue", "comment", issue, "--body", failureComment(action, reason)]);
// Last, and in one call, so the comment is in place when the next run reads the issue.
if (action === "retry") gh(["issue", "edit", issue, "--add-label", RECOVERED_LABEL, "--add-label", IMPLEMENT_LABEL]);
console.log(`Issue #${issue}: ${action === "retry" ? "queued for its recovery run" : "blocked"}.`);
