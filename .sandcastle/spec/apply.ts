import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { z } from "zod";
import { specSchema } from "./spec-plan";
import { ensureCandidateLabels, specEditArgs } from "../shared/candidate-labels";
import { speccedBody } from "../shared/candidate-issue";
import { required } from "../shared/common";
import { authorAssociation, autoImplement } from "./spec-labels";
import { gh } from "../shared/gh";
import { BLOCKED_LABEL, IMPLEMENT_LABEL, RECOVERED_LABEL, labelNames } from "../shared/labels";

/**
 * Replace the issue body with the expanded spec, keeping the original note at the bottom, and label the issue a
 * candidate of the spec's strength. The only step with issue write access; the spec crossed a job boundary as an
 * artifact, so it is validated again here. A strong `request` also gets `agent:implement`, added with the PAT.
 */

const issueFileSchema = z.object({
  body: z.string(),
  labels: labelNames,
  authorAssociation,
});

const issue = required("ISSUE_NUMBER");
const spec = specSchema.parse(
  JSON.parse(fs.readFileSync(path.join(required("SPEC_DIR"), "spec.json"), "utf8")),
);
const current = issueFileSchema.parse(JSON.parse(fs.readFileSync(required("ISSUE_FILE"), "utf8")));

const file = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "spec-")), "body.md");
fs.writeFileSync(file, speccedBody(spec.body, spec.strength, current.body));

ensureCandidateLabels(gh);
const labels = current.labels;
// A success spends the retry: a later failure gets its own recovery attempt.
const recovered = [RECOVERED_LABEL, BLOCKED_LABEL].filter((l) => labels.includes(l)).flatMap((l) => ["--remove-label", l]);
gh([...specEditArgs(issue, file, spec.strength, labels), ...recovered]);
console.log(`Issue #${issue} now holds the full spec, labeled strength:${spec.strength}.`);

// A strong human request goes straight to the implement queue; anything else waits for a human. The label is
// added with the PAT, in its own call: an event caused by GITHUB_TOKEN would not start implement.yml.
if (autoImplement(labels, spec.strength, current.authorAssociation)) {
  gh(["issue", "edit", issue, "--add-label", IMPLEMENT_LABEL], required("AGENT_PAT"));
  console.log(`Labeled #${issue} ${IMPLEMENT_LABEL}.`);
}
