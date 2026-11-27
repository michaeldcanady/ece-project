import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { execFileSync } from "node:child_process";
import { z } from "zod";
import { specSchema } from "./spec-plan";
import { ensureCandidateLabels, specEditArgs } from "../shared/candidate-labels";
import { speccedBody } from "../shared/candidate-issue";
import { required } from "../shared/common";

/**
 * Replace the issue body with the expanded spec, keeping the original note at the bottom, and label the issue a
 * candidate of the spec's strength. The only step with issue write access; the spec crossed a job boundary as an
 * artifact, so it is validated again here.
 */

const gh = (args: string[]): string =>
  execFileSync("gh", args, { encoding: "utf8", stdio: ["ignore", "pipe", "inherit"] });

const issueFileSchema = z.object({
  body: z.string(),
  labels: z.array(z.object({ name: z.string() })),
});

const issue = required("ISSUE_NUMBER");
const spec = specSchema.parse(
  JSON.parse(fs.readFileSync(path.join(required("SPEC_DIR"), "spec.json"), "utf8")),
);
const current = issueFileSchema.parse(JSON.parse(fs.readFileSync(required("ISSUE_FILE"), "utf8")));

const file = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "spec-")), "body.md");
fs.writeFileSync(file, speccedBody(spec.body, spec.strength, current.body));

ensureCandidateLabels(gh);
gh(specEditArgs(issue, file, spec.strength, current.labels.map((l) => l.name)));
console.log(`Issue #${issue} now holds the full spec, labeled strength:${spec.strength}.`);
