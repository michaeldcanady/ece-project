import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { execFileSync } from "node:child_process";
import { specSchema } from "./spec-plan";
import { required } from "../shared/common";

/**
 * Replace the issue body with the expanded spec, keeping the original note at
 * the bottom. The only step with issue write access; the spec crossed a job
 * boundary as an artifact, so it is validated again here.
 */

const gh = (args: string[]): string =>
  execFileSync("gh", args, { encoding: "utf8", stdio: ["ignore", "pipe", "inherit"] });

const issue = required("ISSUE_NUMBER");
const spec = specSchema.parse(
  JSON.parse(fs.readFileSync(path.join(required("SPEC_DIR"), "spec.json"), "utf8")),
);
const original = (
  JSON.parse(fs.readFileSync(required("ISSUE_FILE"), "utf8")) as { body: string }
).body;

const file = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "spec-")), "body.md");
fs.writeFileSync(
  file,
  `${spec.body}\n\n---\nSpecced by the \`agent:spec\` workflow. Add the \`agent:implement\` label to have it implemented.\n\n<details><summary>Original note</summary>\n\n${original}\n\n</details>\n`,
);
gh(["issue", "edit", issue, "--body-file", file]);
gh(["issue", "edit", issue, "--remove-label", "agent:spec"]);
console.log(`Issue #${issue} now holds the full spec.`);
