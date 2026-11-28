import * as fs from "node:fs";
import * as path from "node:path";
import { execFileSync } from "node:child_process";
import * as sandcastle from "@ai-hero/sandcastle";

export type Effort = "low" | "medium" | "high" | "xhigh" | "max";

export interface Role {
  readonly model: string;
  readonly effort: Effort;
}

const SONNET = "claude-sonnet-5-5";
const OPUS = "claude-opus-5-5";

/**
 * Model and reasoning effort for every agent session; change them here only. Effort is the cheaper lever: lower it
 * before switching model, and raise it before switching up. Evidence for each choice: the implement gate
 * (`mise run check`) catches most errors, so implement does not need high effort; the first review of a PR is the
 * human's last automated check, so it gets the strongest model.
 */
export const ROLES = {
  improve: { model: SONNET, effort: "high" },
  spec: { model: SONNET, effort: "high" },
  implement: { model: SONNET, effort: "medium" },
  implementReview: { model: SONNET, effort: "low" },
  prReviewFirst: { model: OPUS, effort: "high" },
  prReviewFollowup: { model: SONNET, effort: "medium" },
} as const satisfies Record<string, Role>;

export type RoleName = keyof typeof ROLES;

/**
 * Whether implement runs its own review pass before the PR opens. pr-review reads the same diff against the same
 * standards, so leaving this off saves one agent session and one `mise run check` per issue. Off by default: one
 * observed pass fixed a hypothetical edge case and left the rest for the human.
 */
export const IMPLEMENT_REVIEW_PASS = false;

export const outputDir = (): string => process.env.OUTPUT_DIR ?? "/tmp";

export const required = (name: string): string => {
  const value = process.env[name];
  if (value === undefined || value === "") {
    console.error(`Missing required env var: ${name}`);
    process.exit(1);
  }
  return value;
};

/** Record why the run failed (the workflow comments it on the issue) and exit non-zero. */
export const fail = (message: string): never => {
  console.error(`\nFAILED: ${message}`);
  fs.mkdirSync(outputDir(), { recursive: true });
  fs.writeFileSync(path.join(outputDir(), "failure_reason.txt"), message);
  process.exit(1);
};

export const writeText = (filename: string, value: string): void => {
  fs.mkdirSync(outputDir(), { recursive: true });
  fs.writeFileSync(path.join(outputDir(), filename), value);
};

export const writeJson = (filename: string, value: unknown): void =>
  writeText(filename, JSON.stringify(value, null, 2));

/** Run a command without a shell. Returns its exit code and combined output; never throws. */
export const exec = (
  file: string,
  args: string[],
): { code: number; output: string } => {
  try {
    const output = execFileSync(file, args, {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    });
    return { code: 0, output };
  } catch (error) {
    const e = error as { status?: number; stdout?: string; stderr?: string };
    return {
      code: e.status ?? 1,
      output: `${e.stdout ?? ""}${e.stderr ?? ""}`,
    };
  }
};

/** The agent gets the OAuth token and nothing else: no GitHub credentials. */
const agentFor = ({ model, effort }: Role) =>
  sandcastle.claudeCode(model, {
    effort,
    env: { CLAUDE_CODE_OAUTH_TOKEN: required("CLAUDE_CODE_OAUTH_TOKEN") },
  });

export const claudeAgent = (role: RoleName) => agentFor(ROLES[role]);


/** The shared writing style, injected into every agent prompt as `{{WRITING_STYLE}}`. */
export const writingStyle = (): string =>
  fs.readFileSync(path.join(import.meta.dirname, "writing-style.md"), "utf8");
