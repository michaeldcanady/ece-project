import * as fs from "node:fs";
import * as path from "node:path";
import { execFileSync } from "node:child_process";
import * as sandcastle from "@ai-hero/sandcastle";

/** The model both workflows use; change it here only. */
export const MODEL = "claude-opus-5-5";

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
export const claudeAgent = () =>
  sandcastle.claudeCode(MODEL, {
    env: { CLAUDE_CODE_OAUTH_TOKEN: required("CLAUDE_CODE_OAUTH_TOKEN") },
  });
