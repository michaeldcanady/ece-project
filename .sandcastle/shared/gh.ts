import { execFileSync } from "node:child_process";

/** Run `gh` without a shell and return its stdout. A `token` replaces GH_TOKEN for this call only (the PAT, where a label must start a workflow). */
export const gh = (args: string[], token?: string): string =>
  execFileSync("gh", args, {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "inherit"],
    maxBuffer: 256 * 1024 * 1024,
    env: token === undefined ? process.env : { ...process.env, GH_TOKEN: token },
  });
