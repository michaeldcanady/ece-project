import { RECOVERED_LABEL } from "./labels";

/** What a failed job does next: spend its one recovery attempt, or stop and wait for a human. */
export type Recovery = "retry" | "give-up";

/** Pure. A job gets one attempt plus one recovery, so only an issue not yet marked recovered is retried. */
export const recoveryAction = (labels: readonly string[]): Recovery =>
  labels.includes(RECOVERED_LABEL) ? "give-up" : "retry";

/** Pure. Replace every occurrence of each secret, so failure text can be posted as a public comment. */
export const redact = (text: string, secrets: readonly string[]): string =>
  secrets.filter((s) => s !== "").reduce((t, s) => t.split(s).join("***"), text);

/** Pure. A code fence longer than any run of backticks in `text`, so the text cannot close it early. */
const fenceFor = (text: string): string => {
  const longest = Math.max(0, ...(text.match(/`+/g) ?? []).map((run) => run.length));
  return "`".repeat(Math.max(3, longest + 1));
};

/** Pure. The issue comment for a failed implement run: what happens next, then the reason in a fence. */
export const failureComment = (action: Recovery, reason: string): string => {
  const summary =
    action === "retry"
      ? "The agent could not finish this issue, so no PR was opened. It runs once more, reading this comment."
      : "The agent could not finish this issue on its recovery run either, so no PR was opened. The issue is blocked.";
  const fence = fenceFor(reason);
  return `${summary}\n\n${fence}\n${reason}\n${fence}\n`;
};
