import type { Strength } from "../shared/candidate-labels";

/** Added to an issue whose spec run already failed once; its presence means the retry is spent. */
export const RECOVERED_LABEL = "agent:recovered";

/** The label a human adds to ask for a spec run, and that the retry re-adds. */
export const SPEC_LABEL = "agent:spec";

/** The label that puts an issue in the implement queue. */
export const IMPLEMENT_LABEL = "agent:implement";

/** What a failed spec run does next: spend the one retry, or stop and leave the issue unchanged. */
export type Recovery = "retry" | "give-up";

/** Pure. A spec run gets one attempt plus one recovery, so only an issue not yet marked recovered is retried. */
export const recoveryAction = (labels: readonly string[]): Recovery =>
  labels.includes(RECOVERED_LABEL) ? "give-up" : "retry";

/** Who may file a request that starts an agent run unattended: the repository's own people, not any commenter. */
const TRUSTED_AUTHORS: readonly string[] = ["OWNER", "MEMBER", "COLLABORATOR"];

/**
 * Pure. Only a strong request from a trusted author goes straight to the implement queue: the issue text steers
 * an agent, so a stranger's text waits for a human. An issue already queued is left alone.
 */
export const autoImplement = (labels: readonly string[], strength: Strength, authorAssociation: string): boolean =>
  labels.includes("request") &&
  strength === "strong" &&
  TRUSTED_AUTHORS.includes(authorAssociation) &&
  !labels.includes(IMPLEMENT_LABEL);
