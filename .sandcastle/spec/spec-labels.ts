import type { Strength } from "../shared/candidate-labels";
import { IMPLEMENT_LABEL, RECOVERED_LABEL, REQUEST_LABEL } from "../shared/labels";

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
  labels.includes(REQUEST_LABEL) &&
  strength === "strong" &&
  TRUSTED_AUTHORS.includes(authorAssociation) &&
  !labels.includes(IMPLEMENT_LABEL);
