import { z } from "zod";
import type { Strength } from "../shared/candidate-labels";
import { recoveryAction, type Recovery } from "../shared/recovery";
import { BLOCKED_LABEL, IMPLEMENT_LABEL, RECOVERED_LABEL, REQUEST_LABEL, SPEC_LABEL } from "../shared/labels";

/** How `gh` reports an issue author's tie to the repository. */
export const authorAssociation = z.enum([
  "OWNER",
  "MEMBER",
  "COLLABORATOR",
  "CONTRIBUTOR",
  "FIRST_TIME_CONTRIBUTOR",
  "FIRST_TIMER",
  "MANNEQUIN",
  "NONE",
]);
export type AuthorAssociation = z.infer<typeof authorAssociation>;

/** Who may file a request that starts an agent run unattended: the repository's own people, not any commenter. */
const TRUSTED_AUTHORS: readonly AuthorAssociation[] = ["OWNER", "MEMBER", "COLLABORATOR"];

/**
 * Pure. Only a strong request from a trusted author goes straight to the implement queue: the issue text steers
 * an agent, so a stranger's text waits for a human. An issue already queued is left alone.
 */
export const autoImplement = (labels: readonly string[], strength: Strength, author: AuthorAssociation): boolean =>
  labels.includes(REQUEST_LABEL) &&
  strength === "strong" &&
  TRUSTED_AUTHORS.includes(author) &&
  !labels.includes(IMPLEMENT_LABEL);

/** What `gh` calls a failed spec run makes on the issue, as data so the label rules can be tested without `gh`. */
export interface FailurePlan {
  readonly action: Recovery;
  /** The label this path adds; it must exist before `gh issue edit` can add it. */
  readonly createLabel: { readonly name: string; readonly description: string; readonly color: string };
  /** Arguments after `gh issue edit <n>`, for the call made with GITHUB_TOKEN. */
  readonly edit: readonly string[];
}

/** Pure. A first failure marks `agent:recovered` and swaps out `agent:spec` (the PAT re-adds it); a second labels `agent:blocked`. */
export const failurePlan = (labels: readonly string[]): FailurePlan =>
  recoveryAction(labels) === "retry"
    ? { action: "retry", createLabel: { name: RECOVERED_LABEL, description: "The one recovery attempt for this issue has been used", color: "fef2c0" }, edit: ["--add-label", RECOVERED_LABEL, "--remove-label", SPEC_LABEL] }
    : {
        action: "give-up",
        createLabel: { name: BLOCKED_LABEL, description: "The agent could not finish this issue", color: "d93f0b" },
        edit: ["--remove-label", SPEC_LABEL, "--remove-label", RECOVERED_LABEL, "--add-label", BLOCKED_LABEL],
      };

/** Pure. The marks a successful spec run clears: a stale recovery attempt and a stale block. */
export const staleMarkArgs = (labels: readonly string[]): string[] =>
  [RECOVERED_LABEL, BLOCKED_LABEL].filter((l) => labels.includes(l)).flatMap((l) => ["--remove-label", l]);
