import { z } from "zod";
import type { Strength } from "../shared/candidate-labels";
import { IMPLEMENT_LABEL, REQUEST_LABEL } from "../shared/labels";

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
