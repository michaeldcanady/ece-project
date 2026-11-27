/**
 * What makes an issue a backlog candidate: the `agent:candidate` label plus exactly one `strength:*` label.
 * Both the nightly improve run (new issues) and the spec run (existing issues) label through here.
 */

export const STRENGTHS = ["strong", "explore", "speculative"] as const;
export type Strength = (typeof STRENGTHS)[number];

export const CANDIDATE_LABEL = "agent:candidate";

export const strengthLabel = (strength: Strength): `strength:${Strength}` => `strength:${strength}`;

/** A gh call without a shell. */
export type Gh = (args: string[]) => string;

/** Create agent:candidate and every strength:* label (idempotent: `--force`). */
export const ensureCandidateLabels = (gh: Gh): void => {
  gh(["label", "create", CANDIDATE_LABEL, "--force", "--description", "Architecture candidate filed by the nightly run"]);
  for (const s of STRENGTHS) {
    gh(["label", "create", strengthLabel(s), "--force"]);
  }
};

export interface LabelChange {
  readonly add: readonly string[];
  readonly remove: readonly string[];
}

const STRENGTH_LABELS: readonly string[] = STRENGTHS.map(strengthLabel);

/** Pure. Add agent:candidate and strength:<s>; remove any other strength:* label in `current`. */
export const candidateLabelChange = (strength: Strength, current: readonly string[]): LabelChange => {
  const keep = strengthLabel(strength);
  return {
    add: [CANDIDATE_LABEL, keep],
    remove: current.filter((l) => STRENGTH_LABELS.includes(l) && l !== keep),
  };
};

/** The `gh issue edit` arguments that make `issue` a candidate of `strength`, replace its body and drop `agent:spec`. */
export const specEditArgs = (
  issue: string,
  bodyFile: string,
  strength: Strength,
  current: readonly string[],
): string[] => {
  const change = candidateLabelChange(strength, current);
  return [
    "issue", "edit", issue, "--body-file", bodyFile,
    "--remove-label", "agent:spec",
    ...change.add.flatMap((l) => ["--add-label", l]),
    ...change.remove.flatMap((l) => ["--remove-label", l]),
  ];
};
