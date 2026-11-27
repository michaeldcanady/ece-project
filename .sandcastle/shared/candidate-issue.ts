import type { Strength } from "./candidate-labels";

/**
 * The Candidate issue body: the layout of a candidate issue's body and the only parser of it. The spec comes
 * first, then a `---` rule and the footer of the run that wrote it, then the Original note if there is one.
 * Pure: strings in, strings out.
 */

/** What the scan needs to know about an existing candidate issue. Structurally the element type of existingSchema. */
export interface FiledIssue {
  readonly number: number;
  readonly title: string;
  readonly state: string;
  readonly body: string;
}

const RULE = "\n\n---\n";

/** The body of a new issue filed by the nightly improve run. */
export const filedBody = (spec: string, strength: Strength): string => {
  const next =
    strength === "strong"
      ? "This candidate is `strong`, so it is labeled `agent:implement` automatically."
      : "Add the `agent:implement` label to have it implemented.";
  return `${spec}${RULE}Filed by the nightly improve run. ${next}\n`;
};

/** The body of an issue the agent:spec run expanded, keeping its original note at the bottom. */
export const speccedBody = (spec: string, strength: Strength, originalNote: string): string =>
  `${spec}${RULE}Specced by the \`agent:spec\` workflow as \`strength:${strength}\`. Add the \`agent:implement\` label to have it implemented.\n\n<details><summary>Original note</summary>\n\n${originalNote}\n\n</details>\n`;

/** The spec's Files section (or a migrated note's **Files:** line), trimmed and cut to 600 characters; "" if absent. */
export const filesOf = (body: string): string =>
  (
    body.match(/^## Files\n([\s\S]*?)(?=^## |\n---|$(?![\s\S]))/m)?.[1] ??
    body.match(/^\*\*Files:\*\*(.*(?:\n(?!\n).*)*)/m)?.[1] ?? // migrated notes
    ""
  )
    .trim()
    .slice(0, 600);

/** The ALREADY FILED prompt argument: one line per issue with its Files, or "(none yet)". */
export const alreadyFiledList = (issues: readonly FiledIssue[]): string =>
  issues.length === 0
    ? "(none yet)"
    : issues
        .map((i) => `- #${i.number} [${i.state}] ${i.title}\n  Files: ${filesOf(i.body).replace(/\s+/g, " ") || "(not stated)"}`)
        .join("\n");
