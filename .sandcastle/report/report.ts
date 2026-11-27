/** Turns fetched issues and PRs into the weekly Factory report. Pure: fetch.ts does the reading. */

export interface Issue {
  readonly number: number;
  readonly state: "OPEN" | "CLOSED";
  readonly labels: readonly string[];
  readonly createdAt: string;
  readonly updatedAt: string;
}

export interface Pr {
  readonly number: number;
  readonly state: "OPEN" | "CLOSED" | "MERGED";
  readonly headRefName: string;
  readonly labels: readonly string[];
  readonly createdAt: string;
}

export const WINDOW_DAYS = 7;

const BRANCH = /^agent\/issue-(\d+)/;

/** The issue number a Factory branch (`agent/issue-N...`) belongs to, or undefined for any other branch. */
export const factoryIssue = (pr: Pr): number | undefined => {
  const match = BRANCH.exec(pr.headRefName);
  return match === null ? undefined : Number(match[1]);
};

const within = (iso: string, since: Date): boolean => new Date(iso).getTime() >= since.getTime();

const percent = (part: number, whole: number): string =>
  whole === 0 ? "n/a" : `${Math.round((100 * part) / whole)}%`;

export interface RecoveryCounts {
  readonly ended: number;
  readonly blocked: number;
  readonly other: number;
}

/**
 * Where issues that carry `agent:recovered` stand now. `ended` has a merged or open Factory PR, `blocked` carries
 * `agent:blocked` and has no such PR, `other` is everything else (still running, or its PR was closed unmerged).
 */
export const recovery = (issues: readonly Issue[], prs: readonly Pr[], since: Date): RecoveryCounts => {
  const withPr = new Set(prs.filter((p) => p.state !== "CLOSED").flatMap((p) => factoryIssue(p) ?? []));
  const recovered = issues.filter((i) => i.labels.includes("agent:recovered") && within(i.updatedAt, since));
  const ended = recovered.filter((i) => withPr.has(i.number)).length;
  const blocked = recovered.filter((i) => !withPr.has(i.number) && i.labels.includes("agent:blocked")).length;
  return { ended, blocked, other: recovered.length - ended - blocked };
};

export const buildReport = (issues: readonly Issue[], prs: readonly Pr[], now: Date): string => {
  const since = new Date(now.getTime() - WINDOW_DAYS * 24 * 60 * 60 * 1000);
  const filed = issues.filter((i) => within(i.createdAt, since));
  const human = filed.filter((i) => i.labels.includes("request")).length;
  const scan = filed.filter((i) => i.labels.includes("agent:candidate") && !i.labels.includes("request")).length;

  const factory = prs.filter((p) => factoryIssue(p) !== undefined && within(p.createdAt, since));
  const merged = factory.filter((p) => p.state === "MERGED").length;
  const closed = factory.filter((p) => p.state === "CLOSED").length;
  const open = factory.filter((p) => p.state === "OPEN").length;
  const oneWay = factory.filter((p) => p.labels.includes("door:one-way")).length;
  const reviewed = factory.filter((p) => p.labels.includes("door:one-way") || p.labels.includes("door:two-way")).length;

  const blocked = issues.filter((i) => i.state === "OPEN" && i.labels.includes("agent:blocked"));
  const rec = recovery(issues, prs, since);
  const recovered = rec.ended + rec.blocked + rec.other;

  const day = (d: Date): string => d.toISOString().slice(0, 10);
  return [
    `## Factory weekly report, ${day(since)} to ${day(now)}`,
    "",
    "### Issues filed",
    `- By humans (label \`request\`): ${human}`,
    `- By the nightly scan (\`agent:candidate\` without \`request\`): ${scan}`,
    "",
    "### Factory pull requests opened",
    `- Opened (branch \`agent/issue-*\`): ${factory.length}`,
    `- Merged: ${merged}`,
    `- Closed unmerged: ${closed}`,
    `- Still open: ${open}`,
    "",
    "### Blocked now",
    `- Open issues labeled \`agent:blocked\`: ${blocked.length}` +
      (blocked.length === 0 ? "" : ` (${blocked.map((i) => `#${i.number}`).join(", ")})`),
    "",
    "### Recovery (approximate)",
    `- Issues labeled \`agent:recovered\` and updated in the window: ${recovered}`,
    `- With a merged or open Factory PR: ${rec.ended}`,
    `- Still blocked: ${rec.blocked}`,
    `- Other (running, or the PR closed unmerged): ${rec.other}`,
    `- Recovery rate: ${percent(rec.ended, recovered)}`,
    "",
    "### Review door",
    `- PRs labeled \`door:one-way\`: ${oneWay} of ${factory.length} (${percent(oneWay, factory.length)})`,
    `- PRs the reviewer has labeled either way: ${reviewed} of ${factory.length}`,
    "",
    "### What is approximate",
    "- GitHub keeps no label history here, so recovery counts the label as it stands now. An issue whose label was removed is missed, and \"updated in the window\" stands in for \"recovered in the window\".",
    "- PR outcomes are the state now of the PRs opened in the window, not what happened in the window.",
    "- Cost per PR is not reported: the data is not available.",
    "",
  ].join("\n");
};
