/** Turns fetched issues and PRs into the weekly Factory report. Pure: fetch.ts does the reading. */
import { BLOCKED_LABEL, DOOR_ONE_WAY_LABEL, DOOR_TWO_WAY_LABEL, RECOVERED_LABEL, RECOVERED_OK_LABEL, REQUEST_LABEL } from "../shared/labels";
import { CANDIDATE_LABEL } from "../shared/candidate-labels";

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

const WINDOW_DAYS = 7;

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
  readonly withPr: number;
  readonly blocked: number;
  readonly other: number;
}

/**
 * Where issues that carry `agent:recovered` stand now. `withPr` has a merged or open Factory PR, `blocked` carries
 * `agent:blocked` and has no such PR, `other` is everything else (still running, or its PR was closed unmerged).
 */
export const recovery = (issues: readonly Issue[], prs: readonly Pr[], since: Date): RecoveryCounts => {
  const issuesWithPr = new Set(prs.filter((p) => p.state !== "CLOSED").flatMap((p) => factoryIssue(p) ?? []));
  const recovered = issues.filter(
    (i) => (i.labels.includes(RECOVERED_LABEL) || i.labels.includes(RECOVERED_OK_LABEL)) && within(i.updatedAt, since),
  );
  const withPrCount = recovered.filter((i) => issuesWithPr.has(i.number) || i.labels.includes(RECOVERED_OK_LABEL)).length;
  const blocked = recovered.filter((i) => !issuesWithPr.has(i.number) && !i.labels.includes(RECOVERED_OK_LABEL) && i.labels.includes(BLOCKED_LABEL)).length;
  return { withPr: withPrCount, blocked, other: recovered.length - withPrCount - blocked };
};

interface Figures {
  readonly since: Date;
  readonly now: Date;
  readonly human: number;
  readonly scan: number;
  readonly opened: number;
  readonly merged: number;
  readonly closed: number;
  readonly open: number;
  readonly oneWay: number;
  readonly doorLabeled: number;
  readonly blocked: readonly Issue[];
  readonly recovery: RecoveryCounts;
}

const figures = (issues: readonly Issue[], prs: readonly Pr[], now: Date): Figures => {
  const since = new Date(now.getTime() - WINDOW_DAYS * 24 * 60 * 60 * 1000);
  const filed = issues.filter((i) => within(i.createdAt, since));
  const factoryPrs = prs.filter((p) => factoryIssue(p) !== undefined && within(p.createdAt, since));
  const count = (kept: readonly Pr[], pred: (p: Pr) => boolean): number => kept.filter(pred).length;
  return {
    since,
    now,
    human: filed.filter((i) => i.labels.includes(REQUEST_LABEL)).length,
    scan: filed.filter((i) => i.labels.includes(CANDIDATE_LABEL) && !i.labels.includes(REQUEST_LABEL)).length,
    opened: factoryPrs.length,
    merged: count(factoryPrs, (p) => p.state === "MERGED"),
    closed: count(factoryPrs, (p) => p.state === "CLOSED"),
    open: count(factoryPrs, (p) => p.state === "OPEN"),
    oneWay: count(factoryPrs, (p) => p.labels.includes(DOOR_ONE_WAY_LABEL)),
    doorLabeled: count(factoryPrs, (p) => p.labels.includes(DOOR_ONE_WAY_LABEL) || p.labels.includes(DOOR_TWO_WAY_LABEL)),
    blocked: issues.filter((i) => i.state === "OPEN" && i.labels.includes(BLOCKED_LABEL)),
    recovery: recovery(issues, prs, since),
  };
};

const day = (d: Date): string => d.toISOString().slice(0, 10);

const render = (f: Figures): string => {
  const rec = f.recovery;
  const recovered = rec.withPr + rec.blocked + rec.other;
  return [
    `## Factory weekly report, ${day(f.since)} to ${day(f.now)}`,
    "",
    "### Issues filed",
    `- By humans (label \`request\`): ${f.human}`,
    `- By the nightly scan (\`agent:candidate\` without \`request\`): ${f.scan}`,
    "",
    "### Factory pull requests opened",
    `- Opened (branch \`agent/issue-*\`): ${f.opened}`,
    `- Merged: ${f.merged}`,
    `- Closed unmerged: ${f.closed}`,
    `- Still open: ${f.open}`,
    "",
    "### Blocked now",
    `- Open issues labeled \`agent:blocked\`: ${f.blocked.length}` +
      (f.blocked.length === 0 ? "" : ` (${f.blocked.map((i) => `#${i.number}`).join(", ")})`),
    "",
    "### Recovery (approximate)",
    `- Issues labeled \`agent:recovered\` and updated in the window: ${recovered}`,
    `- With a merged or open Factory PR: ${rec.withPr}`,
    `- Still blocked: ${rec.blocked}`,
    `- Other (running, or the PR closed unmerged): ${rec.other}`,
    `- Recovery rate: ${percent(rec.withPr, recovered)}`,
    "",
    "### Review door",
    `- PRs labeled \`door:one-way\`: ${f.oneWay} of ${f.opened} (${percent(f.oneWay, f.opened)})`,
    `- PRs the reviewer has labeled either way: ${f.doorLabeled} of ${f.opened}`,
    "",
    "### What is approximate",
    "- GitHub keeps no label history here, so recovery counts the label as it stands now. An issue whose label was removed is missed, and \"updated in the window\" stands in for \"recovered in the window\".",
    "- PR outcomes are the state now of the PRs opened in the window, not what happened in the window.",
    "- Cost per PR is not reported: the data is not available.",
    "",
  ].join("\n");
};

export const buildReport = (issues: readonly Issue[], prs: readonly Pr[], now: Date): string =>
  render(figures(issues, prs, now));
