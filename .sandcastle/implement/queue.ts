import { PRIORITY_HIGH_LABEL, REQUEST_LABEL } from "../shared/labels";

/** One open issue labeled `agent:implement`, in the shape `gh issue list --json number,createdAt,labels` returns. */
export interface QueuedIssue {
  number: number;
  createdAt: string;
  labels: { name: string }[];
}

const has = (issue: QueuedIssue, label: string): boolean => issue.labels.some((l) => l.name === label);

/** Sort key, larger first: `priority:high`, then a human request. */
const rank = (issue: QueuedIssue): number => Number(has(issue, PRIORITY_HIGH_LABEL)) * 2 + Number(has(issue, REQUEST_LABEL));

/**
 * Orders the implement queue: `priority:high` first, then human-filed issues (`request`) before the nightly scan's
 * candidates, then the oldest issue. The issue number breaks a tie so the order is the same on every run.
 */
export const orderQueue = (issues: readonly QueuedIssue[]): QueuedIssue[] =>
  [...issues].sort(
    (a, b) => rank(b) - rank(a) || Date.parse(a.createdAt) - Date.parse(b.createdAt) || a.number - b.number,
  );

/** The issue to implement next, or undefined when the queue is empty. */
export const nextIssue = (issues: readonly QueuedIssue[]): number | undefined => orderQueue(issues)[0]?.number;
