/** One open issue labeled `agent:implement`, in the shape `gh issue list --json number,createdAt,labels` returns. */
export interface QueuedIssue {
  number: number;
  createdAt: string;
  labels: { name: string }[];
}

const has = (issue: QueuedIssue, label: string): boolean => issue.labels.some((l) => l.name === label);

/**
 * Orders the implement queue: `priority:high` first, then human-filed issues (`request`) before the nightly scan's
 * candidates, then the oldest issue. The issue number breaks a tie so the order is the same on every run.
 */
export const orderQueue = (issues: readonly QueuedIssue[]): QueuedIssue[] =>
  [...issues].sort(
    (a, b) =>
      Number(has(b, "priority:high")) - Number(has(a, "priority:high")) ||
      Number(has(b, "request")) - Number(has(a, "request")) ||
      Date.parse(a.createdAt) - Date.parse(b.createdAt) ||
      a.number - b.number,
  );

/** The issue to implement next, or undefined when the queue is empty. */
export const nextIssue = (issues: readonly QueuedIssue[]): number | undefined => orderQueue(issues)[0]?.number;
