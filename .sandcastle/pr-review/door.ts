import { z } from "zod";

/** Reversibility of a change: a one-way door is hard to undo, a two-way door is easy to. */
export const DOORS = ["one-way", "two-way"] as const;

export const doorSchema = z.enum(DOORS);
export type Door = z.infer<typeof doorSchema>;

/** Create both door labels (idempotent: `--force`), so a repo that has not rerun `setup.sh` still works. */
export const ensureDoorLabels = (gh: (args: string[]) => string): void => {
  gh(["label", "create", "door:one-way", "--force", "--description", "PR review: hard to reverse", "--color", "b60205"]);
  gh(["label", "create", "door:two-way", "--force", "--description", "PR review: easy to reverse", "--color", "0e8a16"]);
};

/** The label that states a door, and the one it replaces. Written out so `check_agent_labels.py` sees both. */
export const doorLabels = (door: Door): { readonly add: string; readonly remove: string } =>
  door === "one-way"
    ? { add: "door:one-way", remove: "door:two-way" }
    : { add: "door:two-way", remove: "door:one-way" };
