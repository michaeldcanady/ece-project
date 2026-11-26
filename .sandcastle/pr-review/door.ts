import { z } from "zod";

/** Reversibility of a change: a one-way door is hard to undo, a two-way door is easy to. */
export const DOORS = ["one-way", "two-way"] as const;

export const doorSchema = z.enum(DOORS);
export type Door = z.infer<typeof doorSchema>;

/**
 * Create a door label only if the repo lacks it, so a repo that has not rerun `setup.sh` still works. `setup.sh` owns
 * their description and colour; this never overwrites them and writes nothing once the labels exist.
 */
export const ensureDoorLabels = (gh: (args: string[]) => string): void => {
  const existing = new Set(gh(["label", "list", "--limit", "1000", "--json", "name", "--jq", ".[].name"]).split("\n"));
  for (const name of ["door:one-way", "door:two-way"]) {
    if (!existing.has(name)) gh(["label", "create", name]);
  }
};

/** The label that states a door, and the one it replaces. Written out so `check_agent_labels.py` sees both. */
export const doorLabels = (door: Door): { readonly add: string; readonly remove: string } =>
  door === "one-way"
    ? { add: "door:one-way", remove: "door:two-way" }
    : { add: "door:two-way", remove: "door:one-way" };
