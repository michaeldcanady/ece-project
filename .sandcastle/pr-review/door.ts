import { z } from "zod";

/** Reversibility of a change: a one-way door is hard to undo, a two-way door is easy to. */
export const DOORS = ["one-way", "two-way"] as const;
export const doorSchema = z.object({ kind: z.enum(DOORS), reason: z.string().min(1) });
export type Door = z.infer<typeof doorSchema>;

/** The review-body line for a door. Pure. */
export const renderDoor = (door: Door): string => `**Door: ${door.kind}.** ${door.reason}`;
