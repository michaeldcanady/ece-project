import { z } from "zod";
import { STRENGTHS } from "../shared/candidate-labels";

/** The expanded spec for one issue; apply.ts replaces the issue body with it and labels it a candidate of that strength. */
export const specSchema = z.object({
  strength: z.enum(STRENGTHS),
  body: z.string().min(300).includes("## Acceptance criteria"),
});

export type Spec = z.infer<typeof specSchema>;
