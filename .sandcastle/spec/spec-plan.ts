import { z } from "zod";

/** The expanded spec for one issue; apply.ts replaces the issue body with it. */
export const specSchema = z.object({
  body: z.string().min(300).includes("## Acceptance criteria"),
});

export type Spec = z.infer<typeof specSchema>;
