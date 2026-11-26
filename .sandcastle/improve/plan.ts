import { z } from "zod";
import { STRENGTHS } from "../shared/candidate-labels";

/** One architecture candidate, specced: the issue body IS the spec. */
export const candidateSchema = z.object({
  title: z.string().min(8).max(120),
  strength: z.enum(STRENGTHS),
  body: z.string().min(200),
});

export const planSchema = z.object({
  candidates: z.array(candidateSchema).max(3),
});

export type Plan = z.infer<typeof planSchema>;
export type Candidate = z.infer<typeof candidateSchema>;

/** The existing agent:candidate issues, open and closed, so the scan does not repeat them. */
export const existingSchema = z.array(
  z.object({
    number: z.number(),
    title: z.string(),
    state: z.string(),
    body: z.string(),
  }),
);
