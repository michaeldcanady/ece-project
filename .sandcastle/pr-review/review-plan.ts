import { z } from "zod";
import { doorSchema } from "./door";

/** What the review agent emits. review-actions.ts decides which of it GitHub may receive. */
export const reviewSchema = z.object({
  summary: z.string().min(1),
  door: doorSchema,
  comments: z
    .array(z.object({ path: z.string(), line: z.number().int().positive(), body: z.string().min(1) })),
  resolved: z.array(z.object({ threadId: z.string(), reply: z.string().min(1) })),
});

export type Review = z.infer<typeof reviewSchema>;
