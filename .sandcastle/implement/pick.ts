import * as fs from "node:fs";
import { z } from "zod";
import { required } from "../shared/common";
import { nextIssue } from "./queue";

/** Print the number of the next issue to implement, or nothing when the queue is empty. */

const queueSchema = z.array(
  z.object({
    number: z.number().int(),
    createdAt: z.string(),
    labels: z.array(z.object({ name: z.string() })),
  }),
);

const queue = queueSchema.parse(JSON.parse(fs.readFileSync(required("QUEUE_FILE"), "utf8")));
const next = nextIssue(queue);
if (next !== undefined) console.log(next);
