import * as fs from "node:fs";
import * as path from "node:path";
import { z } from "zod";
import { gh } from "../pr-review/github-reads";
import { buildReport, type Issue, type Pr } from "./report";

/** Reads issues and PRs with `gh` and writes report.md. No model runs here, so the job needs no secret. */

const labels = z.array(z.object({ name: z.string() })).transform((l) => l.map((x) => x.name));

const issuesSchema = z.array(
  z.object({
    number: z.number(),
    state: z.enum(["OPEN", "CLOSED"]),
    labels,
    createdAt: z.string(),
    updatedAt: z.string(),
  }),
);

const prsSchema = z.array(
  z.object({
    number: z.number(),
    state: z.enum(["OPEN", "CLOSED", "MERGED"]),
    headRefName: z.string(),
    labels,
    createdAt: z.string(),
  }),
);

// Well above a week of Factory traffic; recovery looks at `agent:recovered` issues of any age, so it needs the tail.
const LIMIT = "1000";

const issues: Issue[] = issuesSchema.parse(
  JSON.parse(gh(["issue", "list", "--state", "all", "--limit", LIMIT, "--json", "number,state,labels,createdAt,updatedAt"])),
);
const prs: Pr[] = prsSchema.parse(
  JSON.parse(gh(["pr", "list", "--state", "all", "--limit", LIMIT, "--json", "number,state,headRefName,labels,createdAt"])),
);

const outDir = process.env.OUTPUT_DIR ?? ".";
fs.mkdirSync(outDir, { recursive: true });
const report = buildReport(issues, prs, new Date());
fs.writeFileSync(path.join(outDir, "report.md"), report);
console.log(report);
