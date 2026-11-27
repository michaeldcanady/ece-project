import * as fs from "node:fs";
import * as path from "node:path";
import { z } from "zod";
import { outputDir } from "../shared/common";
import { gh } from "../shared/gh";
import { labelNames as labels } from "../shared/labels";
import { buildReport, type Issue, type Pr } from "./report";

/** Reads issues and PRs with `gh` and writes report.md. No model runs here, so the job needs no secret. */

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

const outDir = outputDir();
fs.mkdirSync(outDir, { recursive: true });
const report = buildReport(issues, prs, new Date());
fs.writeFileSync(path.join(outDir, "report.md"), report);
console.log(report);
