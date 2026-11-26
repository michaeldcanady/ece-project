# Agent workflows

Nightly architecture improvement, built on Sandcastle (`@ai-hero/sandcastle`). TypeScript, kept apart from the
Python project; `mise run typecheck:agent` checks it and runs as part of `mise run check`.

## Flow

1. `.github/workflows/improve.yml` scans the repo, specs up to 3 candidates and files each as an issue labeled
   `agent:candidate` plus `strength:strong` or `strength:explore`. Manual runs default to a dry run, which writes
   the plan to the run summary and files nothing. The cron line is commented out until a dry run looks right.
2. A human adds `agent:implement` to an issue. `.github/workflows/implement.yml` implements the spec, runs
   `mise run check`, runs a review pass, runs the check again, then opens a PR that closes the issue.
3. A failure at any point leaves the issue labeled `agent:blocked` with the reason as a comment. No PR opens.

Labels move `agent:candidate` > `agent:implement` > `agent:in-progress` > a PR, or `agent:blocked`.

## Facts a file will not tell you

- The agent holds only `CLAUDE_CODE_OAUTH_TOKEN`. Pushing and the PR use `AGENT_PAT` in their own steps, and a
  separate job with `issues: write` files issues. The PAT has no Workflows permission on purpose.
- The agent may not change `.github/`, `.sandcastle/`, `CLAUDE.md`, `CODING_STANDARDS.md`, `.claude/` or
  `.githooks/`: `implement/implement.ts` fails the run if the diff touches them.
- The GitHub runner is the sandbox (`noSandbox()`), so there is no Docker image.
- DAG tests skip on the runner (no Airflow container); the PR body says so.
- The scan dedupes against existing `agent:candidate` issues by title only.
- A PAT, not `GITHUB_TOKEN`, so that `check.yml` runs on the PR without an approval click.

## Setup

`mise run agent:setup` in a real terminal: secrets, labels, backlog issues, first dry run.
