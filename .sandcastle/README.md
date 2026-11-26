# Agent workflows

Nightly architecture improvement, built on Sandcastle (`@ai-hero/sandcastle`). TypeScript, kept apart from the
Python project; `mise run typecheck:agent` checks it and runs as part of `mise run check`.

## Flow

1. `.github/workflows/improve.yml` scans the repo, specs up to 3 candidates and files each as an issue labeled
   `agent:candidate` plus `strength:strong` or `strength:explore`. It runs nightly; a manual run defaults to a dry
   run, which writes the plan to the run summary and files nothing.
2. A `strong` candidate is labeled `agent:implement` by that same run, so it goes straight to step 3. Any other
   candidate waits: a human adds `agent:implement` to start it.
3. `.github/workflows/implement.yml`, started by that label, implements the spec, runs `mise run check`, runs a
   review pass, runs the check again, then opens a PR that closes the issue. Reviewing the PR is the human gate.
4. A failure at any point leaves the issue labeled `agent:blocked` with the reason as a comment. No PR opens.

A thin issue (a short note, such as a migrated backlog entry) is not implementable. Adding `agent:spec` runs
`.github/workflows/spec.yml`, which expands it into the full spec and replaces the issue body, keeping the note
under "Original note". It also rates the spec and labels the issue `agent:candidate` plus one `strength:*`, so a
specced issue joins the backlog and the scan's dedupe. It never adds `agent:implement`: its job holds only
`GITHUB_TOKEN`, and a human decides. `implement.yml` refuses an issue with no `## Acceptance criteria` section and says to run
`agent:spec` first. The section format lives in one place, `shared/spec-format.md`.

A pull request is reviewed by `.github/workflows/pr-review.yml` on every push to it, in three jobs like `spec.yml`
(`fetch` reads, `review` runs the model with only the OAuth token, `post` writes). It posts one `COMMENT` review with
inline comments, and replies to and resolves its own earlier threads that the new code addresses. It never approves or
requests changes, and it skips drafts and fork PRs. Each review states whether the head is a one-way or two-way
door. Which writes are allowed is decided in one pure module, `pr-review/review-actions.ts`.

Labels move `agent:candidate` (a spec run adds it, with a `strength:*`) > (`agent:spec`) > `agent:implement` > `agent:in-progress` > `agent:pr-open`, or `agent:blocked`.

## Facts a file will not tell you

- The agent holds only `CLAUDE_CODE_OAUTH_TOKEN`. Pushing and the PR use `AGENT_PAT` in their own steps, and a
  separate job with `issues: write` files issues. The PAT has no Workflows permission on purpose.
- `improve.yml`'s apply job also gets `AGENT_PAT`, only to add `agent:implement` to strong candidates. A label added
  with `GITHUB_TOKEN` would not start `implement.yml`. The agent job never sees it.
- The agent may not change `.github/`, `.sandcastle/`, `CLAUDE.md`, `CODING_STANDARDS.md`, `.claude/` or
  `.githooks/`: `implement/implement.ts` fails the run if the diff touches them.
- The GitHub runner is the sandbox (`noSandbox()`), so there is no Docker image.
- DAG tests skip on the runner (no Airflow container); the PR body says so.
- The scan dedupes against every existing `agent:candidate` issue, open or closed, by title and by the files its
  `## Files` section names.
- The PR reviewer posts as `github-actions[bot]`, a shared author, so it recognises its own threads by the
  `REVIEW_MARKER` comment in the body. A head sha it already reviewed (`sha=<sha>` in a marked review) is skipped.
- The `post` job of `pr-review.yml` has `contents: write` only because resolving a review thread needs it.
- `pr-review.yml` and `.sandcastle/pr-review/` are covered by `PROTECTED`, so a human must change them.
- A PAT, not `GITHUB_TOKEN`, so that `check.yml` runs on the PR without an approval click.

## Setup

`mise run agent:setup` in a real terminal: secrets, labels, backlog issues, first dry run.
