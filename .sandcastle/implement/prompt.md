# TASK

Implement issue #{{ISSUE_NUMBER}} on branch `{{BRANCH}}`, already created from `main`. You are unattended: decide,
do not ask. The issue is the spec; stay inside its scope and its "Out of scope" section.

# ISSUE

{{ISSUE_CONTEXT}}

If a comment above says a previous run could not finish this issue, this is the recovery attempt and the last one.
Read that comment, find its cause, and fix that cause first. An earlier branch is not available: start from `main`.

# CONTEXT

Read before editing: `CLAUDE.md`, `GLOSSARY.md`, `CODING_STANDARDS.md`, `.claude/rules/*`, and `docs/adr/` if
present. Then read the code and tests the issue names.

# EXECUTION

1. Red-green-refactor where a test seam exists or the issue proposes one: write a failing test through the new
   interface, make it pass with the smallest correct change, then refactor. Do not invent test seams the issue does
   not call for.
2. Keep refactors apart from behavior changes, one type per file, strong types (no `Any`, no `dict`).
3. Run `mise run check` and get it green before you finish. It is the gate: the run fails and no PR opens if it is
   red. The DAG tests may skip when the Airflow container is absent; that is expected.
4. Commit in small steps with clear messages on `{{BRANCH}}`.

Do not push, open or edit PRs, touch issue labels, or modify `.github/`, `.sandcastle/`, `CLAUDE.md`, or
`CODING_STANDARDS.md`.

When the acceptance criteria are met and `mise run check` is green, output `<promise>COMPLETE</promise>`.

# WRITING STYLE

{{WRITING_STYLE}}
