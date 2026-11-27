# CLAUDE.md

- **Commands** are mise tasks (`mise tasks` lists them). Check your work with `mise run check`, which runs lint
  (ruff, strong types, mypy, layers, actionlint), the `.sandcastle/` typecheck and the tests, the same as CI and the
  pre-commit hook.
- **Node tooling** (`.sandcastle/`) runs through mise (`mise run typecheck:agent`, `mise run test:agent`); there is
  no bare `npm` on the PATH.
- **Reading an issue:** `gh issue view N --json title,body,comments`; `--comments` alone can print nothing.
- **Types:** `Any` and `dict` are rejected by `mise run lint`; use the exact type or a dataclass.
- **Reviewing** a change: apply `CODING_STANDARDS.md`.
- **DAG tests** run against the sibling `airflow` container's REST API (`tests/dag_catalog.py`) when Airflow is
  not installed; a skip names the reason (`-rs`). After editing a DAG file, wait ~30 s for the container to re-parse.
- **Specs and backlog:** the backlog is the GitHub issues labeled `agent:candidate`; each issue is its own spec and
  the PR that closes it is its Delivery record. How the nightly agent workflows work: `.sandcastle/README.md`.
  `docs/specs/` holds the earlier specs; a spec's Delivery section lists the commits that implement it.
  `docs/architecture-candidates.md` is the old backlog, replaced by those issues once migrated.
- **Committing** while the main tree holds unrelated uncommitted work (the hook runs the whole repo's check): commit
  from a `git worktree add`, which has its own `.venv`.
