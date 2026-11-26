# CLAUDE.md

- **Commands** are mise tasks (`mise tasks` lists them). Check your work with `mise run check`, which runs lint,
  the strong-types check and the tests, the same as CI and the pre-commit hook.
- **Types:** `Any` and `dict` are rejected by `mise run lint`; use the exact type or a dataclass.
- **Reviewing** a change: apply `CODING_STANDARDS.md`.
- **DAG tests** run against the sibling `airflow` container's REST API (`tests/dag_catalog.py`) when Airflow is
  not installed; a skip names the reason (`-rs`). After editing a DAG file, wait ~30 s for the container to re-parse.
- **Specs and backlog:** the backlog of open refactors is the GitHub issues labeled `agent:candidate`. The nightly
  `improve` workflow files them, each issue is its own spec, and the PR that closes it is its Delivery record.
  Adding `agent:implement` to an issue starts the `implement` workflow (`.sandcastle/`, set up by
  `mise run agent:setup`). `docs/specs/` holds the earlier specs; a spec's Delivery section lists the commits that
  implement it. `docs/architecture-candidates.md` is the old backlog, replaced by those issues once migrated.
