# CLAUDE.md

- **Commands** are mise tasks (`mise tasks` lists them). Check your work with `mise run check`, which runs lint,
  the strong-types check and the tests, the same as CI and the pre-commit hook.
- **Types:** `Any` and `dict` are rejected by `mise run lint`; use the exact type or a dataclass.
- **Reviewing** a change: apply `CODING_STANDARDS.md`.
- **DAG tests** run against the sibling `airflow` container's REST API (`tests/dag_catalog.py`) when Airflow is
  not installed; a skip names the reason (`-rs`). After editing a DAG file, wait ~30 s for the container to re-parse.
- **Specs and backlog:** `docs/specs/` holds the specs; a spec's Delivery section lists the commits that implement it
  (no issue tracker). `docs/architecture-candidates.md` is the backlog of open refactors.
