# CLAUDE.md

- **Commands** are mise tasks (`mise tasks` lists them). Check your work with `mise run check`, which runs lint,
  the strong-types check and the tests, the same as CI and the pre-commit hook.
- **Types:** `Any` and `dict` are rejected by `mise run lint`; use the exact type or a dataclass.
- **Reviewing** a change: apply `CODING_STANDARDS.md`.
