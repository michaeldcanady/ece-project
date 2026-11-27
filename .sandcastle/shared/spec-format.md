A markdown spec with exactly these sections, in this order:

- `## Problem`: the friction, with file paths and function names. Cite lines you actually read.
- `## Files`: the modules involved.
- `## Proposal`: the deepened module in domain and architecture vocabulary. Name it. Sketch its interface (names and
  types, not bodies). Say what sits behind the seam and what moves.
- `## Benefits`: in terms of locality and leverage, and how the tests improve.
- `## Tests`: which existing tests survive unchanged, which are replaced, and what new test is written through the
  new interface.
- `## Acceptance criteria`: a checklist, each item verifiable, ending with "`mise run check` passes". For a bug,
  the first item is a failing test that reproduces it.
- `## Out of scope`: what an implementer must not touch.

The spec must obey `CODING_STANDARDS.md` and `.claude/rules/*` (strong types, one type per file, layered
dependencies, refactors kept apart from behavior changes). Make the change small enough for one pull request; split
a large one and spec only the first step.
