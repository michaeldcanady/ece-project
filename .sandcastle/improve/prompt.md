# TASK

Find architecture **deepening opportunities** in this repository and write each one up as an implementable spec.
You are running unattended: no one will answer questions, so make the decisions yourself and record them in the
spec. Do not edit, commit or push anything; this run is read-only. Your only output is the final `<output>` tag.

# METHOD

Follow the `improve-codebase-architecture` method, headless:

1. Read `CLAUDE.md`, `GLOSSARY.md`, `CODING_STANDARDS.md`, `.claude/rules/*` and any `docs/adr/` first. ADRs record
   decisions you must not re-litigate.
2. Find the hot spots: walk back through `git log --oneline` and let the paths that keep changing pull your
   attention first. Widen the net only if the changes are scattered.
3. Explore those areas organically and note friction: understanding one concept needs many small modules; modules
   whose interface is nearly as complex as the implementation (**shallow**); pure functions extracted only for
   testing while the real bugs hide in how they are called; tightly coupled modules leaking across a seam; code that
   is untested or hard to test through its interface.
4. Apply the **deletion test** to every suspect: would deleting the module concentrate complexity, or just move it?
   Keep only the ones where it concentrates.
5. Use the vocabulary exactly: **module, interface, depth, seam, adapter, leverage, locality** (the `codebase-design`
   skill) for architecture, and `GLOSSARY.md` terms for the domain. Do not drift into "service", "component" or
   "boundary". One adapter is a hypothetical seam; two is a real one.

`docs/architecture-candidates.md`, if it still exists, is the old backlog: treat its entries as already filed.

# ALREADY FILED

Do not propose any of these again, open or closed. A closed one was rejected.

{{ALREADY_FILED}}

# OUTPUT

Pick at most **3** candidates, strongest first, and **none** if nothing deserves `explore` or better. A quiet night
is a fine result; padding the list is not. Rate each:

- `strong`: real friction, a real seam, clear leverage, low risk to the tests that exist
- `explore`: a plausible gain that needs a judgment call the implementer must make
- `speculative`: drop it; do not output speculative candidates

Each candidate's `body` is the spec, in markdown, with exactly these sections:

- `## Problem`: the friction, with file paths and function names. Cite lines you actually read.
- `## Files`: the modules involved.
- `## Proposal`: the deepened module in domain and architecture vocabulary. Name it. Sketch its interface (names and
  types, not bodies). Say what sits behind the seam and what moves.
- `## Benefits`: in terms of locality and leverage, and how the tests improve.
- `## Tests`: which existing tests survive unchanged, which are replaced, and what new test is written through the
  new interface.
- `## Acceptance criteria`: a checklist, each item verifiable, ending with "`mise run check` passes".
- `## Out of scope`: what an implementer must not touch.

The spec must obey `CODING_STANDARDS.md` and `.claude/rules/*` (strong types, one type per file, layered
dependencies, refactors kept apart from behavior changes). Make the change small enough for one pull request; split
a large one and file only the first step.

Finish with the `<output>` tag only when the specs are complete.
