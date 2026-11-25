# Design principles: a rule, a layer check and an audit

Source: the Kempner Institute handbook, "Software Design Principles" (sections 6.1–6.13), as agreed in the
grilling of 2026-11-25. Vocabulary: `GLOSSARY.md` and the `codebase-design` skill. Standards: `CODING_STANDARDS.md`.

## Problem Statement

The pipeline's code is meant to follow the handbook's design principles, but only some of them are written down or
checked. Idempotence, typed records and one type per file are rules. The rest, such as which module may depend on which,
are habits. A change can pass lint and the tests while a pure core module starts importing a module that does I/O (for
example, Enrichment importing the Batch log module, which pulls in the lake and storage). Nobody finds out until a
reviewer notices, or a test needs a Kafka or storage fake that a pure module should never have needed. A person adding
code, or an agent working on it, has no single place that says which parts of the handbook apply here and which are
already covered.

## Solution

Three things, in two commits:

1. A project rule that holds only the principles the repo does not already state, and points at the handbook for the
   rest (KISS, DRY, YAGNI, single responsibility, design patterns). The reviewer and the implementer both read it.
2. A layer check in `mise run lint` that fails when a module imports from a higher layer. It starts with a baseline of
   today's violations that may only shrink, so it can land without a rewrite.
3. An audit of the current code against the handbook, written into the architecture backlog as candidates. Fixing them
   is separate work, taken one candidate at a time.

## User Stories

1. As a maintainer, I want one rule that lists the design principles this repo follows, so that I do not have to
   re-read the handbook for every review.
2. As a maintainer, I want the rule to hold only what the repo does not already say, so that each principle has one
   source and the rules do not drift apart.
3. As a maintainer, I want the rule to cite the handbook by section for the principles it leaves out, so that I can
   find the full text when I need it.
4. As an implementer, I want the rule loaded when I work on pipeline, producer, DAG or script code, so that I see it
   at the moment it applies.
5. As a reviewer, I want a pointer from the coding standards to the rule, so that I apply it during review.
6. As a maintainer, I want dependencies to point one way, toward a core that does no I/O, so that I can test the
   core with values alone.
7. As a maintainer, I want the layers named (contract, core, infrastructure, orchestration and entry), so that "which
   layer is this module in" has one answer.
8. As a developer, I want `mise run lint` to fail when a module imports from a higher layer, so that the mistake never
   reaches review.
9. As a developer, I want the failure to name the importing module, the imported module and both layers, so that I can
   fix it without reading the check's code.
10. As a developer, I want imports within a layer and imports of lower layers to pass, so that ordinary code is not
    flagged.
11. As a developer, I want the check to start with a baseline of existing violations, so that it can land without a
    large rewrite.
12. As a developer, I want a violation outside the baseline to fail the check, so that no new ones are added.
13. As a developer, I want the check to tell me to lower the baseline when I fix a violation, so that the gain cannot
    be spent again.
14. As a developer, I want the check to add no dependency, so that the dev environment stays as small as it is.
15. As a maintainer, I want the check to follow the same pattern as the strong-types check, so that I learn one way
    of keeping a ratchet.
16. As a maintainer, I want the layer map in one place at the top of the check, so that changing a module's layer is
    a one-line edit.
17. As a maintainer, I want the producer package to have its own chain (contract, sources and sink, jobs, tasks), so
    that its layering is checked too.
18. As a maintainer, I want a new module that is in no layer to fail the check, so that nothing escapes the map by
    being forgotten.
19. As a reviewer, I want refactor commits kept apart from behaviour changes, so that I can review each for what it
    claims to be.
20. As a reviewer, I want documentation and glossary edits in their own commit, so that a refactor commit holds only
    code that changes no behaviour.
21. As a maintainer, I want significant design decisions recorded as ADRs (context, decision, consequences), so that
    the reasoning outlives the conversation.
22. As a maintainer, I want the ADR directory created when the first ADR is written, so that no empty structure sits in
    the repo.
23. As an implementer, I want the default to be functions plus dataclasses, with a class added only when shared,
    updated state justifies it, so that I do not reach for inheritance by habit.
24. As an implementer, I want internals prefixed with an underscore and a package's public names listed in `__all__`,
    so that the public surface stays small and stable.
25. As an implementer, I want new outputs to carry the config and code version that produced them, so that a result can
    be traced to its inputs.
26. As a maintainer, I want the audit to list every place the code departs from the handbook, so that I can choose
    what to fix and in what order.
27. As a maintainer, I want each audit finding to name the files, the problem and a direction, so that it can be
    picked up as its own change.
28. As a maintainer, I want the audit findings in the existing architecture backlog, so that there is one list of open
    work.
29. As a maintainer, I want fixing the findings kept out of this change, so that the rule and the check land in a
    small, reviewable change.
30. As an agent working on the repo, I want the rule's scope declared, so that it loads for code and not for every
    conversation.
31. As a maintainer, I want the check's behaviour covered by tests that build small module trees, so that I can change
    the scanner without fear.
32. As a maintainer, I want one test that runs the check on the real repository, so that a new violation fails the
    build even if the unit tests pass.

## Implementation Decisions

- **The rule** holds the net-new principles only: layered dependencies pointing toward a core that does no I/O;
  refactor commits that change no behaviour and stay separate from behaviour changes and from documentation edits;
  ADRs for significant design decisions; functions plus dataclasses as the default, a class only for shared, updated
  state; underscore-prefixed internals and `__all__` for public names; new outputs carry the config and code version.
  It cites the handbook by section for KISS, DRY, YAGNI, single responsibility and the patterns, and does not restate
  them.
- **The rule is scoped** to the contract, pipeline, producer, DAG and script code, like the other rules in the repo.
  The coding standards get a two-line pointer to it.
- **Layers for the pipeline package**, lowest first. A module may import from its own layer or a lower one.
  1. Contract: the wire contract package.
  2. Core, pure values and logic: Station status and Weather observation parsing, the curated rows, Enrichment, the
     Snapshot, the window metrics, the time helpers, and the Counts record once it has its own file.
  3. Infrastructure, adapters over Kafka, storage and the lake: the storage adapters, the Kafka reader, the lake
     table, the schemas, the Batch log and the serving files.
  4. Orchestration and entry: the Batch steps and driver, the daily quality report, the status check, the station
     dimension build, the runtime wiring, then the DAGs.
- **Layers for the producer package:** contract, then the sources and the sink, then the jobs, then the task entry
  points.
- **The layer check** is a script using the standard library's parser, run by `mise run lint` next to the strong-types
  check. It holds the layer map and the baseline in plain data at the top of the file. It reads each module's imports
  (absolute and relative), resolves them to modules in the map, and reports every import from a higher layer. It
  also reports a module that is in no layer. It exits 1 on any report.
- **The baseline** records today's violations by importing module and imported module. A violation not in the
  baseline fails. A baseline entry that no longer occurs also fails, telling the developer to remove it. The
  baseline only shrinks.
- **The audit's findings**, entered in the architecture backlog: move the Counts record into its own file; split the
  pipeline config into a pure value and its environment loader; split the station dimension build by layer, joining
  the existing Station dimension candidate; record config and code version with each Batch log entry.
- **Delivery:** commit one is the rule, the coding-standards pointer and the backlog entries (documentation only).
  Commit two is the layer check, its baseline, its tests and the lint wiring. Each commit passes the full check.

## Testing Decisions

- A good test crosses the check's entry point: a source root, the layer map and the baseline in, an exit code and
  printed report out. It does not reach into the scanner's internals.
- One seam: the layer check's entry point, the same surface `mise run lint` uses. No other module is tested for this
  change.
- Tests build small module trees under a temporary directory and assert on the exit code and the report: an import of
  a lower layer passes; an import within a layer passes; an import of a higher layer fails and names both modules and
  both layers; a violation in the baseline passes; a baseline entry that no longer occurs fails; a module in no
  layer fails; a relative import and an absolute import resolve to the same module.
- One test runs the check against the real repository and expects exit 0, so a new violation outside the baseline
  fails the build.
- The rule, the coding-standards pointer and the backlog entries are documentation and have no tests.
- Prior art: the end-to-end tests cross the public interface with real inputs and assert on outputs. The strong-types
  check has no test of its own; its ratchet message is the pattern this check follows.

## Out of Scope

- Fixing any violation the audit finds, including moving Counts, splitting the config and splitting the station
  dimension build. Each is its own change.
- Changing the Batch log schema or the serving files, so recording config and code version with each Batch log entry
  stays a backlog candidate.
- Splitting existing multi-type files under the file-layout rule. That rule applies to files that are created or
  changed.
- A third-party import-linting package.
- Enforcing the rule's judgement items (functions plus dataclasses, public surface, ADRs) with a deterministic check.
- A reproducibility feature beyond the Idempotent rule already in place.

## Further Notes

- The handbook is a guide for research code, and the pipeline is production-shaped. The handbook's "do not over-layer a
  small script" applies: the layer map has four layers because the code already splits that way, not because the
  handbook lists four.
- The layer check is the one mechanical part. Everything else in the rule is judgement and goes to the reviewer, as the
  retro skill advises.
- The first ADR, once one is written, is a good place to record why `schemas.py` stays the source of truth for column
  types (decided in the typed-rows work).
