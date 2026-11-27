# Coding standards

Read during review. Each rule says what good looks like; `mise run lint` already enforces the mechanical parts.

## Strong types

Every value gets its exact type, or a custom class written for it. `scripts/check_strong_types.py` rejects
`Any`, `typing.Dict` and the builtin `dict` in annotations, in every file. `scripts/check_mypy.py` runs mypy with a per-file error baseline that only shrinks, so
annotations are checked against the code that uses them. Review for what the checks cannot see:

- **Records are classes.** A value with named fields (a parsed JSON object, a step record, a function's
  options) is a frozen `@dataclass`. A `TypedDict` fits only where the value must stay a real `dict`, such as
  one passed to `json.dumps` unchanged. A class built just to satisfy the check while code still reads it by
  string key is a finding.
- **Parse at the boundary.** Bytes, HTTP bodies, capture lines and config are parsed into their class once,
  where they enter, the way `citibike_contract.decode` returns an `Envelope`. Code past the boundary reads
  attributes.
- **Closed sets are types.** Event types, outcomes and reason codes are a `Literal[...]` or an `Enum`.
- **Lookups are Mappings.** A map with open keys (topic to partition, station id to its latest value) is a
  `Mapping[K, V]` with exact types, never a `dict`; a map with a fixed set of keys is a record.

## Idempotent

See `.claude/rules/idempotent.md`: the same data in gives the same results out. Review a change against it.

## File layout

See `.claude/rules/file-layout.md`: one type per file, and each function with its type or in a module named for its utility. Review a change against it.

## Design principles

See `.claude/rules/design-principles.md`: layered dependencies, refactors kept apart, ADRs, a small public surface.
Review a change against it.
