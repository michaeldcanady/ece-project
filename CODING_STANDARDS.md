# Coding standards

Read during review. Each rule says what good looks like; `mise run lint` already enforces the mechanical parts.

## Strong types

Every value gets its exact type, or a custom class written for it. `scripts/check_strong_types.py` rejects
`Any`, `typing.Dict` and the builtin `dict` in annotations; files written before the rule may only lower their
count in its `BASELINE`. Review for what the check cannot see:

- **Records are classes.** A value with named fields (a parsed JSON object, a step record, a function's
  options) is a frozen `@dataclass`. A `TypedDict` fits only where the value must stay a real `dict`, such as
  one passed to `json.dumps` unchanged. A class built just to satisfy the check while code still reads it by
  string key is a finding.
- **Parse at the boundary.** Bytes, HTTP bodies, capture lines and config are parsed into their class once,
  where they enter, the way `citibike_contract.decode` returns an `Envelope`. Code past the boundary reads
  attributes.
- **Closed sets are types.** Event types, outcomes and reason codes are a `Literal[...]` or an `Enum`.
- **Touched code moves forward.** A change to a function signature in a `BASELINE` file types that signature,
  and lowers the file's count.

## Idempotent

See `.claude/rules/idempotent.md`: the same data in gives the same results out. Review a change against it.
