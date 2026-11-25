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

The same data in gives the same results out. Run the pipeline again on the same input, such as the test capture, and
every output (lake rows, keys, serving files, Batch log entry, report) comes out equal; a rerun overwrites, it never
adds a second copy. Review for what a passing test can miss:

- **Outputs are a function of the input.** A key, id or row value derives from the data (Kafka identity, batch id,
  event time), never from the wall clock, a random id, a counter or the order a task happened to run. Where a time is
  needed, it is a parameter (`now`, `processed_at`) so a test can pin it.
- **Writes overwrite by key.** A step that can run twice (an Airflow retry, a rerun after a failed batch) writes the same
  keys with the same bytes. Append-only writes, and anything that must be undone before a rerun, are a finding.
- **Order is fixed.** Rows are sorted by a deterministic key before they are flagged or written, so the arrival order of
  records does not change a result.
- **Commit last.** The step that consumes input (the offset commit) runs after every output exists, so a crash leaves
  work to redo, not work to skip.
- **A test shows it.** A change to a step that reads or writes the lake adds or keeps a test that runs the same input
  twice and compares the outputs (see `test_retry_after_crash_before_commit_is_idempotent`).
