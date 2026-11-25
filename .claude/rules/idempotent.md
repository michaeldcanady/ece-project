---
paths:
  - "citibike_pipeline/**"
  - "citibike_producer/**"
  - "dags/**"
  - "tests/**"
---

# Idempotent

The same data in gives the same results out. Run the pipeline again on the same input, such as the test capture, and
every output (lake rows, keys, serving files, Batch log entry, report) comes out equal. A rerun overwrites; it never
adds a second copy.

- **Outputs are a function of the input.** A key, id or row value derives from the data (Kafka identity, batch id,
  event time), never from the wall clock, a random id, a counter or the order a task happened to run. Where a time is
  needed, it is a parameter (`now`, `processed_at`) so a test can pin it.
- **Writes overwrite by key.** A step that can run twice (an Airflow retry, a rerun after a failed batch) writes the same
  keys with the same bytes. Append-only writes, and anything that must be undone before a rerun, are a bug.
- **Order is fixed.** Sort rows by a deterministic key before flagging or writing them, so the arrival order of records
  does not change a result.
- **Commit last.** The step that consumes input (the offset commit) runs after every output exists, so a crash leaves
  work to redo, not work to skip.
- **Show it with a test.** A change to a step that reads or writes the lake adds or keeps a test that runs the same
  input twice and compares the outputs (see `test_retry_after_crash_before_commit_is_idempotent`).
