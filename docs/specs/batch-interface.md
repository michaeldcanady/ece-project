# Batch interface and Batch log

Candidates 3 and 2 of `docs/architecture-candidates.md`, as agreed in the architecture review of 2026-11-25.
Vocabulary: `GLOSSARY.md` (Batch, Batch id, Plan, Batch log). Standards: `CODING_STANDARDS.md`.

## Ticket 1: typed Batch interface (blocks ticket 2)

Files: `citibike_pipeline/batch.py`, `dags/citibike_micro_batch.py`, `tests/test_pipeline_e2e.py`,
`tests/test_dags.py`, `citibike_pipeline/lake.py`, `scripts/check_strong_types.py`.

- `Plan`, `Stats`, `Published` become frozen dataclasses owned by `batch.py`, each with `to_json` and
  `from_json` so they survive Airflow XCom. They keep today's JSON keys, minus `status_keys`, so in-flight XCom
  values and the plan files under `ops/batch_plan/` stay readable. A missing field fails when the record is built.
- `plan_batch`, `process_batch`, `compute_metrics`, `finalize_batch` take and return these records.
- One driver function runs the steps in order: plan, stop if the plan has no data, process, publish, finalize
  with the commit last. `Plan.has_data` replaces `plan["total_messages"] > 0`. No test-only flag on the driver.
- The DAG keeps one Airflow task per step (retries stay per step) and `short_circuit`, using `Plan.has_data`.
  One test asserts the DAG's task order matches the driver's step order.
- Tests: happy-path and drain tests go through the driver. Crash-retry tests call the typed step functions up
  to the crash point, then the driver to prove the retry. Replace the test helper `run_batch`'s copy of the order.
- Delete the `status_keys` branch in `batch_latencies`, `lake.hours_from_legacy_keys`, and the test covering them.
- Lower the weak-annotation count of every touched file in `BASELINE` of `scripts/check_strong_types.py`.

## Ticket 2: Batch log module (after ticket 1)

Files: new `citibike_pipeline/batch_log.py`, `batch.py` (`finalize_batch`), `quality.py` (`_batch_section`),
`status.py` (`summarize`), `lake.py`, tests.

- `BatchLogEntry`: a frozen dataclass separate from `Stats`; `finalize_batch` builds it from `Stats` plus the
  plan times and latencies.
- A small class over `Storage`: `record(entry)` writes the per-batch file and updates the rolling 24 h serving
  copy (dedupe by batch id); `recent()` returns the last 24 h of entries; `day(date)` returns a UTC day's entries.
  Readers get `BatchLogEntry` objects, not dicts.
- `BATCH_LOG` and `SERVING_BATCH_LOG` keys move from `lake.py` into `batch_log.py`; `schemas.BATCH_LOG` stays.
- `finalize_batch`, `status.summarize` and `quality._batch_section` use it; the hand-built key listing for the
  Batch log goes away.

## Out of scope

The DLQ (its readers in `quality.py` stay), `parse_records`, `build_status_rows`, `_weather_rows`, and the metric
calculation inside `compute_metrics` (which only changes to take `Plan`/`Stats` and return `Published`).

## Done when

`mise run check` passes, and the behaviour tests (plan stability, crash retry, no duplicate rows, compaction
safety) still pass through the new interface.
