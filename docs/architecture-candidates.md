# Architecture candidates (backlog)

Open deepening candidates from the architecture reviews of 2026-11-25. Done work lives in git history and
`docs/specs/` (hour-partitioned lake table, envelope contract, `docs/specs/batch-interface.md`: typed Batch
steps and the Batch log module). Vocabulary: `GLOSSARY.md` and the `codebase-design` skill.

## 1. Split `compute_metrics` into a pure snapshot and a publish (worth exploring, in-process)

**Files:** `citibike_pipeline/batch.py` (`compute_metrics`, ~100 lines), `citibike_pipeline/metrics.py`,
`citibike_pipeline/lake.py` (`SERVING_*`), `citibike_pipeline/status.py`.

**Problem:** the serving-file rules (written last, after history; `as_of` never moves backwards) share one
function with the metric calculation, so testing a metric needs a lake and Kafka-shaped batches. Readers
(`status.py`, the dashboard) rebuild the serving file set from `lake.SERVING_*` keys.

**Direction:** a pure snapshot calculation over a status table, plus one publish/read module that owns the
serving files.

**Caveat:** the deletion test is weak for the serving module alone (one writer, one reader); the pure split
carries most of the value.

## 2. Put the Poll log payload in the contract (worth exploring, in-process)

**Files:** `citibike_producer/jobs.py` (`Job._finish`), `citibike_pipeline/validation.py`
(`validate_poll_log_payload`), `citibike_pipeline/quality.py` (`_poll_log_section`),
`citibike_contract/envelope.py`.

**Problem:** the Envelope is typed, but the Poll log payload it carries is a 15-key dict written in
`_finish` (nine parameters) and read by string key in two pipeline modules. Outcomes are loose strings.

**Direction:** a `PollLog` type in `citibike_contract` (stdlib only) with a `Literal` outcome, built by the jobs and
decoded once on the pipeline side.

**Tests:** a producer to pipeline round trip at the contract interface.

## 3. Parse the batch into typed observation rows once (speculative, in-process)

**Files:** `citibike_pipeline/batch.py` (`parse_records`, `build_status_rows`, `_weather_rows`),
`citibike_pipeline/schemas.py`, `quality.py` (`STATUS_COLS`), `batch.py` (`METRIC_COLS`).

**Problem:** every column name is spelled in the schema, the 40-key row dict and three column lists.

**Direction:** one row class that derives the schema and the column lists.

**Caveat:** one writer per table, so the seam is hypothetical; a wide change for a modest gain.
