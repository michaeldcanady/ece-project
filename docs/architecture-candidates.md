# Architecture candidates (backlog)

Open deepening candidates from the architecture reviews of 2026-11-25. Done work lives in git history and
`docs/specs/` (hour-partitioned lake table, envelope contract, `docs/specs/batch-interface.md`: typed Batch
steps and the Batch log module; `citibike_pipeline/snapshot.py` and `serving.py`: the pure Snapshot and the
serving files). Vocabulary: `GLOSSARY.md` and the `codebase-design` skill.

## 1. Put the Poll log payload in the contract (worth exploring, in-process)

**Files:** `citibike_producer/jobs.py` (`Job._finish`), `citibike_pipeline/validation.py`
(`validate_poll_log_payload`), `citibike_pipeline/quality.py` (`_poll_log_section`),
`citibike_contract/envelope.py`.

**Problem:** the Envelope is typed, but the Poll log payload it carries is a 15-key dict written in
`_finish` (nine parameters) and read by string key in two pipeline modules. Outcomes are loose strings.

**Direction:** a `PollLog` type in `citibike_contract` (stdlib only) with a `Literal` outcome, built by the jobs and
decoded once on the pipeline side.

**Tests:** a producer to pipeline round trip at the contract interface.

## 2. Parse the batch into typed observation rows once (speculative, in-process)

**Files:** `citibike_pipeline/batch.py` (`parse_records`, `build_status_rows`, `_weather_rows`),
`citibike_pipeline/schemas.py`, `quality.py` (`STATUS_COLS`), `snapshot.py` (`STATUS_ROW_COLS`).

**Problem:** every column name is spelled in the schema, the 40-key row dict and three column lists.

**Direction:** one row class that derives the schema and the column lists.

**Caveat:** one writer per table, so the seam is hypothetical; a wide change for a modest gain.
