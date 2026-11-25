# Architecture candidates (backlog)

Open deepening candidates from the architecture reviews of 2026-11-25. Done work lives in git history and
`docs/specs/` (hour-partitioned lake table, envelope contract, `docs/specs/batch-interface.md`: typed Batch
steps and the Batch log module; `citibike_pipeline/snapshot.py` and `serving.py`: the pure Snapshot and the
serving files; `citibike_contract/poll_log.py`: the typed Poll log payload and Outcome;
`observations.py`, `enrichment.py` and `rows.py`: Station status and Weather observation parsed once, pure Enrichment, typed curated rows). Vocabulary: `GLOSSARY.md` and the `codebase-design` skill.

## 1. Poll: one module for "poll once, publish, log the outcome" (strong)

**Files:** `citibike_producer/jobs.py` (`Job._finish`, both `run_once`), `tasks.py` (`poll_once`), `dags/_common.py`
(`run_poll`), `sources.py` (`FeedRegistry`).

**Problem:** `GbfsStationJob.run_once` and `WeatherJob.run_once` repeat one protocol (fetch, skip-if-unchanged, send,
flush, check delivery, mark seen, `_finish`); `_finish` takes 11 parameters and mutates the failure counters. `poll_once`
returns `{"ok", "outcome", "error", "state"}` and `run_poll` reads it by string key; the carry-over state is a dict.

**Direction:** a Poll module that owns the protocol, with a small adapter per source (feed url, response to envelopes);
a typed `PollResult` and `ProducerState` with `to_json`/`from_json`. Two adapters already exist, so the seam is real.

## 2. Station dimension: one module for build, key and load (worth exploring)

**Files:** `citibike_pipeline/station_info.py` (`build_dim_station`), `batch.py` (`_load_dim`), `lake.py`
(`DIM_STATION_CURRENT`), `schemas.py` (`DIM_STATION`).

**Problem:** the dimension has no glossary term and its knowledge is split: `station_info` hand-writes the
`snapshot_dt=` key, `_load_dim` returns `Dict[str, Dict[str, Any]]` and enrichment reads it with `d.get("name")`.

**Direction:** a Station dimension module with `build` and `load -> StationDimension | None`; the empty-snapshot rule
stays inside it. Modest seam (one writer, one reader): a locality gain more than leverage.

## 3. Daily quality report as a typed value (worth exploring)

**Files:** `citibike_pipeline/quality.py`, `dags/citibike_daily_quality.py`.

**Problem:** `daily_quality` builds a nested `Dict[str, Any]`; `verdict` and `render_markdown` read it by string path,
and `BatchSection` is typed only to be flattened back by `to_json`.

**Direction:** a frozen `DailyReport` of section records; `verdict`, the Markdown and one `to_json` take it.

## 4. Batch step records: a codec for the Airflow hand-off (speculative)

**Files:** `citibike_pipeline/batch.py` (`Plan`, `Stats`, `Published`, `STEPS`), `dags/citibike_micro_batch.py`.

**Problem:** each DAG task repeats `runtime.build()` and `X.from_json`, and `STEPS` duplicates the DAG order by comment.

**Caveat:** DAG tests already pin the order, in-flight XCom shapes constrain the codec, and there is no second
orchestrator, so the seam is hypothetical. Do not do yet.
