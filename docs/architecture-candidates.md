# Architecture candidates (backlog)

Open deepening candidates from the architecture reviews of 2026-11-25. Done work lives in git history and
`docs/specs/` (hour-partitioned lake table, envelope contract, `docs/specs/batch-interface.md`: typed Batch
steps and the Batch log module; `citibike_pipeline/snapshot.py` and `serving.py`: the pure Snapshot and the
serving files; `citibike_contract/poll_log.py`: the typed Poll log payload and Outcome). Vocabulary: `GLOSSARY.md` and the `codebase-design` skill.

## 1. Parse the batch into typed observation rows once (speculative, in-process)

**Files:** `citibike_pipeline/batch.py` (`parse_records`, `build_status_rows`, `_weather_rows`),
`citibike_pipeline/schemas.py`, `quality.py` (`STATUS_COLS`), `snapshot.py` (`STATUS_ROW_COLS`).

**Problem:** every column name is spelled in the schema, the 40-key row dict and three column lists.

**Direction:** one row class that derives the schema and the column lists.

**Caveat:** one writer per table, so the seam is hypothetical; a wide change for a modest gain.
