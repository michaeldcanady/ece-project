# Architecture candidates (backlog)

Deepening candidates from the architecture review of 2026-11-25. Candidate 1 (deepen the
hour-partitioned lake table) is done on branch `refactor/lake-table`; these are queued for later, in the
suggested order.

## 2. Move the envelope into the contract (worth exploring, in-process)

**Files:** `citibike_contract/__init__.py`, `citibike_producer/envelope.py` (`make_envelope`, `encode`),
`citibike_producer/replay.py` (`env["replay"]`, `env["replayed_at"]`, `_key_for`),
`citibike_pipeline/validation.py` (`parse_envelope`), `citibike_pipeline/batch.py` (`parse_records`,
`build_status_rows`).

**Problem:** the contract package pins down topics and timestamps but not the envelope. Its fields are
written in `make_envelope`, extended in place by `replay` (`replay`, `replayed_at`), and read back with
string keys across `batch.py` (about 12 `env.get(...)` calls, `parse_iso(env["replayed_at"])` twice).
A renamed field fails silently as `None`.

**Direction:** give `citibike_contract` one envelope module (stdlib only, so the producer image can
still import it) that the producer and the pipeline both build, encode and decode through: building,
marking as replayed, encoding, and decoding to an envelope or reason codes. The Kafka key per event
type (`replay._key_for`) moves with it. Payload validation stays in the pipeline.

**Tests:** a producer→pipeline round-trip test at the contract interface; `batch.py` stops parsing
timestamps.

## 3. Give the micro-batch step records a real interface (worth exploring, in-process)

**Files:** `citibike_pipeline/batch.py` (`plan_batch`, `process_batch`, `compute_metrics`,
`finalize_batch`), `dags/citibike_micro_batch.py`, `tests/test_pipeline_e2e.py` (`run_batch`, `drain`).

**Problem:** the plan, stats (14 keys) and publish outputs are the real interface between the steps,
yet they are bare dicts that are serialised to XCom and re-read by key name. `finalize_batch` copies
11 stats keys by name. Their order, the `has_data` gate (`plan["total_messages"]`) and the commit-last
rule are written out twice, once in the DAG and once in the test helper `run_batch`.

**Direction:** keep one Airflow task per step so retries stay per step. The batch module would own
typed step records (which serialise themselves for XCom) and a driver for the step order, which both
the DAG and the tests use.

**Tests:** crash-retry tests drive the same step order the DAG uses; a missing stats key fails loudly.

## 4. Serving-snapshot module (speculative, local-substitutable)

**Files:** `citibike_pipeline/batch.py` (`compute_metrics`, `finalize_batch`),
`citibike_pipeline/status.py` (`summarize`), `citibike_pipeline/lake.py` (`SERVING_*`).

**Problem:** the serving-file rules live inside two batch steps: they are written last (after
history), `as_of` never moves backwards, and the batch log is a rolling 24 h table deduped by
`batch_id`. Readers (`status.py`, the dashboard) rebuild the file set from `lake.SERVING_*` keys.

**Direction:** move the serving files behind one publish/read module.

**Caveat:** the deletion test is weak (one writer, one reader). Revisit after candidate 1 lands; it
may fall out of the lake-table work.
