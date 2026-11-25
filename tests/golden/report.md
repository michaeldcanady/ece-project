# Data quality report, 2026-10-20 (UTC)

**Verdict: FAIL**

## Stream completeness
- station_status polls logged: 40 of 1440 expected (0.0278)
- gaps over 150s: 0 (total 0s)
- outcomes: {'ok': 40}

## Observations
- rows: 1600 across 40 stations
- stale share (threshold 30.0 min): 0.025
- repeat share: 0.0244, out-of-order: 0, unknown station rows: 40
- age CDF (seconds -> share of rows at or below): {'60': 0.975, '300': 0.975, '900': 0.975, '1800': 0.975, '3600': 0.975, '7200': 0.975, '21600': 1.0}

## Pipeline
- batches: 2, DLQ rows: 40, dropped delivery duplicates: 0
- latency (s): {'median_of_batch_p50': 126.0, 'p95_of_batch_p95': 2345.0, 'max': 2405.0}
- DLQ reasons: {'bad_num_bikes_available': 40}

## Most frequently empty stations
- st-002: 1.0 of observations, 40
- st-000: 0.15 of observations, 6
