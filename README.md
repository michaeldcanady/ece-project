# citibike_producer

Polls Citi Bike GBFS 2.3 and Open-Meteo and publishes enveloped events to Kafka. Python 3.9+.
The producer forwards payloads as received. Validation, dedup, and enrichment belong to the Airflow micro-batch.

## Run

Dependencies are managed with [uv](https://docs.astral.sh/uv/). `uv.lock` is committed; `.python-version` pins 3.9.

```bash
uv sync                                               # creates .venv with the locked dependencies
export CB_BOOTSTRAP_SERVERS=localhost:9092
uv run citibike-create-topics --replication 1         # once
uv run citibike-producer live                         # poll live feeds
uv run citibike-producer replay sample.jsonl.gz --speed 60
uv run pytest                                         # no network or broker needed
```

Add a dependency with `uv add <pkg>`, a dev-only one with `uv add --dev <pkg>`. In a container, use
`uv sync --frozen --no-dev` so the build uses exactly the locked versions.

Run it under a supervisor (`restart: unless-stopped` in compose, or systemd) so a crash or reboot resumes capture.

## Topics

| Topic | Key | Cadence | Notes |
|---|---|---|---|
| `citibike.station_status` | station_id | 30 s | one message per station per poll |
| `citibike.station_info` | station_id | 60 min | compacted |
| `weather.current` | location id | 15 min | one Manhattan coordinate |
| `pipeline.poll_log` | job name | every poll | outcome, latency, feed ttl, delivery counts |
| `pipeline.dlq` | | | written by Airflow, not the producer |

## Envelope (schema_version 1)

`schema_version, event_type, poll_id, source, source_url, gbfs_version, feed_last_updated (source epoch s),
fetched_at (UTC ingest time, ms), producer_version, payload`. Replay adds `replay: true` and `replayed_at`.

## poll_log outcomes

`ok`, `skipped_unchanged` (feed `last_updated` did not advance, nothing published), `timeout`,
`connection_error`, `http_error`, `parse_error`, `discovery_error`, `kafka_error`.
Gaps in `poll_log` itself mean the producer was down.

## Behavior worth knowing

- Feed URLs come from `gbfs.json` discovery, refreshed every 6 h and on 404/410.
- Kafka: idempotent producer, `acks=all`, lz4. Each poll flushes and reports delivered and failed counts.
- A failed poll retries after 30 s, doubling to a 120 s cap, and never exceeds the job interval.
- A `kafka_error` poll is not marked as seen, so the next poll republishes it. Downstream dedup must key on `(station_id, last_reported)`, since the retry carries a new `poll_id`.
- Replay keeps original `fetched_at` (event time) but lets the broker stamp the record time. Old CreateTime values would make time-based retention delete replayed data.
- `CB_TEE_PATH=/path/capture.jsonl.gz` also writes every envelope locally. Use it to cut the sample capture for the repo.

## Config (env)

`CB_BOOTSTRAP_SERVERS, CB_STATUS_INTERVAL_S (30), CB_INFO_INTERVAL_S (3600), CB_WEATHER_INTERVAL_S (900),
CB_WEATHER_LAT/LON, CB_SKIP_UNCHANGED_FEED (true), CB_MAX_BACKOFF_S (120), CB_TEE_PATH, GIT_SHA` (recorded as `producer_version`).
