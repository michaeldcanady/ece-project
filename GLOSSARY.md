# Citi Bike pipeline

A streaming pipeline that polls Citi Bike station feeds and weather, carries them through Kafka, and
builds a lake of station observations and station-availability metrics.

## The lake

**Lake**:
The object store (a local directory or an S3 prefix) holding every table, log and serving file the
pipeline produces.
_Avoid_: bucket, data dir, warehouse

**Lake table**:
A named collection of Kafka-sourced rows, each identified by its Kafka identity and split into hour
partitions by the row's event time. The raw and curated station-status, weather and poll-log data are
lake tables.
_Avoid_: dataset, prefix (a prefix is just where a lake table lives)

**Hour partition**:
The slice of a lake table whose event times fall in one UTC hour.
_Avoid_: bucket, folder

**Compaction**:
Merging an hour partition's per-batch files into one file once the hour is old enough, without
losing or duplicating a row.
_Avoid_: merge, cleanup

**Batch**:
The offset ranges one micro-batch reads and processes, identified by its batch id. A batch can span several
Airflow runs when one fails and a later run takes it over.
_Avoid_: run, job

**Batch id**:
A hash of a batch's planned offset ranges. Every rerun of the same ranges writes the same keys, so a rerun
overwrites instead of duplicating.
_Avoid_: run id (that names an Airflow run, not the data)

**Plan**:
A batch's offset ranges, fixed before anything is read and reused until its offsets are committed.
_Avoid_: schedule

**Batch log**:
The record of micro-batch runs: one entry per batch, with its counts and latencies.
_Avoid_: run log, ops log

**Station status**:
One station's bike and dock counts as the feed reported them, parsed from a station-status envelope's payload.
A payload that fails its parse is rejected with reason codes and never becomes a Station status.
_Avoid_: message, record (a record is what Kafka stores), observation row (that is the curated row it enriches into)

**Weather observation**:
The current weather at a location, parsed from a weather envelope's payload.
_Avoid_: message, record, reading

**Station information**:
One station's fixed details (name, location, capacity), parsed from a station-information envelope's payload.
The hourly build keeps the latest per station, and Enrichment attaches it to each Station status.
_Avoid_: dimension, station metadata

**Enrichment**:
The step that turns a Batch's Station statuses into curated rows: it drops duplicate deliveries, flags repeats,
out-of-order, stale and unknown-station observations, and attaches the station dimension and the latest weather
at or before each observation. Pure: it reads no storage.
_Avoid_: transform, processing

**Quality report**:
The daily summary of one UTC day's data health: poll outcomes per job, rejected envelopes by reason, and how
stale, repeated and empty the station data was. Written as `report.json` and `report.md`.
_Avoid_: data report, daily report

**Kafka identity**:
The (topic, partition, offset) triple that names the message a row came from; no two rows of a lake
table share one.
_Avoid_: row id, key

**Serving file**:
A lake file overwritten every batch for the dashboard to read: current station state, at-risk stations, the
system summary and the recent Batch log. Written after the batch's history, so a reader never sees a
batch that history lacks.
_Avoid_: output file, gold file

**Snapshot**:
The station states, windows and system availability computed for one batch at one as-of time. A snapshot
is a value; publishing it is what writes the serving files.
_Avoid_: metrics (that names the individual measures), report

**Live check**:
A run against the running stack's real data that reprocesses Kafka's retained messages into a separate lake and
reports, per check, whether the cleaning and the serving files hold. It reads the live lake and never writes it.
_Avoid_: smoke test, integration test (the in-memory run over a synthetic capture is the end-to-end test)

## Service level

**Service failure**:
A station that cannot serve a rider right now: it has no bike to rent, no dock to return to, or is offline.
_Avoid_: outage, empty station (EMPTY is one cause of a failure, not the whole set)

**At risk**:
A station that still works but is projected to become empty or full within the risk horizon. An early warning,
not yet a Service failure.
_Avoid_: failing, critical

**Service level**:
The share of all stations that can both rent and return a bike right now: online, with at least one bike and
one free dock. At-risk stations count as served; they are reported beside it, not inside it.
_Avoid_: availability (that names the separate shares of stations with a bike or with a dock), uptime

**Area**:
A roughly one-kilometre cell of the map grouping nearby stations, used to rank where Service failures
concentrate. Not an administrative region.
_Avoid_: region (the station feed's region code is too coarse to rank), neighbourhood, zone

**Rebalance**:
Moving bikes by crew from stations or Areas with a surplus to those with a shortage.
_Avoid_: redistribution, restocking

**Risk horizon**:
How far ahead, in minutes, a station's trend is projected when deciding whether it is At risk.
_Avoid_: forecast window, lookahead

## The wire contract

**Envelope**:
The message the producer sends for every observation, poll or replayed record: its event type, provenance
(source, poll, fetch time, producer version) and the payload it carries.
_Avoid_: message, event, record (a record is what Kafka stores; an envelope is what it contains)

**Event type**:
What an envelope's payload describes: a station's status, a station's information, current weather, or a
poll log. Each event type has its own topic.
_Avoid_: kind, message type

**Poll**:
One fetch of a source by a producer job; every envelope it yields shares one poll id.
_Avoid_: request, scrape

**Poll log**:
The envelope a job sends after each poll, recording its outcome whether it succeeded or not.
_Avoid_: heartbeat, job log

**Outcome**:
How a poll ended, one of a fixed set the contract defines. Only an ok poll and a poll skipped because the feed
was unchanged count as successes.
_Avoid_: status, result

**Replay**:
Re-sending a captured envelope through Kafka later. A replayed envelope keeps its original fetch time
and also records when it was replayed.
_Avoid_: backfill, resend

**Kafka key**:
The key an envelope is sent under: the station for station events, the location for weather, the job
for poll logs. It decides the partition, and on the station information topic, which envelope Kafka keeps.
_Avoid_: Kafka identity (that names a stored message; the key groups messages)

## The agent workflows

**Factory**:
The whole loop from an issue to a reviewed pull request: intake, spec, implementation and review. A human reviews the
pull request; nothing merges without that.
_Avoid_: pipeline (that names the data pipeline), agent workflows (that names its parts)

**Door**:
How reversible a change is. A one-way door cannot be undone by reverting it; a two-way door can.
_Avoid_: risk, impact

**Spec**:
A GitHub issue body that says what to build and how to check it. The issue is its own spec; the pull request that
closes it records the delivery.
_Avoid_: ticket, design doc

**Strength**:
How much of a spec's work an agent can do unattended, set by the `strength:` label on the issue.
_Avoid_: size, priority
