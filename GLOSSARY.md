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

**Batch log**:
The record of micro-batch runs: one entry per batch, with its counts and latencies.
_Avoid_: run log, ops log

**Kafka identity**:
The (topic, partition, offset) triple that names the message a row came from; no two rows of a lake
table share one.
_Avoid_: row id, key

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

**Replay**:
Re-sending a captured envelope through Kafka later. A replayed envelope keeps its original fetch time
and also records when it was replayed.
_Avoid_: backfill, resend

**Kafka key**:
The key an envelope is sent under: the station for station events, the location for weather, the job
for poll logs. It decides the partition, and on the station information topic, which envelope Kafka keeps.
_Avoid_: Kafka identity (that names a stored message; the key groups messages)
