# Spec: move the envelope into the contract

Status: `ready-for-agent` · Backlog candidate 2 in `docs/architecture-candidates.md` · Branch:
`refactor/envelope-contract`, stacked on `refactor/lake-table` (merge order: plan-identity, lake-table, this).

## Problem Statement

The producer and the pipeline must agree, byte for byte, on what an **envelope** looks like, but only half of
that agreement is written down where both can see it. The contract package names the topics, the event types
and the timestamp format. The envelope itself is defined three times over:

- the producer builds it as a plain dictionary in one module;
- replay adds the replay fields to that dictionary in place;
- the pipeline reads it back by string field name in many places, and parses its timestamps itself.

The **Kafka key** each envelope is sent under is decided twice as well: once by each live job, and again by
replay from the envelope's payload. The tests reuse replay's private copy of the rule.

This hurts in four concrete ways:

1. **Silent drift.** If a field is renamed on one side, the other side reads `None` and keeps going: rows land
   in the lake with missing provenance, and nothing fails.
2. **A poisoned batch.** The pipeline checks an envelope's fetch time but not its replay time. A replayed
   envelope with a malformed replay time passes the check and then raises inside the micro-batch. The batch
   fails on every retry, so one bad message blocks every later batch until someone intervenes.
3. **A fragile key rule.** The station information topic is compacted: Kafka keeps only the latest envelope
   per Kafka key. If live jobs and replay ever key the same station differently, replayed station information
   and live station information stop replacing each other.
4. **No single place to test the wire format.** There is no test that the bytes a producer writes are the
   bytes the pipeline reads, so a change on either side can break the other unnoticed.

## Solution

The contract package gains one envelope module that both sides use for everything they must agree on. The
producer **builds** an envelope as a typed value, **marks** it as replayed when replaying, and **encodes** it.
The pipeline **decodes** bytes into the same typed value, or gets an error listing every reason the bytes are
not a valid envelope. The contract also computes the **Kafka key** for any envelope.

From the operator's point of view nothing on the wire changes: every envelope already in Kafka or in a
capture still decodes, producers and the pipeline can be deployed in either order, and the DLQ reasons stay
the same. The one visible change: an envelope with a malformed replay time now goes to the DLQ with the
reason `bad_replayed_at` instead of failing the batch.

## User Stories

1. As a pipeline maintainer, I want one definition of the envelope's fields, so that the producer and the
   pipeline cannot disagree about a field's name.
2. As a pipeline maintainer, I want renaming an envelope field to break at import or in the tests, not to
   produce `None` values in the lake, so that drift is caught before it ships.
3. As a pipeline maintainer, I want the pipeline to read envelope fields as typed attributes, so that I stop
   writing string lookups with defaults throughout the micro-batch.
4. As a pipeline maintainer, I want the envelope's fetch time and replay time already parsed into UTC
   datetimes when I decode, so that timestamp parsing is not repeated in each step.
5. As an operator, I want an envelope with a malformed replay time to land in the DLQ with a reason code, so
   that one bad replayed message cannot block every later micro-batch.
6. As an operator, I want the DLQ reason codes to stay exactly as they are today (apart from the new
   `bad_replayed_at`), so that my DLQ queries and the daily quality report keep working.
7. As an operator, I want every envelope already in Kafka to still decode after the deploy, so that no
   retained message turns into a DLQ row because of the refactor.
8. As an operator, I want every existing capture, including the committed sample capture, to still replay,
   so that recorded data stays usable.
9. As an operator, I want to deploy the producer and the pipeline in either order, so that the rollout needs
   no coordination.
10. As an operator, I want the bytes a live producer writes to be identical before and after this change, so
    that anything else reading the topics is unaffected.
11. As a producer maintainer, I want to build an envelope by naming its fields, so that I cannot pass
    arguments in the wrong order (today's builder takes nine positional values).
12. As a producer maintainer, I want to pass fetch times as datetimes, so that the contract alone decides how
    timestamps are written on the wire.
13. As a producer maintainer, I want marking an envelope as replayed to give me a new envelope, not change the
    one I read, so that replay cannot alter a captured envelope by accident.
14. As a producer maintainer, I want the Kafka key computed by the contract from the envelope, so that live
    jobs and replay always agree on it.
15. As an operator of the station information topic, I want replayed and live station information for the
    same station to share one Kafka key, so that topic compaction keeps only the latest one.
16. As an operator, I want an envelope whose station id is missing to be keyed `_missing_station_id`, as it is
    today, so that it still surfaces downstream in the DLQ.
17. As an operator, I want a weather envelope without a location id to be keyed `_missing_location_id`, so
    that the contract does not need the producer's configuration and the case stands out.
18. As an operator running a replay, I want each skipped capture line logged with the reasons it is not a
    valid envelope, so that I can tell a truncated file from a format problem.
19. As an operator running a replay, I want replay to send only envelopes the pipeline will accept as
    envelopes, so that a replay does not fill the DLQ with lines that were never valid.
20. As an operator, I want a replayed envelope to keep its original fetch time and record when it was
    replayed, as it does today, so that latency is still measured against replay time.
21. As a pipeline maintainer, I want an envelope from a future producer that adds a new top-level field to
    decode rather than be rejected, so that adding a field does not need the pipeline deployed first.
22. As a pipeline maintainer, I want the micro-batch and the station dimension build to handle only the
    contract's invalid-envelope error, so that a bug in decoding crashes the task loudly instead of being
    treated as bad data.
23. As a pipeline maintainer, I want the invalid-envelope error to carry every failed check, not just the
    first, so that the DLQ reason lists all of them, as it does today.
24. As a pipeline maintainer, I want the invalid-envelope error to be a kind of value error, so that any
    existing handler for malformed values keeps working.
25. As a pipeline maintainer, I want payload validation (station, weather, poll log and station information
    rules) to stay in the pipeline, so that the contract holds only what both sides must agree on.
26. As a test author, I want to build test envelopes with the contract's type rather than by hand, so that my
    tests stay in step with the wire format.
27. As a test author, I want the fake Kafka consumer to key messages with the contract's rule, so that tests
    stop reaching into replay's internals.
28. As a reviewer, I want one test proving every line of the sample capture decodes and re-encodes to the same
    bytes, so that I can trust the wire format did not change.
29. As a reviewer, I want the producer image to keep working without pyarrow, so that the contract stays
    importable from both images.
30. As a newcomer, I want the envelope's terms (envelope, event type, poll, poll log, replay, Kafka key)
    defined in the glossary, so that I learn one vocabulary for both sides.

## Implementation Decisions

- **One new contract module for the envelope.** It holds the envelope type, encode, decode, the
  invalid-envelope error with its reason codes, and the Kafka key rule. The contract package's top level
  re-exports the envelope type and decode. Like the rest of the contract, it uses the standard library only.
- **The envelope is a frozen (immutable) dataclass** whose attributes use the wire names exactly: event type,
  poll id, source, source URL, GBFS version, feed last-updated time, fetch time, producer version, payload,
  replay flag, replay time. The fetch time and replay time are timezone-aware UTC datetimes; the replay flag is
  a boolean; the payload is a plain dictionary. The schema version is not an attribute: encode writes the
  current version, and decode checks it.
- **Building.** An envelope is built with its constructor, using keyword arguments. The producer's separate
  builder function is deleted, along with the producer's envelope module.
- **Replay marking.** The envelope has a method that takes the replay time and returns a new envelope with the
  replay flag set and the replay time recorded. The original is unchanged.
- **Encode keeps the wire format exactly.** Fields are written in today's order (schema version, event type,
  poll id, source, source URL, GBFS version, feed last-updated time, fetch time, producer version, payload),
  as compact JSON with non-ASCII characters left as they are. The replay flag and replay time are written
  last, and only for a replayed envelope. Timestamps are written with the contract's existing timestamp
  format (ISO-8601 UTC, milliseconds). The schema version stays 1.
- **Decode returns an envelope or raises.** On invalid input it raises the invalid-envelope error, a subclass
  of the standard value error. The error carries the full list of reason codes, and its message is the codes
  joined by commas. All checks run, so several reasons can be reported at once.
- **Reason codes.** Today's codes are kept unchanged: `empty_value`, `invalid_json`, `not_object`,
  `bad_schema_version`, `unknown_event_type`, `missing_payload`, `missing_poll_id`, `bad_fetched_at`. One is
  added: `bad_replayed_at`, for a replay time that is present but not a valid timestamp.
- **Unknown top-level fields are dropped on decode**, without raising, so an envelope from a newer producer
  still decodes. They are therefore not re-sent by replay.
- **The Kafka key rule lives in the contract:**
  - station status and station information → the payload's station id, or `_missing_station_id`;
  - current weather → the payload's location id, or `_missing_location_id` (replay used to fall back to the
    configured location id; that fallback is removed);
  - poll log → the payload's job name, matching what live jobs use today.
- **The producer side.** Live jobs build envelopes with the constructor, passing datetimes, and send each one
  under the contract's Kafka key. The Kafka sink encodes envelopes with the contract's encoder. Replay
  decodes capture lines with the contract and logs the reasons for each line it skips. It marks each
  envelope as replayed, then sends it under the contract's Kafka key. Replay's private key function is
  deleted.
- **The pipeline side.** The micro-batch's record parsing and the station dimension build decode with the
  contract. They catch only the invalid-envelope error and write the joined reasons to the DLQ, as they do
  now. The micro-batch reads envelope fields as attributes and stops parsing timestamps itself. The
  pipeline's own envelope parser is removed from its validation module; payload validation stays there.
- **No change to the lake.** Raw and curated rows keep their columns and values. The raw row's stored payload
  JSON stays the payload alone.
- **Glossary.** The wire-contract terms (envelope, event type, poll, poll log, replay, Kafka key) are already
  in `GLOSSARY.md` on this branch. The Kafka key is defined apart from the Kafka identity.

## Testing Decisions

- **A good test checks behaviour through an interface**: bytes in and envelopes or reasons out, envelopes in
  and bytes or keys out, captures in and lake rows or DLQ rows out. It never checks how the module is built
  inside. Tests should survive refactors of the envelope module's internals.
- **Seam 1, the contract's envelope interface (new, and the main seam).** A new test module covers:
  - every line of the committed sample capture decodes and re-encodes to exactly the same bytes;
  - each reason code, from a matching bad input, including several reasons at once and `bad_replayed_at`;
  - a replayed envelope round-trips, keeping the original fetch time, and the original is unchanged;
  - an envelope with an unknown top-level field decodes, and the field is dropped;
  - the Kafka key for every event type, including both missing-id fallbacks;
  - the invalid-envelope error is a value error, and its message lists the reasons.

  These are in-process tests with no fakes, in the style of the existing contract tests.
- **Seam 2, the micro-batch over the fake Kafka consumer (existing).** The end-to-end suite gains a case: an
  envelope with a malformed replay time lands in the DLQ with `bad_replayed_at`, and the batch commits.
  Existing tests keep checking that replayed rows carry the replay time. Prior art: the end-to-end tests that
  drive plan, process, metrics and finalize over the fake consumer and check DLQ and curated rows.
- **Seam 3, the producer jobs and replay over the fake Kafka producer (existing).** Existing producer and
  replay tests keep checking the bytes sent and the Kafka key. Their hand-built envelope dictionaries become
  envelopes built with the contract. Prior art: the producer tests that capture what the fake producer was
  sent.
- **Test helpers.** The pipeline unit tests' envelope helper builds with the contract and encodes. The fake
  consumer's capture loader keys messages with the contract's Kafka key rule.
- **Tests that go away.** The tests of the pipeline's own envelope parser move to the new contract tests and
  are rewritten there against decode, keeping their cases.
- The full test suite and the linter pass.

## Out of Scope

- **Payload validation** (station status, weather, poll log and station information rules) stays in the
  pipeline as it is.
- **The wire format.** No new fields, no renamed fields, no schema version change.
- **The lake.** No column, schema or layout changes in raw, curated or gold data.
- **Typed payloads.** The payload stays a plain dictionary. Typing each event type's payload is a possible
  later step.
- **The micro-batch step records** (plan, stats and publish dictionaries): backlog candidate 3.
- **The serving files**: backlog candidate 4.
- **The broker tools and topic creation**: unchanged, apart from using the contract's types if they decode
  envelopes.

## Further Notes

- On the committed sample capture (1,937 envelopes), every fetch time round-trips exactly through the
  contract's timestamp format, and re-encoding gives identical bytes for every line. No envelope has a
  top-level field beyond the ten known ones. This is what makes the "no change on the wire" decision safe;
  the round-trip test keeps it so.
- The `bad_replayed_at` change also fixes a latent crash: today a malformed replay time passes the
  envelope check and then raises inside the micro-batch.
- No issue tracker is configured for this repo (run `/setup-matt-pocock-skills` to set one up). This file
  stands in for the tracker issue; it can be pasted into a GitHub issue as-is.
