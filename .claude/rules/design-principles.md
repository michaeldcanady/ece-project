---
paths:
  - "citibike_contract/**"
  - "citibike_pipeline/**"
  - "citibike_producer/**"
  - "dags/**"
  - "scripts/**"
---

# Design principles

Source: the Kempner Institute handbook, "Software Design Principles" (sections 6.1–6.13): https://handbook.eng.kempnerinstitute.harvard.edu/s2_swe_for_research/software_design_principles.html This rule holds only what the
repo does not already state. Read these in the handbook and do not restate them here: KISS, DRY, YAGNI, separation of
concerns and single responsibility (6.1); cohesion, coupling and information hiding (6.2); design patterns (6.5). Typed records, idempotence and one type per file are in `CODING_STANDARDS.md`.

- **Dependencies point one way, toward a core that does no I/O** (6.3, 6.12). A module may import from its own layer
  or a lower one: contract, then core (pure values and logic), then infrastructure (Kafka, storage, the lake), then
  orchestration and entry. `scripts/check_layers.py` holds the layer map and the producer's chain, enforces this, and
  fails a module in no layer. A core module that needs a Kafka or storage fake to be tested is in the wrong layer.
- **Refactors change no behaviour** (6.8). A refactor commit stays apart from a behaviour change, and from documentation or
  glossary edits, so each can be reviewed for what it claims to be.
- **Significant design decisions are recorded as ADRs** (6.7; context, decision, consequences). Create the ADR directory
  when the first ADR is written, not before.
- **Functions plus dataclasses by default** (6.10). Add a class only when shared, updated state justifies it; do not reach
  for inheritance by habit.
- **Small public surface** (6.11). Prefix internals with an underscore and list a package's public names in `__all__`.
- **Outputs are traceable** (6.9). A new output carries the config and code version that produced it.
