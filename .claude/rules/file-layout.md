---
paths:
  - "citibike_contract/**"
  - "citibike_pipeline/**"
  - "citibike_producer/**"
  - "dags/**"
  - "scripts/**"
---

# File layout

Each type has its own file, named for the type. Types are classes, enums, dataclasses and exceptions
(`Plan` in `plan.py`, `StationStatus` in `station_status.py`, `InvalidEnvelope` in `invalid_envelope.py`). A function
lives in one of two places:

- **With its type**, when it builds, parses, converts or reads that type (`parse`, `to_json`, `from_table`,
  `from_delivered`). It is a method or a function in the type's file.
- **In a module named for its utility**, when it serves no single type: `ranges.py` for offset-range arithmetic,
  `timeutil.py` for time conversion. One module, one utility; the name says what is inside.

A function that touches several types goes with the type it returns, or the type it is mostly about. A function that
fits neither place means the utility module is missing: add it.

Apply this to files you create and files you change. Moving a type into its own file is part of a change that
touches the type; leave untouched files as they are.
