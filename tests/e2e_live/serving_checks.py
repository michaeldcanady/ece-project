"""Check 6: the serving files are readable by a BI tool (flat typed columns, UTC timestamps, stable names)."""
from __future__ import annotations

from typing import List

import pyarrow as pa
import pyarrow.types as pt

from citibike_pipeline import serving
from citibike_pipeline.batch_log import SERVING_BATCH_LOG
from citibike_pipeline.schemas import BATCH_LOG, CURRENT_STATE, SYSTEM_AVAILABILITY
from citibike_pipeline.storage import Storage, read_parquet

from .check_result import CheckResult

_EXPECTED = [(serving.CURRENT, CURRENT_STATE), (serving.AT_RISK, CURRENT_STATE),
             (serving.SUMMARY, SYSTEM_AVAILABILITY), (SERVING_BATCH_LOG, BATCH_LOG)]


def _nested(t: pa.DataType) -> bool:
    return pt.is_nested(t) or pt.is_dictionary(t) or pt.is_union(t)


def bi_readable(storage: Storage, lake_name: str) -> List[CheckResult]:
    out: List[CheckResult] = []
    for key, schema in _EXPECTED:
        name = "6 %s: %s" % (lake_name, key.split("/")[-1])
        if not storage.exists(key):
            out.append(CheckResult(name, False, "file is missing"))
            continue
        t = read_parquet(storage, key)
        problems = []
        if t.schema.names != schema.names:
            problems.append("columns differ from the declared schema")
        if len(set(t.schema.names)) != len(t.schema.names):
            problems.append("duplicate column names")
        problems += ["%s is nested" % f.name for f in t.schema if _nested(f.type)]
        problems += ["%s is not UTC" % f.name for f in t.schema if pt.is_timestamp(f.type) and f.type.tz != "UTC"]
        if t.schema.types != schema.types:
            problems.append("column types differ from the declared schema")
        if key == serving.CURRENT and t.num_rows == 0:
            problems.append("no stations")
        detail = "%d rows" % t.num_rows + ("; " + "; ".join(problems) if problems else "")
        out.append(CheckResult(name, not problems, detail))
    return out
