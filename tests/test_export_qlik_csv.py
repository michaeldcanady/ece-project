"""station_hourly rolls the 15-minute windows up to hours, with the columns labeled correctly."""
from __future__ import annotations

import importlib.util
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

_SPEC = importlib.util.spec_from_file_location(
    "export_qlik_csv", Path(__file__).resolve().parent.parent / "scripts" / "export_qlik_csv.py")
assert _SPEC is not None and _SPEC.loader is not None
export = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(export)


def test_station_hourly_labels_columns_by_name(tmp_path: Path) -> None:
    d = tmp_path / export.WINDOWS / "dt=2026-01-01"
    d.mkdir(parents=True)
    pq.write_table(pa.table({
        "station_id": ["s1", "s1", "s2"],
        "window_start": [datetime(2026, 1, 1, 10, 0), datetime(2026, 1, 1, 10, 15), datetime(2026, 1, 1, 10, 0)],
        "bikes_last": [2, 4, 7],
        "docks_last": [10, 8, 5],
        "net_change": [1, -3, 2],
        "at_risk_empty": [False, True, False],
        "at_risk_full": [False, False, False],
    }), d / "p.parquet")
    t = export.station_hourly(str(tmp_path))
    assert t.column_names == ["station_id", "hour_start", "avg_bikes", "avg_docks", "net_change_sum",
                              "was_at_risk_empty", "was_at_risk_full"]
    rows = {r["station_id"]: r for r in t.to_pylist()}
    assert rows["s1"]["avg_bikes"] == 3.0
    assert rows["s1"]["avg_docks"] == 9.0
    assert rows["s1"]["net_change_sum"] == -2
    assert rows["s1"]["was_at_risk_empty"] == 1
    assert rows["s2"]["avg_bikes"] == 7.0
    assert rows["s2"]["hour_start"] == datetime(2026, 1, 1, 10, 0)
