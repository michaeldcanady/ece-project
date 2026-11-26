"""Export the lake's serving tables as CSV files to upload to Qlik Cloud.

Qlik reads the files by hand-upload (Create > Upload data files), so a refresh is: run this, re-upload.
Writes four files to the output directory, overwriting them:

- current_station_state.csv: the latest state of every station (the serving file)
- system_availability_5m.csv: system-wide availability every five minutes, all history in the lake
- station_hourly.csv: per-station hourly averages, rolled up from the 15-minute windows (the full windows are
  ~900k rows, too large to hand-upload)
- batch_log.csv: the recent micro-batch log (counts, latencies, status), for the pipeline health panel

Usage: python scripts/export_qlik_csv.py [LAKE_DIR] [OUT_DIR]
"""
from __future__ import annotations

import glob
import os
import sys
from typing import List

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

SERVING = "gold/serving/current_station_state.parquet"
BATCH_LOG = "gold/serving/batch_log_recent.parquet"
AVAILABILITY = "gold/system_availability_5m"
WINDOWS = "gold/station_window_15m"
HOURLY_COLUMNS = ["station_id", "window_start", "bikes_last", "docks_last", "net_change",
                  "at_risk_empty", "at_risk_full"]


def _read_all(files: List[str], columns: List[str] = None) -> pa.Table:
    if not files:
        raise SystemExit("no parquet files found: is the lake path right and the pipeline running?")
    return pa.concat_tables([pq.read_table(f, columns=columns) for f in sorted(files)], promote_options="default")


def _parts(lake: str, prefix: str) -> List[str]:
    return glob.glob(os.path.join(lake, prefix, "**", "*.parquet"), recursive=True)


def current_state(lake: str) -> pa.Table:
    return _read_all([os.path.join(lake, SERVING)])


def batch_log(lake: str) -> pa.Table:
    return _read_all([os.path.join(lake, BATCH_LOG)])


def availability(lake: str) -> pa.Table:
    return _read_all(_parts(lake, AVAILABILITY))


def station_hourly(lake: str) -> pa.Table:
    t = _read_all(_parts(lake, WINDOWS), HOURLY_COLUMNS)
    t = t.append_column("hour_start", pc.floor_temporal(t["window_start"], unit="hour"))
    t = t.append_column("risk_empty_i", pc.cast(t["at_risk_empty"], pa.int32()))
    t = t.append_column("risk_full_i", pc.cast(t["at_risk_full"], pa.int32()))
    grouped = t.group_by(["station_id", "hour_start"]).aggregate(
        [("bikes_last", "mean"), ("docks_last", "mean"), ("net_change", "sum"),
         ("risk_empty_i", "max"), ("risk_full_i", "max")])
    return grouped.rename_columns(["station_id", "hour_start", "avg_bikes", "avg_docks", "net_change_sum",
                                   "was_at_risk_empty", "was_at_risk_full"])


def main(argv: List[str]) -> None:
    lake = argv[1] if len(argv) > 1 else os.environ.get("CB_LOCAL_ROOT", "/lake")
    out = argv[2] if len(argv) > 2 else "data/qlik_export"
    os.makedirs(out, exist_ok=True)
    for name, table in (("current_station_state", current_state(lake)),
                        ("system_availability_5m", availability(lake)),
                        ("station_hourly", station_hourly(lake)),
                        ("batch_log", batch_log(lake))):
        pacsv.write_csv(table, os.path.join(out, "%s.csv" % name))
        print("%s: %d rows" % (name, table.num_rows))


if __name__ == "__main__":
    main(sys.argv)
