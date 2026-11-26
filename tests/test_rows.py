"""The row classes and the column lists stay in step with the Parquet schemas."""
from dataclasses import fields

from citibike_pipeline.dead_letter_row import DeadLetterRow
from citibike_pipeline.quality import STATUS_COLS
from citibike_pipeline.raw_row import RawRow
from citibike_pipeline.rows import CuratedStatusRow, CuratedWeatherRow
from citibike_pipeline.schemas import DLQ, RAW, STATUS, WEATHER
from citibike_pipeline.snapshot import STATUS_ROW_COLS, StatusRow


def names(cls):
    return [f.name for f in fields(cls)]


def test_the_curated_status_row_has_the_status_schema_columns_in_order():
    assert names(CuratedStatusRow) == STATUS.names


def test_the_curated_weather_row_has_the_weather_schema_columns_in_order():
    assert names(CuratedWeatherRow) == WEATHER.names


def test_the_columns_other_modules_read_are_status_columns():
    assert set(STATUS_ROW_COLS) <= set(STATUS.names)
    assert set(STATUS_COLS) <= set(STATUS.names)
    assert names(StatusRow) == STATUS_ROW_COLS


def test_the_raw_row_has_the_raw_schema_columns_in_order():
    assert names(RawRow) == RAW.names


def test_the_dead_letter_row_has_the_dlq_schema_columns_in_order():
    assert names(DeadLetterRow) == DLQ.names
