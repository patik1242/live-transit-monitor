from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from silver.silver_transformation import add_derived_columns

# Defining the schema of all columns
INPUT_SCHEMA = T.StructType([
    T.StructField("generated", T.StringType(), True),
    T.StructField("lastUpdate", T.StringType(), True),
    T.StructField("scheduledTripStartTime", T.StringType(), True),
    T.StructField("event_time", T.TimestampType(), True),
    T.StructField("delay", T.IntegerType(), True),
    T.StructField("tripId", T.IntegerType(), True),
    T.StructField("speed", T.IntegerType(), True),
    T.StructField("gpsQuality", T.IntegerType(), True),
])


def make_input(spark_session, **overrides):
    """Create one production-shaped row while keeping each test explicit."""
    # event_time is included here because the raw sample files do not contain it,
    # although the production transformation uses it for local-time conversion.
    values = {
        "generated": "2026-07-29T15:30:03Z",
        "lastUpdate": "2026-07-29T15:30:08Z",
        "scheduledTripStartTime": "2026-07-29T15:06:00Z",
        "event_time": datetime(2026, 7, 29, 15, 30, 3),
        "delay": 35,
        "tripId": 101,
        "speed": 45,
        "gpsQuality": 3,
    }
    values.update(overrides)
    return spark_session.createDataFrame(
        [tuple(values[field.name] for field in INPUT_SCHEMA)], INPUT_SCHEMA
    )


def get_result(spark_session, **overrides):
    """Creates a Spark data frame with add_derived_columns"""
    return add_derived_columns(make_input(spark_session, **overrides)).first()


def test_add_derived_columns_converts_timestamps_and_preserves_rows(spark_session):
    """Testing correct timestamp conversion, correct delay_min calculation, 
    converting UTC to Warsaw local time, if the source contains one row"""
    source = make_input(spark_session)
    result = add_derived_columns(source).first()

    assert source.count() == 1
    assert isinstance(result.generated, datetime)
    assert isinstance(result.lastUpdate, datetime)
    assert isinstance(result.scheduledTripStartTime, datetime)
    assert result.generated == datetime(2026, 7, 29, 15, 30, 3)
    assert result.delay_min == 0.6
    assert result.event_time_local == datetime(2026, 7, 29, 17, 30, 3)


@pytest.mark.parametrize(
    ("delay", "expected_bucket", "expected_delayed"),
    [
        (-61, "early", False),
        (-60, "on_time", False),
        (0, "on_time", False),
        (120, "on_time", False),
        (121, "delayed", True),
    ],
)
def test_delay_bucket_uses_documented_thresholds(
    spark_session, delay, expected_bucket, expected_delayed
):
    """Testing boundaries of delay buckets"""
    # These values cover both sides of each inclusive/exclusive comparison in
    # the chained Spark when expression, especially -60 and 120 seconds.
    result = get_result(spark_session, delay=delay)

    assert result.delay_bucket == expected_bucket
    assert result.is_delayed is expected_delayed


@pytest.mark.parametrize(
    ("delay", "expected_minutes"),
    [(35, 0.6), (-115, -1.9), (90, 1.5), (None, None)],
)
def test_delay_min_is_rounded_to_one_decimal(spark_session, delay, expected_minutes):
    """Confirms if delay_min is rounded to one decimal place and if null delay remains null"""
    assert get_result(spark_session, delay=delay).delay_min == expected_minutes


@pytest.mark.parametrize(
    ("trip_id", "expected_has_trip"),
    [(101, True), (None, False)],
)
def test_has_trip_reflects_trip_id_presence(spark_session, trip_id, expected_has_trip):
    """Checks if real trip_id produces True"""
    assert get_result(spark_session, tripId=trip_id).has_trip is expected_has_trip


@pytest.mark.parametrize(
    ("speed", "expected_stopped", "expected_moving"),
    [(0, True, False), (5, False, True), (-1, False, False), (None, None, None)],
)
def test_movement_flags_distinguish_stopped_and_moving(
    spark_session, speed, expected_stopped, expected_moving
):
    """Verifies both movement columns"""
    result = get_result(spark_session, speed=speed)

    assert result.is_stopped == expected_stopped
    assert result.is_moving == expected_moving


@pytest.mark.parametrize(
    ("gps_quality", "expected_gps_ok"),
    [(3, True), (1, True), (0, False), (-1, False), (None, None)],
)
def test_gps_ok_requires_positive_quality(spark_session, gps_quality, expected_gps_ok):
    """Tests if gps is ok when above zero"""
    assert get_result(spark_session, gpsQuality=gps_quality).gps_ok == expected_gps_ok


def test_null_measurements_propagate_without_being_reinterpreted(spark_session):
    """Tests if null is correctly propagated"""
    result = get_result(
        spark_session,
        scheduledTripStartTime=None,
        event_time=None,
        delay=None,
        speed=None,
        gpsQuality=None,
    )

    assert result.scheduledTripStartTime is None
    assert result.event_time_local is None
    assert result.delay_min is None
    assert result.delay_bucket is None
    assert result.is_delayed is None
    assert result.is_stopped is None
    assert result.is_moving is None
    assert result.gps_ok is None


def test_invalid_timestamp_strings_become_null(spark_session):
    """Checks if values such as not a timestamp do not cause unexpected values"""
    result = get_result(
        spark_session,
        generated="not-a-timestamp",
        lastUpdate="2026-99-99T99:99:99Z",
        scheduledTripStartTime="",
    )

    assert result.generated is None
    assert result.lastUpdate is None
    assert result.scheduledTripStartTime is None


def test_empty_input_keeps_schema_and_has_no_rows(spark_session):
    """Tests if the function behaves correctly with zero rows"""
    empty = spark_session.createDataFrame([], INPUT_SCHEMA)

    result = add_derived_columns(empty)

    assert result.count() == 0
    assert {
        "delay_min",
        "has_trip",
        "delay_bucket",
        "is_delayed",
        "is_stopped",
        "is_moving",
        "gps_ok",
        "event_time_local",
    }.issubset(result.columns)


def test_typical_sample_file_can_be_enriched(spark_session):
    sample_path = Path(__file__).parents[2] / "sample_data" / "gps_20260729_173022.json"
    sample = spark_session.read.json(str(sample_path)).withColumn(
        "event_time", F.lit(datetime(2026, 7, 29, 15, 30, 3)).cast("timestamp")
    )

    result = add_derived_columns(sample)

    assert result.count() == sample.count()
    assert result.filter("delay_bucket IS NULL").count() == 0
    assert result.filter("has_trip = false").count() > 0
    assert result.filter("is_delayed = true").count() > 0
