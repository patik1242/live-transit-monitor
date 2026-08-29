# ldp_pipeline.py
# Lakeflow Spark Declarative Pipeline — Transit GPS: bronze -> silver.
# Enriches raw GPS bronze, applies DQX data-quality checks, splits into a
# deduplicated silver table and a quarantine table.
# Ingestion stays OUTSIDE this file. Gold/analytics/dashboards stay outside.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

dq = DQEngine(WorkspaceClient())

# --- Parameters: from the pipeline configuration ---
CATALOG       = spark.conf.get("catalog")
BRONZE_SCHEMA = spark.conf.get("bronze_schema")
BRONZE_GPS    = f"{CATALOG}.{BRONZE_SCHEMA}.gps_data"

TABLES = {
    "checked":    "gps_positions_silver_checked",
    "valid":      "gps_positions_silver_valid",
    "quarantine": "gps_positions_quarantine",
    "silver":     "gps_positions_silver",
}

# --- DQX quality checks (metadata form) ---
CHECKS = [
    {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "vehicleId"}}},
    {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "lat"}}},
    {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "lon"}}},
    {"criticality": "error", "check": {"function": "is_in_range",
        "arguments": {"column": "lat", "min_limit": 54.2, "max_limit": 54.6}}},
    {"criticality": "error", "check": {"function": "is_in_range",
        "arguments": {"column": "lon", "min_limit": 18.3, "max_limit": 19.0}}},
    {"criticality": "warn", "check": {"function": "is_not_less_than",
        "arguments": {"column": "gpsQuality", "limit": 1}}},
]


def _enrich(df):
    """Derived columns shared by all downstream tables (pure -> testable later)."""
    return (df
        .withColumn("generated",              F.to_timestamp("generated"))
        .withColumn("lastUpdate",             F.to_timestamp("lastUpdate"))
        .withColumn("scheduledTripStartTime", F.to_timestamp("scheduledTripStartTime"))
        .withColumn("delay_min",   F.round(F.col("delay") / 60.0, 1))
        .withColumn("has_trip",    F.col("tripId").isNotNull())
        .withColumn("delay_bucket",
            F.when(F.col("delay") < -60, "early")
             .when(F.col("delay") <= 120, "on_time")
             .otherwise("delayed"))
        .withColumn("is_delayed",  F.col("delay") > 120)
        .withColumn("is_stopped",  F.col("speed") == 0)
        .withColumn("is_moving",   F.col("speed") > 0)
        .withColumn("gps_ok",      F.col("gpsQuality") > 0)
        .withColumn("event_time_local",
            F.from_utc_timestamp("event_time", "Europe/Warsaw")))


# 1) CHECKED — enrich, then apply DQX (adds _error/_warning, keeps ALL rows)
@dp.table(name=TABLES["checked"])
def checked():
    df = _enrich(spark.readStream.table(BRONZE_GPS))
    return dq.apply_checks_by_metadata(df, CHECKS)


# 2) VALID — good rows only, drop DQX helper cols, feed CDC
@dp.table(name=TABLES["valid"])
def valid():
    return (spark.readStream.table(TABLES["checked"])
            .filter("_error IS NULL")
            .drop("_error", "_warning"))


# 3) QUARANTINE — bad rows, keep the DQX error detail
@dp.table(name=TABLES["quarantine"])
def quarantine():
    return spark.readStream.table(TABLES["checked"]).filter("_error IS NOT NULL")


# 4) SILVER (final) — idempotent CDC dedup on the valid stream
dp.create_streaming_table(name=TABLES["silver"])
dp.create_auto_cdc_flow(
    target      = TABLES["silver"],
    source      = TABLES["valid"],
    keys        = ["vehicleId", "generated"],
    sequence_by = F.col("event_time"),
)