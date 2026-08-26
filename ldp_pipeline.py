# ldp_pipeline.py
# Lakeflow Spark Declarative Pipeline — Transit GPS: bronze -> silver.
# Refines raw GPS bronze into a clean, deduplicated silver table, with
# data-quality expectations. Ingestion stays OUTSIDE this file (dev: seeded
# bronze; prod: Event Hub consumer). Gold/dnalytics/dashboards also stay outside.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# --- Parameters: from the pipeline configuration ---
# dev  -> catalog=workspace, bronze_schema=live_transit_monitor
# prod -> catalog=dbr_dev,   bronze_schema=live_transit_monitor
CATALOG       = spark.conf.get("catalog")
BRONZE_SCHEMA = spark.conf.get("bronze_schema")

BRONZE_GPS = f"{CATALOG}.{BRONZE_SCHEMA}.gps_data"

# Single source of truth for pipeline table names.
TABLES = {
    "silver_clean": "gps_positions_silver_clean",
    "silver":       "gps_positions_silver",
}

# ===================================================================
# SILVER (clean) — expectations + derived columns
#   expect_all          -> keep the row, only COUNT the failure (warn)
#   expect_all_or_drop  -> DROP the failing row
#   expect_all_or_fail  -> STOP the pipeline
# ===================================================================
@dp.table(name=TABLES["silver_clean"])
@dp.expect_all_or_drop({
    "coords_present": "lat IS NOT NULL AND lon IS NOT NULL",
    "in_gdansk_bbox": "lat BETWEEN 54.2 AND 54.6 AND lon BETWEEN 18.3 AND 19.0",
})
@dp.expect_all({                       # warn only — does not drop
    "gps_quality_ok": "gpsQuality > 0",
})
@dp.expect_all_or_fail({               # a missing business key stops the run
    "vehicle_present": "vehicleId IS NOT NULL",
})
def silver_clean():
    return (
        spark.readStream.table(BRONZE_GPS)
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
            F.from_utc_timestamp("event_time", "Europe/Warsaw"))
    )

# ===================================================================
# SILVER (final) — idempotent dedup via CDC.
# Replaces dropDuplicates(["vehicleId", "generated"]). The engine performs
# the upsert, so re-running the pipeline never creates duplicate rows.
# ===================================================================
dp.create_streaming_table(name=TABLES["silver"])
dp.create_auto_cdc_flow(
    target      = TABLES["silver"],
    source      = TABLES["silver_clean"],
    keys        = ["vehicleId", "generated"], # composite
    sequence_by = F.col("event_time"),
)