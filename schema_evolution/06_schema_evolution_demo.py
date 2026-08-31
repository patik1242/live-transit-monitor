# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Schema Evolution Demo – Event Hub -> `gps_data_demo`
# MAGIC
# MAGIC Separate demo pipeline. It reads the SAME Event Hub but writes to a
# MAGIC **separate table** (`gps_data_demo`) with a **separate checkpoint**, so it
# MAGIC does NOT touch production `gps_data` or the live map.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, from_json, current_timestamp, to_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

CATALOG = "dbr_dev"
SCHEMA  = "live_transit_monitor"

# --- Event Hub (same source as the production consumer) ---
EH_NAMESPACE = "evhpl24databricks02"
EH_NAME      = "live_transit_monitor_evh"   # <-- CONFIRM the hub name
CONSUMER     = "$Default"                    # or "demo" if a demo consumer group exists
SECRET_SCOPE = "default2"
SECRET_KEY   = "eh-conn-transit"             # LISTEN connection string
BOOTSTRAP    = f"{EH_NAMESPACE}.servicebus.windows.net:9093"
EH_CONN      = dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY)
EH_JAAS      = ("kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
                f'username="$ConnectionString" password="{EH_CONN}";')

# --- Demo target (SEPARATE from production gps_data) ---
BRONZE_DEMO     = f"{CATALOG}.{SCHEMA}.gps_data_demo"
CHECKPOINT_DEMO = "abfss://live-transit-monitor@dlspl21databricks.dfs.core.windows.net/_checkpoint/gps_data_demo"

# COMMAND ----------

# --- RESET (run ONLY before a fresh rehearsal, to get a clean "before" state) ---
spark.sql(f"DROP TABLE IF EXISTS {BRONZE_DEMO}")
dbutils.fs.rm(CHECKPOINT_DEMO, recurse=True)

# COMMAND ----------

# --- Two schema versions ---
# v1 = current contract (no occupancy)
event_schema_v1 = (StructType()
    .add("generated", StringType())
    .add("routeShortName", StringType())
    .add("tripId", IntegerType())
    .add("routeId", IntegerType())
    .add("headsign", StringType())
    .add("vehicleCode", StringType())
    .add("vehicleService", StringType())
    .add("vehicleId", IntegerType())
    .add("speed", IntegerType())
    .add("direction", IntegerType())
    .add("delay", IntegerType())
    .add("scheduledTripStartTime", StringType())
    .add("lat", FloatType())
    .add("lon", FloatType())
    .add("gpsQuality", IntegerType())
    .add("lastUpdate", StringType()))

# v2 = evolved contract (+ occupancy)
event_schema_v2 = StructType(event_schema_v1.fields + [StructField("occupancy", StringType())])

# COMMAND ----------

# --- Helper funtion to run the demo stream with a chosen schema ---
def start_demo_stream(active_schema):
    raw = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", EH_NAME)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", EH_JAAS)
        .option("kafka.group.id", CONSUMER)
        .option("startingOffsets", "latest")     # demo: only new events from now on
        .option("failOnDataLoss", "false")
        .load())

    parsed = (raw
        .select(
            from_json(col("value").cast("string"), active_schema).alias("e"),
            col("value").cast("string").alias("raw_value"),   # rescued: keep full event
            col("partition"), col("offset"), col("timestamp").alias("enqueued_ts"))
        .select("e.*", "raw_value", "partition", "offset", "enqueued_ts")
        .withColumn("event_time", to_timestamp("generated"))
        .withColumn("_source", lit("eventhub-demo"))
        .withColumn("ingestion_ts", current_timestamp()))

    return (parsed.writeStream.format("delta")
        .option("checkpointLocation", CHECKPOINT_DEMO)
        .option("mergeSchema", "true")            # <-- lets bronze gain new columns automatically
        .trigger(processingTime="20 seconds")     # continuous, every 20s
        .toTable(BRONZE_DEMO))

# COMMAND ----------

# STEP 1 — start the demo stream on schema v1 (no occupancy)
query = start_demo_stream(event_schema_v1)

# COMMAND ----------

# STEP 2 — the table has NO occupancy column yet
spark.read.table(BRONZE_DEMO).printSchema()

# COMMAND ----------

# STEP 3 — now, I run the demo_producer.py locally (sends events WITH occupancy - the streaming source has evolved, but we still use schema v1)
# STEP 4 — the stream did NOT break, and the raw event already carries occupancy:
spark.read.table(BRONZE_DEMO).filter("raw_value like '%occupancy%'") \
    .select("raw_value").show(3, False)

# COMMAND ----------

# STEP 5 — EVOLVE THE SCHEMA: stopping the stream, then restart on schema v2 (same checkpoint, mergeSchema on)
query.stop()
query = start_demo_stream(event_schema_v2)

# COMMAND ----------

# STEP 6 — Running python demo_producer.py on my computer again
# STEP 7 — after ~30s: the occupancy column now exists and is populated
spark.read.table(BRONZE_DEMO).groupBy("occupancy").count().show()

# COMMAND ----------

# Newly enriched demo rows
spark.read.table(BRONZE_DEMO) \
    .select("vehicleCode", "routeShortName", "occupancy", "event_time") \
    .filter("occupancy is not null").show(10, False)

# COMMAND ----------

# CLEANUP after the demo (stop the demo stream)
query.stop()