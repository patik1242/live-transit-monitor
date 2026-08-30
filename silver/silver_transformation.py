from pyspark.sql import functions as F

def add_derived_columns(df):
    return (
        df
        .withColumn("generated",              F.try_to_timestamp("generated"))
        .withColumn("lastUpdate",             F.try_to_timestamp("lastUpdate"))
        .withColumn("scheduledTripStartTime", F.try_to_timestamp("scheduledTripStartTime"))
        .withColumn("delay_min",   F.round(F.col("delay") / 60.0, 1))
        .withColumn("has_trip",    F.col("tripId").isNotNull())
        .withColumn("delay_bucket",
            F.when(F.col("delay").isNull(), F.lit(None).cast("string"))        
             .when(F.col("delay") < -60, "early")
             .when(F.col("delay") <= 120, "on_time")
             .otherwise("delayed"))
        .withColumn("is_delayed",  F.col("delay") > 120)
        .withColumn("is_stopped",  F.col("speed") == 0)
        .withColumn("is_moving",   F.col("speed") > 0)
        .withColumn("gps_ok",      F.col("gpsQuality") > 0)
        .withColumn("event_time_local",
            F.from_utc_timestamp("event_time", "Europe/Warsaw"))
    )