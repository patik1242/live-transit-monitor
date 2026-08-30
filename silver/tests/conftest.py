import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    # On Databricks Serverless, use the existing active session
    # instead of creating a new one
    spark = SparkSession.getActiveSession()
    
    if spark is None:
        # Fallback for local development or classic clusters
        spark = SparkSession \
            .builder \
            .appName("Spark Unit Test") \
            .master("local[*]") \
            .config('spark.sql.session.timeZone', 'UTC') \
            .config("spark.sql.shuffle.partitions", "1") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

    yield spark
    # Don't stop the session if we're using the active notebook session

