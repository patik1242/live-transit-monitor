import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession \
        .builder \
        .appName("Spark Unit Test") \
        .master("local[*]") \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    yield spark
    spark.stop()

