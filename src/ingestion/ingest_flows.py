import os
import sys

# Fix PySpark on Windows - must be set before importing SparkSession
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
INPUT_PATH = os.path.join(PROJECT_ROOT, "data", "flows.txt.gz")

# Debug - verify paths
print(f"PROJECT_ROOT: {PROJECT_ROOT}")
print(f"INPUT_PATH:   {INPUT_PATH}")
print(f"File exists:  {os.path.exists(INPUT_PATH)}")

# Schema
schema = StructType([
    StructField("time", IntegerType(), True),
    StructField("duration", IntegerType(), True),
    StructField("src_computer", StringType(), True),
    StructField("src_port", StringType(), True),
    StructField("dst_computer", StringType(), True),
    StructField("dst_port", StringType(), True),
    StructField("protocol", IntegerType(), True),
    StructField("packets_count", IntegerType(), True),
    StructField("bytes_count", IntegerType(), True),
])

# Spark session
spark = (
    SparkSession.builder
    .appName("Ingest Flows Logs")
    .master("local[*]")
    .getOrCreate()
)

# Read
df_pyspark = (
    spark.read
        .option("header", False)
        .option("compression", "gzip")
        .schema(schema)
        .csv(INPUT_PATH)
)

df_pyspark.printSchema()
df_pyspark.show(10, truncate=False)

print(f"Type:       {type(df_pyspark)}")
print(f"Row count:  {df_pyspark.count()}")
print(f"Partitions: {df_pyspark.rdd.getNumPartitions()}")

spark.stop()