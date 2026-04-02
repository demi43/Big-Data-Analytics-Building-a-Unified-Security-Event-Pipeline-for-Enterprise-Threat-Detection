import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
INPUT_PATH  = os.path.join(PROJECT_ROOT, "data", "dns.txt.gz")
OUTPUT_PATH = os.path.join(PROJECT_ROOT, "Parquet", "dns")

print(f"PROJECT_ROOT: {PROJECT_ROOT}")
print(f"INPUT_PATH:   {INPUT_PATH}")
print(f"File exists:  {os.path.exists(INPUT_PATH)}")
print(f"OUTPUT_PATH:  {OUTPUT_PATH}")

schema = StructType([
    StructField("time", IntegerType(), True),
    StructField("SourceComputer", StringType(), True),
    StructField("ComputerResolved", StringType(), True),
])

spark = (
    SparkSession.builder
    .appName("Ingest DNS Logs")
    .master("local[*]")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)

df = (
    spark.read
        .option("header", False)
        .option("compression", "gzip")
        .schema(schema)
        .csv(INPUT_PATH)
)

df.printSchema()
df.show(10, truncate=False)

print(f"Type:       {type(df)}")
print(f"Row count:  {df.count()}")
print(f"Partitions: {df.rdd.getNumPartitions()}")

# Write to Parquet
(
    df.write
      .mode("overwrite")
      .parquet(OUTPUT_PATH)
)

print(f"Written to: {OUTPUT_PATH}")

# Verify round-trip
df_verify = spark.read.parquet(OUTPUT_PATH)
df_verify.printSchema()
df_verify.show(5, truncate=False)
print(f"Verified row count: {df_verify.count()}")

spark.stop()