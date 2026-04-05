import os
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

_SRC = Path(__file__).resolve().parent.parent
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from pipeline_paths import project_root, resolve_input, resolve_parquet_output
from spark_bootstrap import build_spark_session, is_cloud_storage

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

INPUT_PATH = resolve_input("FLOWS_INPUT_URI", "flows.txt.gz")
OUTPUT_PATH = resolve_parquet_output("flows")

print(f"PROJECT_ROOT: {project_root()}")
print(f"INPUT_PATH:   {INPUT_PATH}")
print(f"OUTPUT_PATH:  {OUTPUT_PATH}")
if is_cloud_storage(INPUT_PATH):
    print("File exists:  n/a (cloud URI)")
else:
    print(f"File exists:  {os.path.exists(INPUT_PATH)}")

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

spark = build_spark_session(
    "Flows to Parquet",
    cloud_paths=[INPUT_PATH, OUTPUT_PATH],
    extra_config={"spark.sql.parquet.compression.codec": "snappy"},
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

(
    df.write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
)

print(f"Written to: {OUTPUT_PATH}")

df_verify = spark.read.parquet(OUTPUT_PATH)
df_verify.printSchema()
df_verify.show(5, truncate=False)
print(f"Verified row count: {df_verify.count()}")

spark.stop()
