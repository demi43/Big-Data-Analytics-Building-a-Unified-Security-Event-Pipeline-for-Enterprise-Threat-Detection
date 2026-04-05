import os
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parent.parent
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from pipeline_paths import project_root, resolve_input, resolve_parquet_output
from spark_bootstrap import build_spark_session, is_cloud_storage

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

INPUT_PATH = resolve_input("AUTH_INPUT_URI", "lanl-auth-dataset-1.bz2")
OUTPUT_PATH = resolve_parquet_output("auth")

print(f"PROJECT_ROOT: {project_root()}")
print(f"INPUT_PATH:   {INPUT_PATH}")
print(f"OUTPUT_PATH:  {OUTPUT_PATH}")
if is_cloud_storage(INPUT_PATH):
    print("File exists:  n/a (cloud URI)")
else:
    print(f"File exists:  {os.path.exists(INPUT_PATH)}")

schema = StructType([
    StructField("time", IntegerType(), True),
    StructField("source_user", StringType(), True),
    StructField("source_computer", StringType(), True),
])

extra = {
    "spark.sql.shuffle.partitions": os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", "2"),
    "spark.sql.adaptive.enabled": os.environ.get("SPARK_SQL_ADAPTIVE_ENABLED", "false"),
    "spark.sql.parquet.compression.codec": "snappy",
}

spark = build_spark_session(
    "Auth to Parquet",
    cloud_paths=[INPUT_PATH, OUTPUT_PATH],
    extra_config=extra,
)

df_auth = (
    spark.read
        .option("header", False)
        .option("compression", "bzip2")
        .schema(schema)
        .csv(INPUT_PATH)
)

print(f"Row count:  {df_auth.count()}")
df_auth.printSchema()
df_auth.show(5, truncate=False)

partitions = int(os.environ.get("AUTH_PARQUET_REPARTITION", "2"))
writer = df_auth.repartition(partitions) if partitions > 0 else df_auth
(
    writer.write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
)

print(f"Parquet written to: {OUTPUT_PATH}")

spark.stop()
