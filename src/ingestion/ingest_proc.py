import os
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parent.parent
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from pipeline_paths import project_root, resolve_input
from spark_bootstrap import build_spark_session, is_cloud_storage

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

INPUT_PATH = resolve_input("PROC_INPUT_URI", "proc.txt.gz")

print(f"PROJECT_ROOT: {project_root()}")
print(f"INPUT_PATH:   {INPUT_PATH}")
print(
    "File exists:  "
    + ("n/a (cloud URI)" if is_cloud_storage(INPUT_PATH) else str(os.path.exists(INPUT_PATH)))
)

schema = StructType([
    StructField("time", IntegerType(), True),
    StructField("user_domain", StringType(), True),
    StructField("computer", StringType(), True),
    StructField("process_name", StringType(), True),
    StructField("start_end", StringType(), True),
])

spark = build_spark_session("Ingest Proc Logs", cloud_paths=[INPUT_PATH])

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
