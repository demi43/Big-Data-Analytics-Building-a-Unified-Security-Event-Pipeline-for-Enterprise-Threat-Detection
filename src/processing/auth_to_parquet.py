import os
import sys

# Fix PySpark on Windows - must be set before importing SparkSession
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
INPUT_PATH = os.path.join(PROJECT_ROOT, "data", "lanl-auth-dataset-1.bz2")
OUTPUT_PATH = os.path.join(PROJECT_ROOT, "Parquet", "auth")

# Debug - verify paths
print(f"PROJECT_ROOT: {PROJECT_ROOT}")
print(f"INPUT_PATH:   {INPUT_PATH}")
print(f"OUTPUT_PATH:  {OUTPUT_PATH}")
print(f"File exists:  {os.path.exists(INPUT_PATH)}")

# Full LANL auth dataset schema
schema = StructType([
    StructField("time", IntegerType(), True),
    StructField("source_user", StringType(), True),
    StructField("source_computer", StringType(), True),
])

# Spark session with memory configs
spark = (
    SparkSession.builder
    .appName("Auth to Parquet")
    .master("local[1]")
    .config("spark.driver.memory", "8g")
    .config("spark.driver.maxResultSize", "2g")
    .config("spark.local.dir", "C:\\tmp\\spark")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)

# Read raw auth CSV (bzip2 compressed)
df_auth = (
    spark.read
        .option("header", False)
        .option("compression", "bzip2")
        .schema(schema)
        .csv(INPUT_PATH)
)

# Preview before writing
print(f"Row count:  {df_auth.count()}")
df_auth.printSchema()
df_auth.show(5, truncate=False)

# Write as Parquet - repartition to 2 to control memory
(
    df_auth
        .repartition(2)
        .write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
)

print(f" Parquet written to: {OUTPUT_PATH}")

spark.stop()

# --- To verify output later, uncomment this ---
# spark = SparkSession.builder.appName("Read Silver Auth").getOrCreate()
# auth_df = spark.read.parquet(OUTPUT_PATH)
# auth_df.printSchema()
# auth_df.show(10, truncate=False)