from pyspark.sql import SparkSession
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
INPUT_PATH = os.path.join(PROJECT_ROOT, "data", "lanl-auth-dataset-1.bz2")
OUTPUT_PATH = os.path.join(PROJECT_ROOT, "parquet", "auth")

spark = (
    SparkSession.builder
    .appName("Auth to Parquet")
    .getOrCreate()
)

# Read raw auth (exactly like your ingestion script)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("time", IntegerType(), True),
    StructField("user", StringType(), True),
    StructField("computer", StringType(), True),
])

df_auth = (
    spark.read
        .option("header", "false")
        .option("compression", "bzip2")
        .schema(schema)
        .csv(INPUT_PATH)
)

# Write Silver layer as Parquet
(
    df_auth
        .write
        .mode("overwrite")              # overwrite for now; later you might use append
        .parquet(OUTPUT_PATH)
)

spark.stop()

# spark = SparkSession.builder.appName("Read Silver Auth").getOrCreate()
# auth_df = spark.read.parquet("data/silver/auth")
# auth_df.printSchema()
# auth_df.show(10, truncate=False)