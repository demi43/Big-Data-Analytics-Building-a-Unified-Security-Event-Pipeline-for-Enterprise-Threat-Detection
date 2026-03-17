from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
INPUT_PATH = os.path.join(PROJECT_ROOT, "data", "lanl-auth-dataset-1.bz2")

schema=StructType([
    StructField("time", StringType(), True),
    StructField("user", StringType(), True),
    StructField("computer", StringType(), True),

])

spark=(
    SparkSession.builder
    .appName("Ingest Auth Logs")
    .getOrCreate()
)
spark 

df_pyspark=(spark.read
.option("header",False)
.schema(schema)
.option("compression","bzip2")
.csv(INPUT_PATH)
)

df_pyspark.printSchema()

df_pyspark.show(10,truncate=False)

print(type(df_pyspark))

print(df_pyspark.count())  # number of rows

print(df_pyspark.rdd.getNumPartitions())