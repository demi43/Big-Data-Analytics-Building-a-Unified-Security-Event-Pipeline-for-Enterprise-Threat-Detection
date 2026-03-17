from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
INPUT_PATH = os.path.join(PROJECT_ROOT, "data", "flows.txt.gz")

schema=StructType([
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

spark=(
    SparkSession.builder
    .appName("ingest flows")
    .getOrCreate()
)
spark 

df_pyspark=(spark.read
.option("header",False)
.schema(schema)
.option("compression","gzip")
.csv(INPUT_PATH)
)

df_pyspark.printSchema()

df_pyspark.show(10,truncate=False)

print(type(df_pyspark))

print(df_pyspark.count())  # number of rows

print(df_pyspark.rdd.getNumPartitions())