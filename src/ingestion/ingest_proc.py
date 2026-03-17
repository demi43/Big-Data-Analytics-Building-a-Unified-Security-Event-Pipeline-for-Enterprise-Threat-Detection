from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
INPUT_PATH = os.path.join(PROJECT_ROOT, "data", "proc.txt.gz")

schema=StructType([
    StructField("time", IntegerType(), True),
    StructField("user@domain", StringType(), True),
    StructField("computer", StringType(), True),
    StructField("processname", StringType(), True),
    StructField("Start/End", StringType(), True),
])

spark=(
    SparkSession.builder
    .appName("ingest Proc")
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