import os
from pathlib import Path
import sys

# Ensure parent directory is in path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent)) 

from pipeline_paths import resolve_parquet_output, resolve_gold_output
from spark_bootstrap import build_spark_session, is_cloud_storage
from pyspark.sql import functions as F

# 1. Resolve Silver Paths
SILVER = resolve_parquet_output  # shorthand function
input_paths = [
    SILVER("flows"), 
    SILVER("proc"), 
    SILVER("auth"),
    SILVER("dns"), 
    SILVER("urlhaus"),
]

# 2. Build Spark Session
# The cloud_paths argument is critical here; it triggers the S3A/Vectored IO fixes 
# inside your updated spark_bootstrap.py
spark = build_spark_session("enrich-gold", cloud_paths=input_paths)

# Verification check (optional)
print(f"Spark Version: {spark.version}")
print(f"Vectored IO Enabled: {spark.conf.get('spark.sql.parquet.enableVectorizedReader')}")

# 3. Read Parquet Files
flows_df   = spark.read.parquet(SILVER("flows"))
proc_df    = spark.read.parquet(SILVER("proc"))
auth_df    = spark.read.parquet(SILVER("auth"))
dns_df     = spark.read.parquet(SILVER("dns"))
urlhaus_df = spark.read.parquet(SILVER("urlhaus"))

# ── Deduplicate lookup tables before joining to prevent row fanout ────────────
auth_dedup = (
    auth_df
    .select("source_computer", "time", "source_user")
    .dropDuplicates(["source_computer", "time"])
)

proc_dedup = (
    proc_df
    .select("computer", "time", "process_name")
    .dropDuplicates(["computer", "time"])
)

# Extract host from URL for URLHaus
urlhaus_dedup = (
    urlhaus_df
    .withColumn("host", F.regexp_extract("url", r"https?://([^/]+)", 1))
    .select("host", "threat")
    .dropDuplicates(["host"])
)

# ── Alias and Join logic ──────────────────────────────────────────────────────
flows = flows_df.alias("flows")

# Join flows -> auth
flows_with_user = (
    flows.join(
        auth_dedup.alias("auth"),
        (F.col("flows.src_computer") == F.col("auth.source_computer")) &
        (F.col("flows.time")         == F.col("auth.time")),
        "left"
    )
    .select(
        F.col("flows.*"),
        F.col("auth.source_user").alias("user")
    )
    .alias("fwu")
)

# Join -> processes
flows_with_process = (
    flows_with_user.join(
        proc_dedup.alias("proc"),
        (F.col("fwu.src_computer") == F.col("proc.computer")) &
        (F.col("fwu.time")         == F.col("proc.time")),
        "left"
    )
    .select(
        F.col("fwu.*"),
        F.col("proc.process_name")
    )
    .alias("fwp")
)

# Join -> URLHaus
flows_enriched = (
    flows_with_process.join(
        urlhaus_dedup.alias("ioc"),
        F.col("fwp.dst_computer") == F.col("ioc.host"),
        "left"
    )
    .withColumn(
        "malicious_hit",
        F.when(F.col("ioc.threat").isNotNull(), 1).otherwise(0)
    )
    .alias("enriched")
)

# ── Aggregation to Gold ──────────────────────────────────────────────────────
TIME_BUCKET = 3600

gold_df = (
    flows_enriched
    .withColumn("time_bucket", (F.col("time") / TIME_BUCKET).cast("long") * TIME_BUCKET)
    .groupBy("time_bucket", "user", "src_computer")
    .agg(
        F.count("*")                    .alias("total_flows"),
        F.sum("bytes_count")            .alias("total_bytes"),
        F.sum("packets_count")          .alias("total_packets"),
        F.countDistinct("process_name") .alias("active_processes"),
        F.sum("malicious_hit")          .alias("malicious_hits")
    )
)

# ── Write gold layer ──────────────────────────────────────────────────────────
# Note: resolve_gold_output should return an S3 path if writing back to cloud
output_path = resolve_gold_output("user_activity_summary")
print(f"Writing Gold Layer to: {output_path}")

(
    gold_df.write
    .mode("overwrite")
    .partitionBy("time_bucket")
    .parquet(output_path)
)

print("Job completed successfully.")