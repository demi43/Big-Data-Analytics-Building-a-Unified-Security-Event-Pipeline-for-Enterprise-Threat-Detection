"""PySpark session setup: Windows driver fix, optional Hadoop home, optional S3A."""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path
from typing import Iterable


def fix_windows_pyspark() -> None:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def _normalized_hadoop_home(raw: str) -> Path | None:
    """HADOOP_HOME must be the distro root (folder that contains bin/), not .../bin."""
    p = Path(raw.strip()).expanduser()
    try:
        p = p.resolve()
    except OSError:
        p = Path(raw.strip()).expanduser()
    if p.name.lower() == "bin" and p.is_dir():
        p = p.parent
    if not p.is_dir():
        return None
    return p


def apply_optional_hadoop_home() -> None:
    raw = os.environ.get("HADOOP_HOME", "").strip()
    if not raw:
        return
    hh = _normalized_hadoop_home(raw)
    if hh is None:
        if "HADOOP_HOME" in os.environ:
            del os.environ["HADOOP_HOME"]
        return
    bin_dir = hh / "bin"
    if not bin_dir.is_dir():
        if "HADOOP_HOME" in os.environ:
            del os.environ["HADOOP_HOME"]
        return
    os.environ["HADOOP_HOME"] = str(hh)
    path = os.environ.get("PATH", "")
    os.environ["PATH"] = path + os.pathsep + str(bin_dir)


def default_spark_local_dir() -> str:
    return os.environ.get("SPARK_LOCAL_DIR") or str(
        Path(tempfile.gettempdir()) / "spark-local"
    )


def is_cloud_storage(path: str) -> bool:
    if not isinstance(path, str):
        return False
    p = path.lower()
    return p.startswith("s3://") or p.startswith("s3a://")


def build_spark_session(
    app_name: str,
    *,
    master: str | None = None,
    cloud_paths: Iterable[str] = (),
    extra_config: dict[str, str] | None = None,
):
    fix_windows_pyspark()
    apply_optional_hadoop_home()

    # CORE FIX for NoSuchMethodError
    jvm_flags = (
        "-Dparquet.hadoop.vectored.io.enabled=false "
        "-Dfs.s3a.vectoredreads.enabled=false "
        "-Dparquet.vectored.io.enabled=false"
    )
    
    os.environ["SPARK_SUBMIT_OPTS"] = os.environ.get("SPARK_SUBMIT_OPTS", "") + " " + jvm_flags

    from pyspark.sql import SparkSession

    master = master or os.environ.get("SPARK_MASTER", "local[*]")
    b = SparkSession.builder.appName(app_name).master(master)

    b = b.config("spark.driver.extraJavaOptions", jvm_flags)
    b = b.config("spark.executor.extraJavaOptions", jvm_flags)

    paths = [p for p in cloud_paths if p]
    if any(is_cloud_storage(p) for p in paths):
        # Use 3.3.4 for internal compatibility
        pkg = (
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        b = b.config("spark.jars.packages", pkg)

        b = b.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        b = b.config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        
        # Purge age (The "24h" fix) - Set to 86400 seconds (24 hours)
        b = b.config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
        
        # --- CRITICAL FIX FOR NumberFormatException: "60s" ---
        # We must override the Hadoop defaults that use "s" suffixes
        b = b.config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        b = b.config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        b = b.config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        b = b.config("spark.hadoop.fs.s3a.socket.timeout", "60000")
        b = b.config("spark.hadoop.fs.s3a.connection.idle.time", "60000")
        # ----------------------------------------------------

        b = b.config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        
        # Disable Vectored I/O
        b = b.config("spark.hadoop.fs.s3a.vectored.read.enabled", "false")
        b = b.config("spark.hadoop.fs.s3a.vectored.active.count", "0")
        b = b.config("spark.sql.parquet.enableVectorizedReader", "false")
        b = b.config("spark.sql.parquet.vectoredParquetReader.enabled", "false")
        b = b.config("spark.hadoop.parquet.vectored.io.enabled", "false")

        # AWS Credentials
        access_key = os.environ.get("AWS_ACCESS_KEY_ID", "").strip()
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip()
        session_token = os.environ.get("AWS_SESSION_TOKEN", "").strip()

        if access_key and secret_key:
            b = b.config("spark.hadoop.fs.s3a.access.key", access_key)
            b = b.config("spark.hadoop.fs.s3a.secret.key", secret_key)
            if session_token:
                b = b.config("spark.hadoop.fs.s3a.session.token", session_token)
                b = b.config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
            else:
                b = b.config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        else:
            b = b.config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

    b = b.config("spark.local.dir", default_spark_local_dir())
    b = b.config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
    b = b.config("spark.driver.maxResultSize", os.environ.get("SPARK_DRIVER_MAX_RESULT_SIZE", "1g"))

    if master.startswith("local"):
        b = b.config("spark.executor.memory", "2g")
        b = b.config("spark.executor.cores", "2")
        b = b.config("spark.sql.shuffle.partitions", "4")

    if extra_config:
        for k, v in extra_config.items():
            b = b.config(k, v)

    return b.getOrCreate()