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
        del os.environ["HADOOP_HOME"]
        return
    bin_dir = hh / "bin"
    if not bin_dir.is_dir():
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
    from pyspark.sql import SparkSession

    master = master or os.environ.get("SPARK_MASTER", "local[*]")
    b = SparkSession.builder.appName(app_name).master(master)

    paths = [p for p in cloud_paths if p]
    if any(is_cloud_storage(p) for p in paths):
        pkg = os.environ.get("SPARK_JARS_PACKAGES", "").strip()
        if not pkg:
            # Align with Spark 4 / Hadoop 3.4 (duration-style defaults). Override via .env if you use Spark 3.5 + 3.3.x only.
            pkg = "org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.367"
        b = b.config("spark.jars.packages", pkg)

        b = b.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        b = b.config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        b = b.config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        # Explicitly override Spark 4 defaults that use "s" suffixes
        b = b.config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        b = b.config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        b = b.config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        b = b.config("spark.hadoop.fs.s3a.connection.idle.time", "60000")
        b = b.config("spark.hadoop.fs.s3a.socket.timeout", "60000")

        # Fix for Hadoop 3.3.x multipart upload parsing issues
        # Set numeric values instead of duration strings
        b = b.config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000")  # 24 hours in ms
        b = b.config("spark.hadoop.fs.s3a.multipart.size", "104857600")  # 100MB
        b = b.config("spark.hadoop.fs.s3a.multipart.threshold", "104857600")  # 100MB

        access_key = os.environ.get("AWS_ACCESS_KEY_ID", "").strip()
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip()
        session_token = os.environ.get("AWS_SESSION_TOKEN", "").strip()

        # Spark/Hadoop 3.4 defaults may list AWS SDK v2 providers (software.amazon.awssdk.*).
        # hadoop-aws + aws-java-sdk-bundle only ship SDK v1 (com.amazonaws.*) — force v1 providers.
        if access_key and secret_key:
            b = b.config("spark.hadoop.fs.s3a.access.key", access_key)
            b = b.config("spark.hadoop.fs.s3a.secret.key", secret_key)
            if session_token:
                b = b.config("spark.hadoop.fs.s3a.session.token", session_token)
                b = b.config(
                    "spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
                )
            else:
                b = b.config(
                    "spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                )
        else:
            b = b.config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            )
    b = b.config("spark.local.dir", default_spark_local_dir())

    driver_mem = os.environ.get("SPARK_DRIVER_MEMORY", "").strip()
    if driver_mem:
        b = b.config("spark.driver.memory", driver_mem)
    else:
        # Default to conservative memory for local development
        b = b.config("spark.driver.memory", "2g")
    
    max_result = os.environ.get("SPARK_DRIVER_MAX_RESULT_SIZE", "").strip()
    if max_result:
        b = b.config("spark.driver.maxResultSize", max_result)
    else:
        # Limit result size for local development
        b = b.config("spark.driver.maxResultSize", "1g")

    # Conservative executor settings for local mode
    if master.startswith("local"):
        b = b.config("spark.executor.memory", "2g")
        b = b.config("spark.executor.cores", "2")
        b = b.config("spark.sql.shuffle.partitions", "4")  # Reduce from default

    if extra_config:
        for k, v in extra_config.items():
            b = b.config(k, v)

    return b.getOrCreate()