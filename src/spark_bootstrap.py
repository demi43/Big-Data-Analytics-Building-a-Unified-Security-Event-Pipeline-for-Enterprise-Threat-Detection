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
        b = b.config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        b = b.config("spark.hadoop.fs.s3a.socket.timeout", "60000")
        # Spark 4 can merge Hadoop 3.4 defaults like fs.s3a.threads.keepalivetime=60s; older hadoop-aws used getLong() → NFE.
        b = b.config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")

        access_key = os.environ.get("AWS_ACCESS_KEY_ID", "").strip()
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip()

        if access_key and secret_key:
            b = b.config("spark.hadoop.fs.s3a.access.key", access_key)
            b = b.config("spark.hadoop.fs.s3a.secret.key", secret_key)
        else:
            b = b.config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            )
    b = b.config("spark.local.dir", default_spark_local_dir())

    driver_mem = os.environ.get("SPARK_DRIVER_MEMORY", "").strip()
    if driver_mem:
        b = b.config("spark.driver.memory", driver_mem)
    max_result = os.environ.get("SPARK_DRIVER_MAX_RESULT_SIZE", "").strip()
    if max_result:
        b = b.config("spark.driver.maxResultSize", max_result)

    if extra_config:
        for k, v in extra_config.items():
            b = b.config(k, v)

    return b.getOrCreate()