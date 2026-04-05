"""Resolve bronze (raw) and silver (Parquet) locations from .env / defaults."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_ROOT / ".env", override=False)


def project_root() -> Path:
    return _ROOT


def data_dir_path() -> Path:
    return _ROOT / os.environ.get("DATA_DIR", "data")


def resolve_input(uri_env: str, default_filename: str) -> str:
    """Use URI env if set (local path, s3://, or s3a://), else <DATA_DIR>/<default_filename>."""
    explicit = os.environ.get(uri_env, "").strip()
    if explicit:
        return explicit
    return str(data_dir_path() / default_filename)


def resolve_parquet_output(subdir: str) -> str:
    """If SILVER_PARQUET_URI is set, return <uri>/<subdir>; else <PARQUET_OUTPUT_ROOT or Parquet>/<subdir>."""
    silver = os.environ.get("SILVER_PARQUET_URI", "").strip().rstrip("/")
    if silver:
        return f"{silver}/{subdir}"
    root = os.environ.get("PARQUET_OUTPUT_ROOT", "").strip()
    base = Path(root) if root else _ROOT / "Parquet"
    return str(base / subdir)
