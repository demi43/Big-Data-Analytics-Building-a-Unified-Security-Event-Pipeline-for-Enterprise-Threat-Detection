"""Upload URLHaus-shaped rows to S3 as a single Parquet object (boto3)."""

import io
import os
import sys

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"), override=False)


def write_json_to_s3(data: list) -> None:
    bucket = os.environ.get("S3_URLHAUS_BUCKET", "").strip()
    key = os.environ.get("S3_URLHAUS_KEY", "silver/urlhaus/urlhaus.parquet").strip().lstrip("/")
    if not bucket:
        print("Set S3_URLHAUS_BUCKET in .env (and optionally S3_URLHAUS_KEY).", file=sys.stderr)
        sys.exit(1)

    print(f"Converting JSON to Parquet and uploading to s3://{bucket}/{key} ...")

    pdf = pd.DataFrame(data)
    table = pa.Table.from_pandas(pdf)

    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)

    region = os.environ.get("AWS_REGION", "").strip()
    s3 = boto3.client("s3", region_name=region or None)
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())

    print("Done.")


if __name__ == "__main__":
    print("Script started...")

    data = [
        {"url": "example.com", "threat": "malware"},
        {"url": "bad.com", "threat": "phishing"},
    ]

    write_json_to_s3(data)
