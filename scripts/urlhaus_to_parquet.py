import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io

def write_json_to_s3(data) -> None:
    print("Converting JSON to Parquet and uploading to S3...")
    
    # Convert JSON → DataFrame
    pdf = pd.DataFrame(data)
    
    # Convert → PyArrow Table
    table = pa.Table.from_pandas(pdf)
    
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)
    
    s3 = boto3.client("s3")
    
    s3.put_object(
        Bucket="security-pipeline-lanl",
        Key="silver/urlhaus/urlhaus.parquet",
        Body=buffer,
    )
    
    print("Done.")
if __name__ == "__main__":
    print("Script started...")

    # Example test data (replace with your real JSON)
    data = [
        {"url": "example.com", "threat": "malware"},
        {"url": "bad.com", "threat": "phishing"}
    ]

    write_json_to_s3(data)