import pandas as pd

# Note: Uses your AWS environment variables for credentials
path = "s3://security-pipeline-lanl/gold/user_activity_summary"
df = pd.read_parquet(path)
print(df.head())