import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
import boto3
import os

# Configuration
# Replace with your actual S3 Bucket name (or keep local mock mode if no AWS credentials set)
S3_BUCKET_NAME = os.getenv("SCADA_S3_BUCKET", "malaysia-offshore-scada-lake")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-1") # Singapore / KL region

def export_to_parquet_s3(
    input_path="/opt/airflow/data/gold_scada_processed.csv",
    local_output_dir="/opt/airflow/data/parquet_lake"
):
    """
    Converts processed SCADA CSV into columnar Parquet files and uploads them to S3
    structured with Hive-style partitioning: field_location/year/month/
    """
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Processed Gold file missing at {input_path}")

    df = pd.read_csv(input_path)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Extract date attributes for Hive partitioning
    now = datetime.now(timezone.utc)
    year_str = f"{now.year}"
    month_str = f"{now.month:02d}"

    print(f"📦 Converting {len(df)} records to columnar Parquet format...")

    # Write local partitioned Parquet dataset
    for field in df['field_location'].unique():
        field_df = df[df['field_location'] == field].copy()
        
        # S3 Key structure: raw_scada/field_location=Kasawari/year=2026/month=08/
        partition_path = os.path.join(
            local_output_dir, 
            f"field_location={field}", 
            f"year={year_str}", 
            f"month={month_str}"
        )
        os.makedirs(partition_path, exist_ok=True)
        
        file_name = f"scada_telemetry_{now.strftime('%Y%m%d_%H%M%S')}.parquet"
        file_full_path = os.path.join(partition_path, file_name)
        
        # Save Parquet using Snappy compression
        field_df.to_parquet(file_full_path, engine='pyarrow', compression='snappy', index=False)
        print(f"  └─ Saved Parquet file: {file_full_path}")

        # Upload to AWS S3 using Boto3
        s3_key = f"gold/scada/field_location={field}/year={year_str}/month={month_str}/{file_name}"
        upload_to_s3(file_full_path, S3_BUCKET_NAME, s3_key)

def upload_to_s3(file_path, bucket, s3_key):
    """Uploads local file to Amazon S3 bucket if credentials are available."""
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        s3_client.upload_file(file_path, bucket, s3_key)
        print(f"🚀 Uploaded to S3: s3://{bucket}/{s3_key}")
    except Exception as e:
        print(f"ℹ️ AWS S3 upload skipped ({e}). Local Parquet parquet copy created successfully.")

if __name__ == "__main__":
    export_to_parquet_s3()