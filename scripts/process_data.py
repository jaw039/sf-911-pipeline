import boto3
import json
import pandas as pd
import duckdb
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME = os.getenv('AWS_BUCKET_NAME')
DB_PATH = 'data/processed/sf_911.duckdb'

def list_s3_files(prefix='raw/'):
    s3 = boto3.client('s3')
    
    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
        if 'Contents' not in response:
            return []
        files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.json')]
        return files
    except Exception as e:
        print(f"Error listing S3 files: {e}")
        return []

def read_from_s3(key):
    s3 = boto3.client('s3')
    
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        data = json.loads(response['Body'].read())
        return data
    except Exception as e:
        print(f"Error reading from S3: {e}")
        return None

def clean_and_aggregate(data):
    if not data:
        return None
    
    df = pd.DataFrame(data)
    print(f"Raw records: {len(df)}")
    
    df['received_datetime'] = pd.to_datetime(df['received_datetime'], errors='coerce')
    df = df.dropna(subset=['received_datetime'])
    
    df['hour'] = df['received_datetime'].dt.floor('H')
    hourly_counts = df.groupby('hour').size().reset_index(name='call_count')
    hourly_counts = hourly_counts.sort_values('hour')
    
    print(f"Cleaned records: {len(df)}")
    print(f"Hourly aggregations: {len(hourly_counts)}")
    
    return hourly_counts

def load_to_duckdb(df):
    if df is None or len(df) == 0:
        print("No data to load")
        return
    
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = duckdb.connect(DB_PATH)
    
    con.execute("""
        CREATE TABLE IF NOT EXISTS hourly_calls (
            hour TIMESTAMP PRIMARY KEY,
            call_count INTEGER
        )
    """)
    
    hours_list = df['hour'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist()
    if hours_list:
        hours_str = "', '".join(hours_list)
        con.execute(f"DELETE FROM hourly_calls WHERE hour IN ('{hours_str}')")
    
    con.execute("INSERT INTO hourly_calls SELECT * FROM df")
    
    result = con.execute("SELECT COUNT(*) FROM hourly_calls").fetchone()
    print(f"Total hours in database: {result[0]}")
    
    con.close()

def main():
    print("Starting data processing...")
    
    files = list_s3_files('raw/')
    
    if not files:
        print("No files found in S3")
        return
    
    print(f"Found {len(files)} files")
    latest_file = sorted(files)[-1]
    print(f"Processing: {latest_file}")
    
    data = read_from_s3(latest_file)
    hourly_data = clean_and_aggregate(data)
    load_to_duckdb(hourly_data)
    
    print("Pipeline complete")

if __name__ == "__main__":
    main()