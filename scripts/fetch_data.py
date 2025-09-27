import requests
import json
import boto3
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

API_ENDPOINT = os.getenv('SF_API_ENDPOINT')
APP_TOKEN = os.getenv('SF_APP_TOKEN')
BUCKET_NAME = os.getenv('AWS_BUCKET_NAME')
AWS_REGION = os.getenv('AWS_REGION')

def fetch_sf_911_data():
    # fetches the last 48hrs of 911 calls from SF DataSF API
    ## Returns raw JSON Data:
    ## received_datetime: when called received
    ## call_type_original_desc: type of call
    ## agency: police/fire/medical/sheriff
    
    start_time = (datetime.now() - timedelta(hours=48)).strftime('%Y-%m-%dT%H:%M:%S')

    print (f"Fetching 911 calls since {start_time}")

    # API Parameters using SoQL
    params = {
        '$where': f"received_datetime > '{start_time}'",
        '$limit': 50000,
        '$order': 'received_datetime DESC'
    }

    try:
        # Add authentication headers
        headers = {}
        if APP_TOKEN:
            headers['X-App-Token'] = APP_TOKEN
        
        response = requests.get(API_ENDPOINT, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        print(f"Fetched {len(data)} records")
        return data
        
    except Exception as e:
        print(f"Error fetching data: {e}")
        print(f"Response status: {response.status_code if 'response' in locals() else 'No response'}")
        if 'response' in locals():
            print(f"Response text: {response.text[:200]}...")
        return None

def save_to_s3(data):
    if not data:
        return False
    
    try:
        s3 = boto3.client('s3', region_name=AWS_REGION)
        timestamp = datetime.now()
        s3_key = f"raw/{timestamp.strftime('%Y/%m/%d/%H')}/calls_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(data, indent=2),
            ContentType='application/json'
        )
        
        print(f"Saved to S3: {s3_key}")
        return True
    except Exception as e:
        print(f"Error saving to S3: {e}")
        return False

def save_locally(data):
    if not data:
        return False
    
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filepath = f"data/raw/calls_{timestamp}.json"
        os.makedirs('data/raw', exist_ok=True)
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"Local backup: {filepath}")
        return True
    except Exception as e:
        print(f"Error saving locally: {e}")
        return False

def main():
    print("Starting data fetch...")
    
    data = fetch_sf_911_data()
    
    if data:
        s3_success = save_to_s3(data)
        save_locally(data)
        
        if s3_success:
            print("Pipeline complete")
        else:
            print("Warning: S3 upload failed")
    else:
        print("Failed to fetch data")

if __name__ == "__main__":
    main()



