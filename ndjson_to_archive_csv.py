import os
import gzip
import csv
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from datetime import datetime

# Google Cloud Storage settings
BUCKET_NAME = 'your_bucket_name'

# Initialize Google Cloud Storage client
client = storage.Client()
bucket = client.get_bucket(BUCKET_NAME)

def process_ndjson_to_csv(file_blob):
    # Download the ndjson file from GCS
    file_name = file_blob.name
    blob = bucket.blob(file_name)
    local_file_path = f"/tmp/{file_name}"
    blob.download_to_filename(local_file_path)
    
    # Load existing CSV data (if any) and track timestamps
    csv_file_name = f"{file_name}.csv"
    existing_timestamps = set()
    if os.path.exists(f"/tmp/{csv_file_name}"):
        existing_df = pd.read_csv(f"/tmp/{csv_file_name}")
        existing_timestamps = set(existing_df['timestamp'])
    
    # Read ndjson and convert to CSV for new records
    new_records = []
    with open(local_file_path, 'r') as f:
        for line in f:
            record = json.loads(line)
            if record['timestamp'] not in existing_timestamps:
                new_records.append(record)
    
    if new_records:
        # Create DataFrame from new records and save to CSV
        df = pd.DataFrame(new_records)
        output_csv_path = f"/tmp/{file_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        df.to_csv(output_csv_path, index=False)

        # Compress CSV to gzip
        compressed_file = output_csv_path + ".gz"
        with open(output_csv_path, 'rb') as f_in, gzip.open(compressed_file, 'wb') as f_out:
            f_out.writelines(f_in)
        
        # Upload compressed CSV back to GCS
        blob_gz = bucket.blob(f"{file_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv.gz")
        blob_gz.upload_from_filename(compressed_file)
        
        print(f"Uploaded compressed CSV: {compressed_file} to GCS")
    
    # Clean up temporary files
    os.remove(local_file_path)
    if os.path.exists(output_csv_path):
        os.remove(output_csv_path)
    if os.path.exists(compressed_file):
        os.remove(compressed_file)

def main():
    # List all ndjson files in the bucket
    blobs = bucket.list_blobs(prefix='path/to/ndjson/files/')
    ndjson_files = [blob for blob in blobs if blob.name.endswith('.ndjson')]

    # Use multithreading to process each file concurrently
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(process_ndjson_to_csv, ndjson_files)

if __name__ == "__main__":
    main()

