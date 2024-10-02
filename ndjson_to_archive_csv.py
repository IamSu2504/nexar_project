import os
import gzip
import csv
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from datetime import datetime
import sys

BUCKET_NAME = 'sudemo_bucket'
client = storage.Client()
bucket = client.get_bucket(BUCKET_NAME)

"""
bachnh-30092024-get timestamp is existed in file .csv.gz
"""
def get_existing_timestamps(file_name_prefix):

    existing_timestamps = set()
    
    # Liệt kê các file .csv.gz trong bucket
    blobs = bucket.list_blobs(prefix='')
    for blob in blobs:
        if blob.name.endswith('.csv.gz') and file_name_prefix in blob.name:
            # Trích xuất timestamp từ tên file
            parts = blob.name.split('_')
            if len(parts) > 1:
                timestamp_part = parts[-1].replace('.csv.gz', '')
                existing_timestamps.add(timestamp_part)
    
    return existing_timestamps

"""
bachnh-30092024-convert ndjson to csv
"""
def process_ndjson_to_csv(file_blob, mode):
    try:
        # Get file in GCS
        file_name = file_blob.name
        local_file_path = f"/tmp/{file_name}"
        blob = bucket.blob(file_name)
        blob.download_to_filename(local_file_path)
        
        # Get existed timestamp
        existing_timestamps = get_existing_timestamps(file_name.split('.')[0])

        # Read ndjson
        timestamp_data = {}
        with open(local_file_path, 'r') as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as e:
                    print(f"Error in: {line}. Detail Error: {e}")
                    continue

                # Get timestamp in ndjson
                timestamp = record.get('timestamp')
                if timestamp:
                    if timestamp in existing_timestamps:
                        continue
                    if timestamp not in timestamp_data:
                        timestamp_data[timestamp] = []
                    timestamp_data[timestamp].append(record)

        # Create file csv and archive
        for timestamp, records in timestamp_data.items():
            if records:
				# Save record to csv file
                output_csv_path = f"/tmp/{file_name}_{timestamp}.csv"
                df = pd.DataFrame(records)
                df.to_csv(output_csv_path, index=False)

                # Archive
                compressed_file = output_csv_path + ".gz"
                with open(output_csv_path, 'rb') as f_in, gzip.open(compressed_file, 'wb') as f_out:
                    f_out.writelines(f_in)

                # Upload file to GCS
                blob_gz = bucket.blob(f"{file_name}_{timestamp}.csv.gz")
                blob_gz.upload_from_filename(compressed_file)
                print(f"Upload file {compressed_file} to GCS successfully.")
                os.remove(output_csv_path)
                os.remove(compressed_file)
        os.remove(local_file_path)

    except Exception as e:
        print(f"An error occurred while processing file: {file_blob.name}. Detail Error: {e}")

"""
bachnh-30092024-Create manual mode
"""
def manual_mode():
    # List ndjson in bucket
    blobs = bucket.list_blobs(prefix='')
    ndjson_files = [blob for blob in blobs if blob.name.endswith('.ndjson')]

    if not ndjson_files:
        print("Non ndjson files in the bucket.")
        return

    print("NDJSON LIST:")
    for idx, blob in enumerate(ndjson_files):
        print(f"{idx + 1}: {blob.name}")
    
    selected_index = None
    while selected_index is None:
        try:
            # input file's number
            selected_index = int(input("Input file's number: ")) - 1

            if selected_index < 0 or selected_index >= len(ndjson_files):
                print("Invalid sequence number. Please re-enter.")
                selected_index = None
        except ValueError:
            print("Invalid formatting. Please enter a valid integer.")
            selected_index = None

    # Get file is choosen
    selected_file = ndjson_files[selected_index]
    print(f"Processing file {selected_file.name}")
    process_ndjson_to_csv(selected_file, "manual")

"""
bachnh-30092024-Main
"""
def main():
    try:
        # mode/manual
        if len(sys.argv) > 1:
            mode = sys.argv[1]
        else:
            mode = "manual"  # Defualt

        if mode not in ["manual", "auto"]:
            print("Invalid mode. Use 'manual' or 'auto'.")
            return

        if mode == "manual":
            manual_mode()
        else:
            blobs = bucket.list_blobs(prefix='')
            ndjson_files = [blob for blob in blobs if blob.name.endswith('.ndjson')]

            if not ndjson_files:
                print("No ndjson file to handle.")
                return
            for blob in ndjson_files:
                print(blob.name)

            # Use multithreading to process each file concurrently
            with ThreadPoolExecutor(max_workers=5) as executor:
                executor.map(lambda blob: process_ndjson_to_csv(blob, mode), ndjson_files)

        print("Process Successfully.")
    
    except Exception as e:
        print(f"Error in main: {e}")

if __name__ == "__main__":
    main()

