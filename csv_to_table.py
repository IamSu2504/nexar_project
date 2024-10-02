import sys
import gzip
import re
import csv
import concurrent.futures
from google.cloud import storage, bigquery
from io import BytesIO, StringIO, TextIOWrapper

project_id = 'suproject-436720'
dataset_id = 'Su_demo'

bucket_name = 'sudemo_bucket'
"""
bachnh-01102024-to get timestamp from CSV file name
"""
def extract_timestamp_from_filename(file_name):
    match = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", file_name)
    if match:
        return match.group(1)
    return None

"""
bachnh-01102024-gets the table name from the CSV file name
"""
def get_table_name_from_filename(file_name):
    table_name = re.sub(r"(\.\w+_?\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\.csv\.gz", '', file_name)
    table_name = re.sub(r"\.csv\.gz$", '', table_name)
    table_name = re.sub(r"\.csv$", '', table_name)
    return table_name

"""
bachnh-01102024-check timestamp in bigquery
"""
def check_timestamp_in_bigquery(bq_client, dataset_id, table_name, timestamp):
    query = f"""
    SELECT COUNT(*) as cnt FROM `{project_id}.{dataset_id}.{table_name}`
    WHERE timestamp = '{timestamp}'
    """
    query_job = bq_client.query(query)
    results = query_job.result()
    for row in results:
        return row.cnt > 0
    return False

"""
bachnh-01102024-create a new table on BigQuery if the table doesn't already exist
"""
def create_table_if_not_exists(bq_client, dataset_id, table_name, csv_reader):
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_name)
    
    try:
        bq_client.get_table(table_ref)
        return False 
    except Exception:
        print(f"Table {table_name} does not exist. Creating new table...")
    
    headers = next(csv_reader)
    schema = [bigquery.SchemaField(header, 'STRING') for header in headers]
    
    table = bigquery.Table(table_ref, schema=schema)
    bq_client.create_table(table)
    print(f"Created table {table_name}.")
    return True

"""
bachnh-01102024-import CSV data into BigQuery
"""
def load_data_to_bigquery(bq_client, dataset_id, table_name, csv_reader):
    table_ref = bq_client.dataset(dataset_id).table(table_name)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False 
    )

    # Create a buffer to save CSV data
    csv_stream = StringIO()
    writer = csv.writer(csv_stream)
    
    writer.writerows(csv_reader)

    #Reset the cursor
    csv_stream.seek(0)
    
    # Insert data to BigQuery
    job = bq_client.load_table_from_file(csv_stream, table_ref, job_config=job_config)
    job.result()

"""
bachnh-01102024-processing CSV files from GCS: extracting and importing data
"""
def process_csv_file(gcs_client, bq_client, bucket_name, file_path, dataset_id):
    print(f"Processing file: {file_path}")
    
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    
    # Download .gz files directly from GCS to storage
    gz_stream = BytesIO()
    blob.download_to_file(gz_stream)
    gz_stream.seek(0)  # Reset cursor
    
    # Extract the file and read the CSV data
    with gzip.open(gz_stream, 'rb') as f:
        csv_reader = csv.reader(TextIOWrapper(f, 'utf-8'))
        
        # get timestamp from file and table
        timestamp = extract_timestamp_from_filename(file_path)
        table_name = get_table_name_from_filename(file_path)
        
        # check table if existed
        if create_table_if_not_exists(bq_client, dataset_id, table_name, csv_reader):
            load_data_to_bigquery(bq_client, dataset_id, table_name, csv_reader)
        else:
            # If the table already exists, check the timestamp
            if not check_timestamp_in_bigquery(bq_client, dataset_id, table_name, timestamp):
                # Import data if timestamp does not exist
                load_data_to_bigquery(bq_client, dataset_id, table_name, csv_reader)
            else:
                print(f"Skipping file {file_path}.")

"""
bachnh-01102024-lists all the files in the GCS bucket
"""
def list_files_in_bucket(gcs_client, bucket_name):
    bucket = gcs_client.bucket(bucket_name)
    blobs = bucket.list_blobs()
    file_paths = [blob.name for blob in blobs if blob.name.endswith('.csv.gz')]
    return file_paths

"""
bachnh-01102024-manual mode: select file to import from GCS
"""
def manual_mode(gcs_client, bq_client):
    file_paths = list_files_in_bucket(gcs_client, bucket_name)    
    if not file_paths:
        print("Non .csv.gz file in bucket.")
        return    
    while True:
        print("\nList Files:")
        for idx, file_path in enumerate(file_paths):
            print(f"{idx}: {file_path}")
        
        try:
            file_index = int(input("Input file's number: "))
            if 0 <= file_index < len(file_paths):
                process_csv_file(gcs_client, bq_client, bucket_name, file_paths[file_index], dataset_id)
                break
            else:
                print("Invalid sequence number. Please re-enter.")
        except ValueError:
            print("Invalid formatting. Please enter a valid integer.")

"""
bachnh-01102024-auto mode: auto import files
"""
def automatic_mode(gcs_client, bq_client):
    file_paths = list_files_in_bucket(gcs_client, bucket_name)
    
    if not file_paths:
        print("Non .csv.gz file in bucket.")
        return
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(process_csv_file, gcs_client, bq_client, bucket_name, file_path, dataset_id)
            for file_path in file_paths
        ]
        for future in concurrent.futures.as_completed(futures):
            future.result()

"""
bachnh-01102024-main
"""
def main():
    try:
        gcs_client = storage.Client()
        bq_client = bigquery.Client(project=project_id)
        
        if len(sys.argv) > 1:
            mode = sys.argv[1].strip().lower()
        else:
            mode = "manual"  # Default: Manual
    
        if mode == "auto" or mode == "automatic":
            automatic_mode(gcs_client, bq_client)
        else:
            manual_mode(gcs_client, bq_client)
        print("Process Successfully.")
    except Exception as e:
        print(f"Error in main: {e}")
if __name__ == "__main__":
    main()

