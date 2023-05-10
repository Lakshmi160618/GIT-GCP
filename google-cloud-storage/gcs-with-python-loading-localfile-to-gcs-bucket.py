from google.cloud import storage

def upload_csv_to_gcs(bucket_name, local_file_path, destination_blob_name):
    storage_client = storage.Client.from_service_account_json('C:/Users/Umeshyam/Downloads/service_key.json')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_file_path)
    print(f'File {local_file_path} uploaded to {destination_blob_name} in bucket {bucket_name}.')

def main():
    # Specify the name of your GCS bucket
    bucket_name = 'source_bucket_ns'
   
    # Specify the local path to the CSV file
    local_file_path = 'C:/Users/Umeshyam/Documents/mergeQuery.sql'
   
    # Specify the desired destination blob name in the GCS bucket
    destination_blob_name = 'mergeQuery.sql'
   
    # Call the function to upload the file
    upload_csv_to_gcs(bucket_name, local_file_path, destination_blob_name)

if __name__ == '__main__':
    main()
