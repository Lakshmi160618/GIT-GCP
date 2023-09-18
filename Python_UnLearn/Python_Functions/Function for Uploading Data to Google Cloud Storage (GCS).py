from google.cloud import storage

def upload_to_gcs(bucket_name, source_file, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file)

# Call the function to upload a file to GCS
upload_to_gcs('my-bucket', 'local_file.txt', 'remote_file.txt')
'''
This example defines a function, upload_to_gcs, that uses the Google Cloud Storage client library to upload a local file to a specified GCS bucket.
'''