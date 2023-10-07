from google.cloud import storage

# Set your GCP project ID and the desired bucket name
project_id = 'dev-project-401115'
bucket_name = 'batch3gcp123'

# Initialize the storage client
storage_client = storage.Client(project=project_id)

# Create the GCS bucket
bucket = storage_client.create_bucket(bucket_name)

print(f'Bucket {bucket.name} created.')