from google.cloud import storage

def gcs_to_gcs(event, context):
	def copy_bucket_objects(source_bucket_name, destination_bucket_name):
		# Instantiate the client
		client = storage.Client()
	
		# Get the source and destination buckets
		source_bucket = client.get_bucket(source_bucket_name)
		destination_bucket = client.get_bucket(destination_bucket_name)
	
		# List all objects in the source bucket
		blobs = source_bucket.list_blobs()
	
		# Copy each object to the destination bucket
		for blob in blobs:
			destination_blob_name = blob.name
			source_bucket.copy_blob(blob, destination_bucket, destination_blob_name)
	
		print("Objects copied successfully!")
	
	# Usage: Provide the source and destination bucket names
	source_bucket_name = "source_bucket_ns"
	destination_bucket_name = "destination_bucket_ns"
	
	copy_bucket_objects(source_bucket_name, destination_bucket_name)