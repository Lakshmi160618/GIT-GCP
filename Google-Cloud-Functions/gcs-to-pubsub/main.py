import base64
import json
from google.cloud import pubsub_v1


def process_file(data, context):
    """Triggered by a change to a Cloud Storage bucket."""
    # Get bucket and file details
    bucket_name = data['bucket']
    file_name = data['name']
    
    # Check if the object is finalized
    if 'metageneration' in data and data['metageneration'] == '1':
        # Send a message to Pub/Sub topic
        pubsub_client = pubsub_v1.PublisherClient()
        topic_name = 'projects/rawdata-382314/topics/gcstopubsub'
        message = {'bucket': bucket_name, 'file': file_name}
        message_data = json.dumps(message).encode('utf-8')
        pubsub_client.publish(topic_name, message_data)
        print('Message published to Pub/Sub topic.')
    else:
        print('Object is not finalized.')
