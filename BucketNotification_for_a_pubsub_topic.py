from google.cloud import storage
from google.cloud.storage.notification import BucketNotification

def create_bucket_notification(bucket_name, topic_name):
    """Creates a notification configuration for a bucket."""
    try:
        storage_client = storage.Client.from_service_account_json('/home/gcp/jsonkeys/gcp-training-bce673a8743c.json')
        bucket = storage_client.get_bucket(bucket_name)

        # Create a new BucketNotification instance with the topic name, event types, and payload format
        notification = BucketNotification(
            bucket,
            topic_name=topic_name,
            topic_project=None,
            custom_attributes={'file_type': 'image', 'owner': 'lakshmi@example.com'},
            event_types=['OBJECT_FINALIZE', 'OBJECT_DELETE'],
            payload_format="JSON_API_V1",
            notification_id=None,
        )

        # Create the notification
        notification.create()

        print(f"Successfully created notification for bucket {bucket_name}")
        print(f"Successfully created notification with ID {notification.notification_id} for bucket {bucket_name}")
    except Exception as e:
        print(f"Error creating bucket notification: {str(e)}")

def list_bucket_notifications(bucket_name):
    """Lists notification configurations for a bucket."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        notifications = bucket.list_notifications()

        for notification in notifications:
            notification_id = notification.notification_id
            print(f"Notification ID: {notification.notification_id}")
    except Exception as e:
        print(f"Error listing bucket notifications: {str(e)}")

def print_pubsub_bucket_notification(bucket_name, notification_id):
    """Gets a notification configuration for a bucket."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        notification = bucket.get_notification(notification_id)

        print(f"Notification ID: {notification.notification_id}")
        print(f"Topic Name: {notification.topic_name}")
        print(f"Event Types: {notification.event_types}")
        print(f"Custom Attributes: {notification.custom_attributes}")
        print(f"Payload Format: {notification.payload_format}")
        print(f"Blob Name Prefix: {notification.blob_name_prefix}")
        print(f"Etag: {notification.etag}")
        print(f"Self Link: {notification.self_link}")
    except Exception as e:
        print(f"Error printing bucket notification: {str(e)}")

# Specify the bucket name and Pub/Sub topic name
bucket_name = "bucket_name"
topic_name = "topic_name"

# Create the bucket notification
notification_id = ""
# Create the bucket notification
create_bucket_notification(bucket_name, topic_name)
list_bucket_notifications(bucket_name)
print_pubsub_bucket_notification(bucket_name, notification_id)



'''
# it is to create a topic in the gcp cloud 

gcloud pubsub topics create projects/project_id/topics/topic_name

# it is to provide necessory permission to a service accounts
# Grant Pub/Sub Publisher Role:
 Give the service account the roles/pubsub.publisher role, which allows it 
 to publish messages to Pub/Sub topics.

You can grant this role at the project level or at the topic level depending on your requirements. Granting at the project level gives the service account permissions to publish to any topic in the project.

Grant at the project level:
==============================
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:service-671505287155@gs-project-accounts.iam.gserviceaccount.com" \
  --role="roles/pubsub.publisher"
  
Grant at the topic level:
==========================
gcloud pubsub topics add-iam-policy-binding projects/project_id/topics/topic_name \
  --member="serviceAccount:service-671505287155@gs-project-accounts.iam.gserviceaccount.com" \
  --role="roles/pubsub.publisher"'''
  
  
 # finally check in the pub/sub subscriptions