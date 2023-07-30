from google.cloud import pubsub_v1

def publish_message(project_id, topic_name, message):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)

    # Convert the message to bytes
    data = message.encode('utf-8')

    # Publish the message
    future = publisher.publish(topic_path, data)
    message_id = future.result()

    print(f"Published message ID: {message_id}")

# Set your project ID, topic name, and message
project_id = 'your-project-id'
topic_name = 'your-topic-name'
message = 'Hello, Pub/Sub!'

# Publish the message
publish_message(project_id, topic_name, message)
