import json
import time
from google.cloud import pubsub_v1

project_id = 'modern-replica-387105'
topic_id = 'firsttopic'

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

for i in range(1, 6):  # Send 5 messages with incremented values
    message_data = {
		"rowkey": "rk6",
		"columns": [
			{
			"columnfamily": "cf1",
			"columnname": "field1",
			"columnvalue": "value1"
			},
			{
			"columnfamily": "cf1",
			"columnname": "field2",
			"columnvalue": "value5"
			},
			{
			"columnfamily": "cf1",
			"columnname": "field3",
			"columnvalue": "value3"
			}
		]
		}


    data = json.dumps(message_data).encode('utf-8')
    future = publisher.publish(topic_path, data)
    future.result()  # Wait for the message to be published

    print(f"Published message with incremented value {i}")
    time.sleep(1)  # Sleep for 1 second before sending the next message
