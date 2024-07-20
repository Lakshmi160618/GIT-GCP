import requests
import json

event_data = {
    "id": "1234",
    "type": "google.cloud.storage.object.v1.finalized",
    "data": {
        "bucket": "my-bucket",
        "name": "my-file.txt"
    }
}

headers = {
    'Content-Type': 'application/json',
    'Ce-Id': event_data['id'],
    'Ce-Type': event_data['type'],
    'Ce-Specversion': '1.0',
    'Ce-Source': 'local-event'
}

response = requests.post('http://localhost:8080/', data=json.dumps(event_data), headers=headers)
print(response.status_code)
print(response.text)
