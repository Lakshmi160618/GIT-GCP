from googleapiclient.discovery import build
from google.oauth2 import service_account

# Path to your service account key file
key_file_path = r'C:\\Users\Umeshyam\Downloads\bigqueryuserjsonkey.json'

# Scopes required for the BigQuery API
scopes = ['https://www.googleapis.com/auth/bigquery']

# Create a service account credentials object
credentials = service_account.Credentials.from_service_account_file(
    key_file_path, scopes=scopes
)

# Build the BigQuery API client
service = build('bigquery', 'v2', credentials=credentials)

# Set the project ID and dataset ID
project_id = 'dev-project-389611'
dataset_id = 'fifth_dataset'

# Set the dataset body with location information
dataset_body = {
    'datasetReference': {
        'projectId': project_id,
        'datasetId': dataset_id
    },
    'location': 'US'  # Specify the desired location
}

# Call the datasets.insert method to create the dataset
response = service.datasets().insert(
    projectId=project_id,
    body=dataset_body
).execute()

# Print the response
print('Dataset created: {}'.format(response['datasetReference']['datasetId'])) 
