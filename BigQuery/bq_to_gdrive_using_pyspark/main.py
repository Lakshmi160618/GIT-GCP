from google.oauth2.service_account import Credentials
from google.cloud import bigquery
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import pandas as pd
from google.cloud import bigquery

# Google Drive API scope
SCOPES = ["https://www.googleapis.com/auth/drive"]

# Load service account credentials from a local JSON key file
creds = Credentials.from_service_account_file(
    "/home/sudheerrbl5256/secrets/divine-command-396711-6e2f9a7411ba.json",
    scopes=SCOPES,
)

# Create a Google Drive service
service = build(
    "drive",
    "v3",
    credentials=creds,
    cache_discovery=False,
)

# Initialize BigQuery client with your credentials
'''bigquery_client = bigquery.Client.from_service_account_json(
    "/home/sudheerrbl5256/secrets/divine-command-396711-6e2f9a7411ba.json"
)'''
bigquery_client = bigquery.Client()
# Define your BigQuery SQL query
sql_query = """
SELECT
     *
FROM
    gcp-demo-dev-397415.Lakshmi.Accounts
"""

# Execute the BigQuery SQL query and convert the result to a DataFrame
query_job = bigquery_client.query(sql_query)
df = query_job.to_dataframe()

# Specify the folder ID in Google Drive where you want to upload the file
folder_id = '1pjlonEKhMaWb35ICtTFeT3RhoV5xYqgw'

# Specify the file path for the CSV file
csv_file_path = '/home/sudheerrbl5256/my_dataframebq.csv'

# Save the DataFrame to the CSV file
df.to_csv(csv_file_path, index=False)

# Create a media upload object
media = MediaFileUpload(
    csv_file_path,
    resumable=True
)

# Create the file in Google Drive with the specified parent folder
file_metadata = {
    'name': 'my_dataframebq.csv',
    'parents': [folder_id]
}

# Create the file in Google Drive
uploaded_file = service.files().create(
    body=file_metadata,
    media_body=media,
).execute()

print(f"File '{uploaded_file['name']}' uploaded successfully to Google Drive.")
