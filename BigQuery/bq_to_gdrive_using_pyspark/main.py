from pandas import read_csv, DataFrame
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import pandas as pd

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

# Create or load your Pandas DataFrame (replace this with your data)
data = {
    'Name': ['Alice', 'Bob', 'Charlie'],
    'Age': [25, 30, 35],
    'City': ['New York', 'San Francisco', 'Los Angeles']
}

df = pd.DataFrame(data)

# Specify the file path for the CSV file
csv_file_path = '/home/sudheerrbl5256/my_dataframe.csv'

# Save the DataFrame to the CSV file
df.to_csv(csv_file_path, index=False)  # Set index=False to exclude the index column

# Specify the folder ID in Google Drive where you want to upload the file
folder_id = '1pjlonEKhMaWb35ICtTFeT3RhoV5xYqgw'

# Create a media upload object
media = MediaFileUpload(
    csv_file_path,
    resumable=True
)

# Create the file in Google Drive with the specified parent folder
file_metadata = {
    'name': 'my_dataframe.csv',
    'parents': [folder_id]
}

# Create the file in Google Drive
uploaded_file = service.files().create(
    body=file_metadata,
    media_body=media,
).execute()

print(f"File '{uploaded_file['name']}' uploaded successfully to Google Drive.")
