from google.cloud import bigquery
from google.oauth2.service_account import Credentials  

# Set the path to your service account JSON key file
service_account_file = r"C:\Users\Umeshyam\Downloads\bigqueryadminSA.json"

# Set the scopes required for authentication
scopes = ["https://www.googleapis.com/auth/bigquery"]

# Create the credentials object
credentials = Credentials.from_service_account_file(service_account_file, scopes=scopes)

# Create a BigQuery client
client = bigquery.Client(credentials=credentials)

# TODO(developer): Set dataset_id to the ID of the dataset to create.
dataset_id = "{}.dataset_from_client_library".format(client.project)

# Construct a full Dataset object to send to the API.
dataset = bigquery.Dataset(dataset_id)

# TODO(developer): Specify the geographic location where the dataset should reside.
dataset.location = "US"

# Send the dataset to the API for creation, with an explicit timeout.
# Raises google.api_core.exceptions.Conflict if the Dataset already
# exists within the project.
dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
print("Created dataset {}.{}".format(client.project, dataset.dataset_id)) 
