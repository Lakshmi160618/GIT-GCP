import googleapiclient.discovery
from google.oauth2 import service_account

# Set your project ID and zone
project_id = 'dev-project-401115'
zone = 'us-central1-a'  # Replace with your desired zone

# Replace with the path to your service account JSON key file
credentials_path = '/home/classgcp501/Secrets/compute_engine_sa.json'

# Initialize the Compute Engine API client
credentials = service_account.Credentials.from_service_account_file(credentials_path)
compute = googleapiclient.discovery.build('compute', 'v1', credentials=credentials)

# Define the VM instance configuration
instance_name = 'it-is-from-python'
machine_type = f'zones/{zone}/machineTypes/n1-standard-1'  # Replace with your desired machine type
image_family = 'debian-11'
image_project = 'debian-cloud'
network = '/global/networks/default'

# Create the VM instance request body
instance_body = {
    'name': instance_name,
    'machineType': machine_type,
    'disks': [{
        'boot': True,
        'initializeParams': {
            'sourceImage': f'projects/{image_project}/global/images/family/{image_family}'
        }
    }],
    'networkInterfaces': [{
        'network': network
    }]
}

# Create the VM instance
compute.instances().insert(project=project_id, zone=zone, body=instance_body).execute()

print('VM instance created:', instance_name)
