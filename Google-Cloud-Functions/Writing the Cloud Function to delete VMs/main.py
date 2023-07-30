import google.auth
from google.auth import credentials
from google.cloud import compute_v1
from googleapiclient.errors import HttpError

def delete_running_vms(request):
    try:
        # Authenticate using the default credentials
        credentials, project = google.auth.default()

        # Create a Compute Engine client
        compute_client = compute_v1.InstancesClient(credentials=credentials)

        # Get a list of all running instances in the project
        instances = compute_client.list(project=project, zone="us-central1-a")

        # Delete all the running instances
        for instance in instances:
            compute_client.delete(project=project, zone=instance.zone.split("/")[-1], instance=instance.name)

        # Return a success message
        return "All running VMs have been deleted"
    except HttpError as e:
        # Return an error message if there's an HTTP error
        return f"An error occurred: {e}"