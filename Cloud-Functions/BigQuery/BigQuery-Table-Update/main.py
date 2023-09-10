import functions_framework
from google.cloud import bigquery

# Initialize the BigQuery client
bq_client = bigquery.Client()

# Define your Cloud Function
@functions_framework.cloud_event
def hello_auditlog(cloudevent):
    # Extract the resource name from the Cloud Audit Logging entry
    resource_name = cloudevent.data.get("protoPayload", {}).get("resourceName")

    # Define the specific table name to match
    specific_table_name = "projects/utility-league-394208/datasets/dq_dataset/tables/bigquery_table"

    if resource_name == specific_table_name:
        # Split the resource_name and extract the relevant parts
        parts = resource_name.split("/")
        project_id = parts[1]
        dataset_id = parts[3]
        table_name = parts[5]

        # Construct the source table name in BigQuery format
        source_table_name = f"{project_id}.{dataset_id}.{table_name}"

        # Define the destination table name
        destination_table_name = f"{project_id}.{dataset_id}.{table_name}_T"
 
        # Define the SQL query to update the destination table with new schema and data
        update_query = f"""
            CREATE OR REPLACE TABLE `{destination_table_name}`
            AS
            SELECT *
            FROM `{source_table_name}`
            WHERE 1=1
        """

        # Run the SQL query to update the destination table
        query_job = bq_client.query(update_query)
        query_job.result()  # Wait for the query job to complete

        print(f"Table {destination_table_name} updated with new schema and data successfully.")
    else:
        print("Resource name does not match the specified table name.")

if __name__ == "__main__":
    functions_framework.register_http_function(hello_auditlog)
    functions_framework.main()
         