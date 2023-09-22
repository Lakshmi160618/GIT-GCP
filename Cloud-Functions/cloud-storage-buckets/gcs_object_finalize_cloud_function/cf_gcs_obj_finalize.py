import os
import functions_framework
from google.cloud import bigquery

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data
    event_id = cloud_event["id"]
    event_type = cloud_event["type"]
    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")

    gcs_uri = "gs://" + bucket + "/" + name
    print(f"GCS_uri: {gcs_uri}")

    # Initialize the BigQuery client
    client = bigquery.Client()

    # Define the dataset and table information
    project_id = os.environ.get('project_id')
    print(project_id)
    dataset_id = os.environ.get('dataset_id')
    print(dataset_id)
    table_id = os.environ.get('table_id')
    print(table_id)

    # Create the dataset if it doesn't exist (optional)
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    if not client.get_dataset(dataset):
        client.create_dataset(dataset)

    # Create the table with schema auto-detection
    job_config = bigquery.LoadJobConfig(
        autodetect=True,  # Enable schema auto-detection
        skip_leading_rows=1,  # If your CSV file has a header, set this to 1
    )

    table_ref = dataset_ref.table(table_id)
    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)

    load_job.result()  # Wait for the job to complete

    print(f"Table {table_id} created in dataset {dataset_id} from {gcs_uri} with auto-detected schema.")

    # Run the previous INSERT statement
    insert_query = """
    INSERT INTO `gcp-demo-dev-397415.stage_dataset.Customers_data`
      (cust_id, cust_name, phone_no, address, email_id, created_time, updated_time)
    SELECT
      cust_id,
      cust_name,
      phone_no,
      address,
      email_id,
      current_timestamp(),
      current_timestamp()
    FROM
      `gcp-demo-dev-397415.raw_dataset.Customers_data`
    WHERE cust_id is not null;
    """
    
    insert_query_job = client.query(insert_query)
    
    # Wait for the insert query job to complete
    insert_query_job.result()

    print("INSERT SQL Query executed successfully.")

    # Run the MERGE SQL query
    merge_query = """
    MERGE `gcp-demo-dev-397415.golden_dataset.Customers_data` AS target
    USING (
      SELECT
        cust_id,
        cust_name,
        phone_no,
        address,
        email_id,
        created_time,
        updated_time
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY created_time DESC) AS rn
        FROM
          `gcp-demo-dev-397415.stage_dataset.Customers_data`
      )
      WHERE
        rn = 1
    ) AS source
    ON target.cust_id = source.cust_id

    -- When there's a match (cust_id exists in both source and target), update the target table.
    WHEN MATCHED THEN
      UPDATE SET
        target.cust_name = source.cust_name,
        target.phone_no = source.phone_no,
        target.address = source.address,
        target.email_id = source.email_id, 
        target.updated_time = source.updated_time

    -- When there's no match (cust_id does not exist in target), insert a new record.
    WHEN NOT MATCHED THEN
      INSERT (
        cust_id,
        cust_name,
        phone_no,
        address,
        email_id,
        created_time,
        updated_time
      )
      VALUES (
        source.cust_id,
        source.cust_name,
        source.phone_no,
        source.address,
        source.email_id,
        source.created_time,
        source.updated_time
      );
    """
    
    merge_query_job = client.query(merge_query)
    
    # Wait for the merge query job to complete
    merge_query_job.result()

    print("MERGE SQL Query executed successfully.")
    
    # Run the TRUNCATE SQL statement
    truncate_query = """
    TRUNCATE TABLE `gcp-demo-dev-397415.raw_dataset.Customers_data`
    """
    
    truncate_query_job = client.query(truncate_query)
    
    # Wait for the truncate query job to complete
    truncate_query_job.result()

    print("TRUNCATE SQL Query executed successfully.")
