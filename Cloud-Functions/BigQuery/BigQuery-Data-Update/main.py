import functions_framework
from google.cloud import bigquery

# CloudEvent function to be triggered by an Eventarc Cloud Audit Logging trigger
# Note: this is NOT designed for second-party (Cloud Audit Logs -> Pub/Sub) triggers!
@functions_framework.cloud_event
def hello_auditlog(cloudevent):
    from google.cloud import bigquery

    # Initialize BigQuery client
    client = bigquery.Client()

    # Define the source and destination table references
    source_table_ref = client.dataset('cloud_functions_check').table('bq_stage_tbl')
    destination_table_ref = client.dataset('cloud_functions_check').table('bq_stage_tb2')

    # Define the MERGE statement
    merge_sql = f"""
    MERGE INTO `{destination_table_ref.project}.{destination_table_ref.dataset_id}.{destination_table_ref.table_id}` AS dest
    USING `{source_table_ref.project}.{source_table_ref.dataset_id}.{source_table_ref.table_id}` AS src
    ON dest.id = src.id  -- Replace with your actual join condition
    WHEN MATCHED THEN
    UPDATE SET
        dest.name = src.name,
        dest.cust_id = src.cust_id,
        dest.transaction_id = src.transaction_id,
        dest.transaction_timestamp = src.transaction_timestamp,
        dest.amount = src.amount
    WHEN NOT MATCHED THEN
    INSERT (
        id, name, cust_id, transaction_id, transaction_timestamp, amount
    )
    VALUES (
        src.id, src.name, src.cust_id, src.transaction_id, src.transaction_timestamp, src.amount
    )
    WHEN NOT MATCHED BY SOURCE THEN DELETE;
    """

    # Run the MERGE statement
    query_job = client.query(merge_sql)

    # Wait for the query job to complete
    query_job.result()

    print(f"Data merged from {source_table_ref.table_id} to {destination_table_ref.table_id}")
