from pyspark.sql import SparkSession
from google.cloud import bigquery

# Initialize the Spark session
spark = SparkSession.builder.appName("BigQueryToGCS").getOrCreate() 
# Set up BigQuery client
client = bigquery.Client()

# Define your BigQuery settings
project_id = "gcp-demo-dev-397415"
dataset_id = "Lakshmi"
table_id = "Accounts"
destination_bucket = "lakshmi_2023"
destination_blob = "output.csv"  # Specify the desired output file name

# Build the BigQuery SQL query
query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"

# Load the BigQuery table into a Spark DataFrame
df = spark.read.format("bigquery").option("project", project_id).option("dataset", dataset_id).option("table", table_id).load()

# Save the DataFrame as a CSV file in the GCS bucket
df.write.option("header", "true").csv(f"gs://{destination_bucket}/{destination_blob}")

# Stop the Spark session
spark.stop()