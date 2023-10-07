from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month

# Replace with your own values
project_id = 'gcp-demo-dev-397415'
dataset_id = 'Lakshmi'
table_id = 'sparkfilter'
filter_year = 2023
filter_month = 7

# Initialize a Spark session with the "spark.jars" property
spark = SparkSession.builder \
    .appName("BigQueryExample") \
    .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar") \
    .getOrCreate()

# Load the BigQuery table
table_ref = f"{project_id}:{dataset_id}.{table_id}"
df = spark.read \
    .format("bigquery") \
    .option("table", table_ref) \
    .load()

# Apply the filter condition
filtered_df = df.filter((year("date") == filter_year) & (month("date") == filter_month))

# Show the filtered results
filtered_df.show()

# Stop the Spark session
spark.stop()
