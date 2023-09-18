from google.cloud import bigquery

def query_bigquery(sql_query):
    client = bigquery.Client()
    query_job = client.query(sql_query)
    results = query_job.result()
    return results.to_dataframe()

# Call the function to run a BigQuery SQL query
sql_query = "SELECT * FROM my_dataset.my_table WHERE age >= 30"
result_df = query_bigquery(sql_query)
print(result_df)

'''
In this example, the query_bigquery function connects to Google BigQuery, executes a SQL query, and returns the results as a Pandas DataFrame.
'''