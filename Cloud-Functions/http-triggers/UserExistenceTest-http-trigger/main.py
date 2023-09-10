from google.cloud import bigquery


def factorial(request):
    request_json = request.get_json()
    if request.args and 'input' in request.args:
        user_input = request.args.get('input') 
        # Replace with your Google Cloud project ID
        project_id = 'gcp-demo-dev-397415'

        # Initialize a BigQuery client
        client = bigquery.Client(project=project_id)

        # Define the dataset and table name
        dataset_id = 'Lakshmi'
        table_id = 'Accounts'

        # Prompt the user for input (userid and password separated by a single space)
        # user_input = input("Enter userid and password (e.g., 'johndoe #password123'): ")

        # Split the user input into userid and password
        userid, password = user_input.split()

        # Construct and execute the SQL query
        query = f"""
            SELECT *
            FROM `{project_id}.{dataset_id}.{table_id}`
            WHERE user_id = '{userid}' AND pass_word = '{password}'
        """

        query_job = client.query(query)

        # Check if a matching record was found
        results = list(query_job)
        if len(results) > 0:
            for row in results:
                bal = row['balance']
                return f"Your balance is: {str(bal)}"
        else:
            return "No account matched the provided userid and password."
