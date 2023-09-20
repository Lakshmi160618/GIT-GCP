import pandas as pd

def create_dataframe() -> pd.DataFrame:
    data = {'Name': ['Alice', 'Bob', 'Charlie'],
            'Age': [30, 25, 35]}
    df = pd.DataFrame(data)
    return df

# Call the function to create a DataFrame
result_df = create_dataframe()
print(result_df)

'''
In this example, the create_dataframe function creates a Pandas DataFrame and returns it. The returned DataFrame can then be used for data analysis and manipulation.
'''