# pylint: disable=unsupported-binary-operation
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pymysql
from apache_beam.io.jdbc import ReadFromJdbc
import uuid


# MySQL connection parameters
MYSQL_USER = "root"
MYSQL_PASSWORD = "hani"
MYSQL_HOST = "34.93.124.242"
MYSQL_DB = "salesdatabase"
MYSQL_TABLE = "MASTER"

# BigQuery connection parameters
BIGQUERY_PROJECT = "polar-ring-378013"
BIGQUERY_DATASET = "Master"
BIGQUERY_TABLE = "Master"

# MySQL connection function


def get_mysql_connection():
    return pymysql.connect(
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        host=MYSQL_HOST,
        database=MYSQL_DB
    )


# BigQuery source query
source_query = f"SELECT * FROM `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}`"
# 10 records
# Apache Beam pipeline

pipeline_args = ['--project', 'polar-ring-378013',
                 '--job_name', 'jobname1',
                 '--runner', 'DataflowRunner',
                 '--staging_location', 'gs://polar-ring-378013/test',
                 '--temp_location', 'gs://polar-ring-378013/test',
                 '--region', 'us-central1']


def run(argv=None):
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
        # Read data from BigQuery
        bigquery_data = p | "Read from BigQuery" >> beam.io.Read(
            beam.io.BigQuerySource(query=source_query, use_standard_sql=True)
        )

        # Write data to MySQL
        sql_insertion = bigquery_data | "Write to MySQL" >> beam.ParDo(
            WriteToMySQL(MYSQL_USER, MYSQL_PASSWORD,
                         MYSQL_HOST, MYSQL_DB, MYSQL_TABLE)
        )


class WriteToMySQL(beam.DoFn):
    def __init__(self, user, password, host, database, table):
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.table = table
        self.table2 = 'HISTORY'

    def start_bundle(self):
        # Create a MySQL connection and cursor
        self.conn = pymysql.connect(
			user=MYSQL_USER,
			password=MYSQL_PASSWORD,
			host=MYSQL_HOST,
			database=MYSQL_DB
			)
        self.cursor = self.conn.cursor()

    def process(self, element):
        # Check if Contact exists in MySQL
        record_exists = False
        select_stmt = f"SELECT COUNT(*) FROM {self.table}"
        self.cursor.execute(select_stmt)
        result = self.cursor.fetchone()
        if result[0] > 0:
            record_exists = True
            print("As the master table has already had the records, let me check if the contact is present or not in master table")
            # Check if Contact exists in MySQL
            contact_exists = False
            select_stmt = f"SELECT * FROM {self.table} WHERE Contact = %s"
            self.cursor.execute(select_stmt, element['Contact'])
            result = self.cursor.fetchone()
            if result is not None:
                contact_exists = True
                print("The contact exists. I am updating this record in Master and inserting the same into History")
            else:
                contact_exists = False
                print("The contact doest not exist in master table, I am inserting this with new UUID into both the tables...Master and History")

            if contact_exists == False:
                # Generate a UUID if UID is not present in element
                if 'UID' not in element:
                    element['UID'] = str(uuid.uuid4())

                    # Build the MySQL insert or update statement
                    values = [
                        str(value).replace(
                            "'", "''") if value is not None and value != '' else "NULL"
                        for value in element.values()
                    ]
                    columns = ", ".join(element.keys())
                    placeholders = ", ".join(["%s"] * len(element))
                    insert_stmt = f"INSERT INTO {self.table} ({columns}) VALUES ({placeholders})"
                    self.cursor.execute(insert_stmt, values)
                    insert_stmt = f"INSERT INTO {self.table2} ({columns}) VALUES ({placeholders})"
                    self.cursor.execute(insert_stmt, values)

            if contact_exists == True:
                update_stmt = f"UPDATE {self.table} SET FN = %s, MN = %s, LN = %s, DOB = %s, eMailID = %s, Address = %s, LastUpdatedDay = %s  WHERE Contact = %s"
                self.cursor.execute(update_stmt, (element['FN'], element['MN'], element['LN'], element['DOB'],
                                                  element['eMailID'], element['Address'], element['LastUpdatedDay'], element['Contact']))
                select_stmt = f"SELECT UID FROM {self.table} WHERE Contact = %s"
                self.cursor.execute(select_stmt, element['Contact'])
                result = self.cursor.fetchone()
                element['UID'] = result[0]
                values = [
                    str(value).replace(
                        "'", "''") if value is not None and value != '' else "NULL"
                    for value in element.values()
                ]
                columns = ", ".join(element.keys())
                placeholders = ", ".join(["%s"] * len(element))
                insert_stmt = f"INSERT INTO {self.table2} ({columns}) VALUES ({placeholders})"
                self.cursor.execute(insert_stmt, values)

        else:
            record_exists = False
            print("As the master table has not had any records so far, let me insert this single record as an initial load")
            element['UID'] = str(uuid.uuid4())
            # Build the MySQL insert or update statement
            values = [
                str(value).replace(
                    "'", "''") if value is not None and value != '' else "NULL"
                for value in element.values()
            ]
            columns = ", ".join(element.keys())
            placeholders = ", ".join(["%s"] * len(element))
            insert_stmt = f"INSERT INTO {self.table} ({columns}) VALUES ({placeholders})"
            self.cursor.execute(insert_stmt, values)
            insert_stmt = f"INSERT INTO {self.table2} ({columns}) VALUES ({placeholders})"
            self.cursor.execute(insert_stmt, values)

        self.conn.commit()
        yield element

    def finish_bundle(self):
        # Close the MySQL connection and cursor
        self.cursor.close()
        self.conn.close()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
