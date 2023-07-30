import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigtableio import WriteToBigTable
from google.cloud import pubsub_v1

# Classes
PROJECT = 'modern-replica-387105'
SUBSCRIPTION = 'firsttopic-sub'
REGION = 'us-central1'
TEMP_LOCATION = 'gs://bucket1234forgcp/tmp'
STAGING_LOCATION = 'gs://bucket1234forgcp/stage'
INSTANCE = 'instance1'
TABLE = 'second_table'


class CreateRowFn(beam.DoFn):
    def __init__(self, pipeline_options):
        self.instance_id = pipeline_options.bigtable_instance
        self.table_id = pipeline_options.bigtable_table

    def process(self, element):
        import json
        from google.cloud.bigtable import row

        message = json.loads(element)
        row_key = message['rowkey']
        columns = message['columns']

        direct_row = row.DirectRow(row_key=row_key)

        for column in columns:
            column_family = column['columnfamily']
            column_name = column['columnname']
            column_value = column['columnvalue']
            direct_row.set_cell(column_family, column_name, column_value)

        yield direct_row



# Options
class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--bigtable_project', default='nested')
        parser.add_argument('--bigtable_instance', default='instance')
        parser.add_argument('--bigtable_table', default='table')


pipeline_options = XyzOptions(
    save_main_session=True,
    streaming=True,
    runner='DataflowRunner',
    project=PROJECT,
    region=REGION,
    temp_location=TEMP_LOCATION,
    staging_location=STAGING_LOCATION,
    bigtable_project=PROJECT,
    bigtable_instance=INSTANCE,
    bigtable_table=TABLE
)


# Pipeline
def run(argv=None):
    with beam.Pipeline(options=pipeline_options) as p:
        input_subscription = f"projects/{PROJECT}/subscriptions/{SUBSCRIPTION}"

        _ = (p
             | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(subscription=input_subscription).with_output_types(bytes)
             | 'Conversion UTF-8 bytes to string' >> beam.Map(lambda msg: msg.decode('utf-8'))
             | 'Conversion string to row object' >> beam.ParDo(CreateRowFn(pipeline_options))
             | 'Writing row object to BigTable' >> WriteToBigTable(
                 project_id=pipeline_options.bigtable_project,
                 instance_id=pipeline_options.bigtable_instance,
                 table_id=pipeline_options.bigtable_table)
             )


if __name__ == '__main__':
    run()


python /home/udaykirande860/cbt.py \
    --project=modern-replica-387105 \
    --runner=DataflowRunner \
    --region=us-central1 \
    --job_name=cbt-job9 \
    --temp_location=gs://bucket1234forgcp/tmp \
    --staging_location=gs://bucket1234forgcp/staging 