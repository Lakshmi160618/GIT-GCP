import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import DoFn
from google.cloud import spanner

# Pub/Sub subscription details
project_id = 'centering-star-391312'
subscription_id = 'new_topic_pubsub-sub'
instance_id = 'instance1'
database_id = 'gcpdemodatabase'
table_name = 'gcpdemotable'

# Cloud Spanner client
client = spanner.Client()
instance = client.instance(instance_id)
database = instance.database(database_id)

# Parse the message and extract the values
def parse_message(message):
    # Implement your logic to parse the message and extract the values
    values = [
        message['temp'],
        message['humidity'],
        message['pressure']
    ]
    return values

# DoFn class for writing to Cloud Spanner
class WriteToSpannerFn(DoFn):
    def process(self, element):
        # Parse the message and extract the values
        values = parse_message(element)

        # Write the values to Cloud Spanner
        with database.batch() as batch:
            batch.insert(
                table=table_name,
                columns=['temp', 'humidity', 'pressure'],
                values=[values]
            )

# Define the Dataflow pipeline
def run():
    # Create the PipelineOptions with the DirectRunner
    options = PipelineOptions(
        runner='DirectRunner', streaming=True)

    # Create the pipeline
    with beam.Pipeline(options=options) as p:
        # Read messages from the Pub/Sub subscription
        messages = (p
                    | 'Read Pub/Sub Messages' >> beam.io.ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_id}')
                    )

        # Convert byte strings to regular strings
        messages = messages | 'Convert to String' >> beam.Map(lambda message: message.decode('utf-8'))

        # Parse JSON messages
        parsed_messages = messages | 'Parse JSON' >> beam.Map(lambda message: eval(message))

        # Write parsed messages to Cloud Spanner
        parsed_messages | 'Write to Spanner' >> beam.ParDo(WriteToSpannerFn())


# Execute the pipeline
if __name__ == '__main__':
    run()
