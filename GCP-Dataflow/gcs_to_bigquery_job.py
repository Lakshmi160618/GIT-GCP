import argparse
import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class DataIngestion:
    """A helper class which contains the logic to translate the file into
    a format BigQuery will accept."""

    def parse_method(self, string_input):
        """This method translates a single line of comma separated values to a
        dictionary which can be loaded into BigQuery."""
        # Strip out carriage return, newline and quote characters.
        values = re.split(",", re.sub('\r\n', '', re.sub('"', '',
                                                         string_input)))
        row = dict(
            zip(('shopno', 'shopname','shoparea','shoptype','shopnoofsale'),
                values))
        return row

def run(argv=None):
    """The main function which creates the pipeline and runs it."""
    parser = argparse.ArgumentParser()
    """Here we add some specific command line arguments we expect. Specifically we have the input file to read and the output table to write.
    This is the final stage of the pipeline, where we define the destination of the data. In this case we are writing to BigQuery."""
    parser.add_argument(
        '--input', dest='input', required=False, help='Input file to read. This can be a Google Storage Bucket.', 
        default='gs://gcpgcpgcp/shopdata.csv')

    """This defaults to the newdataset in your BigQuery project. You'll have to create the lake dataset yourself using this command: bq mk newdataset """
    parser.add_argument('--output', dest='output', required=False, help='Output BQ table to write results to.',
                        default='newdataset.sample_csvloading')	

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

	# DataIngestion is a class we built in this script to hold the logic for transforming the file into a BigQuery table.
    data_ingestion = DataIngestion()

    """Initiate the pipeline using the pipeline arguments passed in from the command line. This includes information such as the project ID and
    where Dataflow should store temp files."""
    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (p
     # Read the file. This is the source of the pipeline. All further
     # processing starts with lines read from the file. We use the input
     # argument from the command line. We also skip the first line which is a
     # header row.
     | 'Read from a File' >> beam.io.ReadFromText(known_args.input,
                                                  skip_header_lines=1)
     # This stage of the pipeline translates from a CSV file single row
     # input as a string, to a dictionary object consumable by BigQuery.
     # It refers to a function we have written. This function will
     # be run in parallel on different workers using input from the
     # previous stage of the pipeline.
     | 'String To BigQuery Row' >> beam.Map(lambda s: data_ingestion.parse_method(s)) 
	 | 'Write to BigQuery' >> beam.io.Write(
         beam.io.BigQuerySink(
             # The table name is a required argument for the BigQuery sink.
             # In this case we use the value passed in from the command line.
             known_args.output,
             # Here we use the simplest way of defining a schema:
             # fieldName:fieldType
             schema='shopno:INTEGER,shopname:STRING,shoparea:STRING,shoptype:STRING,shopnoofsale:INTEGER',
             # Creates the table in BigQuery if it does not yet exist.
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             # Deletes all data in the BigQuery table before writing.
             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
    p.run().wait_until_finish()
	
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
	
