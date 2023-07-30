import apache_beam as beam
import json

class FilterData(beam.DoFn):
    def process(self, element):
        parsed_element = json.loads(element)
        if parsed_element['score'] > 90:
            yield parsed_element

with beam.Pipeline() as pipeline:
    data = pipeline | 'ReadData' >> beam.io.ReadFromText('inputfile.json')
    filtered_data = data | 'FilterData' >> beam.ParDo(FilterData())
    filtered_data | 'WriteData' >> beam.io.WriteToText('gcp_demo.json')
