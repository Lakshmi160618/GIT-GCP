import apache_beam as beam
# Define a function that converts integers to strings
class IntToString(beam.DoFn):
    def process(self, element):
        return [str(element)]
# Create a Pipeline object
with beam.Pipeline() as pipeline:
    # Create a PCollection with some integers
    ints = pipeline | beam.Create([1, 2, 3, 4, 5])
    # Apply the IntToString function to each element of the PCollection
    strings = ints | beam.ParDo(IntToString())
    # Print the resulting PCollection
    strings | beam.Map(print)
