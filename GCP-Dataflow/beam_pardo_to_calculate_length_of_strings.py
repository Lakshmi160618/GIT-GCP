import apache_beam as beam
class ComputeLengthFn(beam.DoFn):
    def process(self, element):
        yield len(element)

with beam.Pipeline() as pipeline:
    # Create a PCollection of words
    words = pipeline | beam.Create(["apple", "banana", "cherry", "date", "elderberry"])
    # Use a ParDo to compute the length of each word
    word_lengths = words | beam.ParDo(ComputeLengthFn())
    # Print the contents of the 'word_lengths' PCollection
    word_lengths | beam.Map(print)
