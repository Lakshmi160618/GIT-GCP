import apache_beam as beam
# Create a PCollection of sentences
sentences = ["The quick brown fox", "jumps over the lazy dog"]
with beam.Pipeline() as pipeline:
    # Create a PCollection from the list of sentences
    pcoll = pipeline | beam.Create(sentences)
    # Use the FlatMap transform to split each sentence into words
    words = pcoll | beam.FlatMap(lambda sentence: sentence.split())
    # Print the contents of the 'words' PCollection
    words | beam.Map(print)