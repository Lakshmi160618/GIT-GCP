import apache_beam as beam
# Create a PCollection of numbers
numbers = [1, 2, 3, 4, 5]

with beam.Pipeline() as pipeline:
    # Create a PCollection from the list of numbers
    pcoll = pipeline | beam.Create(numbers)
    # Use the Map transform to double each number
    doubled = pcoll | beam.Map(lambda x: x * 2)
    # Print the contents of the 'doubled' PCollection
    doubled | beam.Map(print)
