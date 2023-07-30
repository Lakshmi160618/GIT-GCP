import apache_beam as beam

with beam.Pipeline() as pipeline:
    # Create a PCollection of records with multiple fields
    records = pipeline | beam.Create([
        {'name': 'Alice', 'age': 30},
        {'name': 'Bob', 'age': 35},
        {'name': 'Charlie', 'age': 40}
    ])

    # Use a ParDo to extract just the 'name' field into a new PCollection
    names = records | beam.Map(lambda record: record['age'])

    # Print the contents of the 'names' PCollection
    names | beam.Map(print)
