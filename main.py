"""
sudo apt-get install python-pip
sudo pip install apache-beam[gcp] oauth2client==3.0.0
sudo pip install -U pip
"""


import apache_beam as beam
import sys

table_spec = 'mynewdataset.weather_stations'
table_schema = 'source:STRING, quote:STRING'

p = beam.Pipeline(argv=sys.argv)
quotes = p | beam.Create([
    {'source': 'Mahatma Gandhi', 'quote': 'My life is my message.'},
    {'source': 'Yoda', 'quote': "Do, or do not. There is no 'try'."},
])

quotes | 'write' >> beam.io.WriteToBigQuery(
    table_spec,
    schema=table_schema,
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
