import apache_beam as beam

PROJECT = 'qwiklabs-gcp-444ccd8556531ce8'
BUCKET = PROJECT

table_spec = 'mynewdataset.weather_stations'
table_schema = 'source:STRING, quote:STRING'


def run():
    argv = [
      '--project={0}'.format(PROJECT),
      '--job_name=bqjob2',
      '--save_main_session',
      '--staging_location=gs://{0}/staging/'.format(BUCKET),
      '--temp_location=gs://{0}/staging/'.format(BUCKET),
      '--runner=DataflowRunner'
    ]

    p = beam.Pipeline(argv=argv)
    # p | beam.Create([
    #     {'source': 'Mahatma Gandhi', 'quote': 'My life is my message.'},
    #     {'source': 'Yoda', 'quote': "Do, or do not. There is no 'try'."},
    # ])
    input = 'gs://{0}/create/*.json'.format(BUCKET)
    # output_prefix = 'gs://{0}/create/output'.format(BUCKET)

    (p
        | 'GetJava' >> beam.io.ReadFromText(input)
        | 'write' >> beam.io.WriteToBigQuery(table_spec, schema=table_schema)
     )

    p.run()


if __name__ == '__main__':
    run()
