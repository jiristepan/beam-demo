
from __future__ import absolute_import

import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.internal.clients import bigquery

class WordClasifyDoFn(beam.DoFn):
  def process(self, element):
    return [(element[0],element[1],len(element[0]))]

class WordExtractingDoFn(beam.DoFn):
  def process(self, element):
    word = re.findall(r'[\w\']+', element.lower(), re.UNICODE)
    return word

def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://dataflow-samples/shakespeare/kinglear.txt',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
  parser.add_argument(
    '--save_to_bq',
    dest='saveToBQ',
    default=False,
    help="should save to bq"
  )
  
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # print(known_args)
  # print(pipeline_args)
  #quit()

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:

    # Read from text file
    lines = p | 'Read' >> ReadFromText(known_args.input)

    # transformation - paralel extract words, and group
    counts = (
        lines
        | 'Split' >> beam.ParDo(WordExtractingDoFn())
        | 'PairWIthOne' >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.CombinePerKey(sum)
        | 'Clasify' >> beam.ParDo(WordClasifyDoFn())
        )

    # Format the counts into a PCollection of strings.
    def format_result(x):
      # print(x)
      return '%s,%d,%d' % (x[0], x[1], x[2])

    output = counts | 'Format' >> beam.Map(format_result)
    # Ekvivalent:
    #  count.apply("Format",beam.Map(format_result)).apply("adflj", ....)

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output | 'Write' >> WriteToText(known_args.output)

    if known_args.saveToBQ:
        # Write to big query
        table_spec = bigquery.TableReference(
          projectId='activate-data',
          datasetId='beam_demo',
          tableId='word_count')
        
        table_schema = {
          'fields': [
            {'name': 'word', 'type': 'STRING', 'mode': 'NULLABLE'}, 
            {'name': 'count', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'length', 'type': 'INT64', 'mode': 'NULLABLE'}
            ]
          }


        (counts 
        | 'Format for bq' >> beam.Map( lambda x: {"word": x[0], "count": x[1], "length":x[2]})
        | 'Write to BQ' >> beam.io.WriteToBigQuery(
            table_spec,
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()