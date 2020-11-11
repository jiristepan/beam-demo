
# Beam pipeline for generting repated text using Dataflow

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


class Multiply(beam.DoFn):
  def process(self, element, count):
    out=[]
    for i in range(count):
        out.append(element)  
    
    return out

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
      '--count',
      dest='count',
      required=True,
      default=10,
      help='Number of repetition of each line')


  print(parser.parse_known_args(argv))
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

#   print(pipeline_args.runner)
  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:

    # Read the text file[pattern] into a PCollection.
    text = p | 'Read' >> ReadFromText(known_args.input)
    texts = text | "Multiply" >> beam.ParDo(Multiply(), int(known_args.count))

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned

    texts | 'Write' >> WriteToText(known_args.output)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()