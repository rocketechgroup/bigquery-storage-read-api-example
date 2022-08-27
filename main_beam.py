import argparse
import logging
import apache_beam as beam

from apache_beam.io import ReadFromBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--table',
        dest='table',
        required=True,
        help='BigQuery fully qualified table id: PROJECT:DATASET.TABLE')

    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
                pipeline
                | 'Reading Mixed JSONL' >> ReadFromBigQuery(method=ReadFromBigQuery.Method.DIRECT_READ,
                                                            table=known_args.table,
                                                            row_restriction='state = "WA" AND year > 2010')
                | 'Map to tuple' >> beam.Map(lambda x: (x['year'], 1))
                | 'Count' >> beam.combiners.Count.PerKey()
                | 'Print' >> beam.Map(lambda x: print(f"year: {x[0]}, count: {x[1]}"))
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
