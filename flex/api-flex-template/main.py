"""
 * Copyright (c) 2025 - Liverpool. All rights reserved
 *
 * Grupo de Asesores Profesionales en Servicios de Integracion {GAPSI} - CDMX - 2025
 *
"""

import apache_beam as beam
import logging
from apache_beam.transforms import combiners
from pipeline.controller import MyOptions
from pipeline.make_request_fn import MakeRequestFn
from pipeline.utils import (
    get_data_dict_file,
    get_control_schema,
    get_schema,
    to_bigquery_row
)
from pipeline.validators import validate_args

# from dotenv import load_dotenv
# load_dotenv()

"""
 * @author: Jaime Arturo Chávez
 * @updated: 
 * @description: starter class
 * @since-version: 1.0
"""


def run():
    from pipeline.utils import create_control_record

    options = MyOptions()
    validate_args(options)

    dictionary = get_data_dict_file(options.pipeline_name, options.stage, options.dataflow_bucket_name)
    setattr(options, 'dictionary', dictionary)
    control_schema = get_control_schema(options.control_schema_path)
    setattr(options, 'control_schema', control_schema)
    table_schema = get_schema(options.pipeline_name, options.stage, options.dataflow_bucket_name)

    with beam.Pipeline(options=options) as pipeline:
        data = (pipeline
                | 'Create initial collection' >> beam.Create([1])
                | 'Make GET request' >> beam.ParDo(MakeRequestFn(options))
        )

        record_count = data | 'Count Records' >> combiners.Count.Globally()

        def fail_on_zero_records(count):
            if count == 0:
                raise RuntimeError("ERROR: No se leyeron ni escribieron registros.")
            return count

        record_count = record_count | 'Fail if Zero Records' >> beam.Map(fail_on_zero_records)
        # _ = record_count | 'print' >> beam.Map(print)

        control_data = record_count | 'Create Control Record' >> beam.Map(
            lambda count: create_control_record(options, count)
        )

        control_data = control_data | 'Control String To BigQuery Row' >> beam.Map(to_bigquery_row)

        data = data | 'Data String To BigQuery Row' >> beam.Map(to_bigquery_row)

        _ = data | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            f'{options.target_project}.{options.target_dataset}.{options.target_table}',
            custom_gcs_temp_location=options.temp_folder,
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            additional_bq_parameters={
                'timePartitioning': {
                    'type': 'DAY',
                    'requirePartitionFilter': False
                }
            }
        )

        _ = control_data | 'Write Control Totals' >> beam.io.WriteToBigQuery(
            f'{options.target_project}.{options.target_dataset}.records_control',
            custom_gcs_temp_location=options.temp_folder,
            schema=control_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            additional_bq_parameters={
                'timePartitioning': {
                    'type': 'DAY',
                    'requirePartitionFilter': False
                }
            }
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

