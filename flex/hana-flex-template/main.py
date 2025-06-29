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
from pipeline.utils import (
    to_bigquery_row,
    get_new_query,
    get_schema,
    get_control_schema,
    get_secret,
    read_from_hana
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

    conn_info = get_secret(options.dataflow_project, options.secret_name_origin)
    options.host = conn_info['host']
    options.port = conn_info['port']
    options.username = conn_info['username']
    options.password = conn_info['password']
    options.database = conn_info['database']

    #options.query = options.query.replace('{schema}', conn_info['schema'])
    #options.query = get_new_query(options)
    # ✅ NUEVA LÓGICA: aplicar `get_new_query()` solo si el query contiene '{schema}'
    if '{schema}' in options.query:
        logging.info("Query contiene '{schema}', aplicando reemplazo y lógica incremental...")
        schema_name = conn_info.get('schema', '')
        options.query = options.query.replace('{schema}', schema_name)
        options.query = get_new_query(options)
    else:
        logging.info("Query no contiene '{schema}', se usará directamente sin modificación.")

    table_schema = get_schema(options.pipeline_name, options.stage, options.dataflow_bucket_name)
    control_schema = get_control_schema(options.dataflow_bucket_name,options.control_schema_name)

    with beam.Pipeline(options=options) as pipeline:

        data = pipeline | 'Reading from HANA' >> beam.Create([options]) | 'Execute HANA Query' >> beam.FlatMap(read_from_hana)

        data = data | 'Data String To BigQuery Row' >> beam.Map(to_bigquery_row)

        table_name = options.target_table.replace('/', '_')

        _ = data | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            f"{options.target_project}:{options.target_dataset}.{table_name}",
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

        record_count = data | 'Count Records' >> combiners.Count.Globally()

        def fail_on_zero_records(count):
            if count == 0:
                raise RuntimeError("ERROR: No se leyeron ni escribieron registros.")
            return count

        record_count = record_count | 'Fail if Zero Records' >> beam.Map(fail_on_zero_records)

        control_data = record_count | 'Create Control Record' >> beam.Map(
            lambda count: create_control_record(options, count)
        )

        control_data = control_data | 'Control String To BigQuery Row' >> beam.Map(to_bigquery_row)

        _ = control_data | 'Write Control Totals' >> beam.io.WriteToBigQuery(
            f'{options.target_project}:{options.target_dataset}.records_control',
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
