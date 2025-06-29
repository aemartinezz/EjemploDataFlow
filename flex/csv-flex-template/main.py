"""
 * Copyright (c) 2025 - Liverpool. All rights reserved
 *
 * Grupo de Asesores Profesionales en Servicios de Integracion {GAPSI} - CDMX - 2025
 *
"""

import apache_beam as beam
# import logging
# import google.cloud.logging
from apache_beam.io import fileio
from datetime import datetime
from pipeline.controller import MyOptions
from pipeline.process_file_fn import ProcessFile
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
    from pipeline.utils import DataflowUtils

    options = MyOptions()
    validate_args(options)

    options.control_schema = DataflowUtils.get_control_schema(options.control_schema_path)
    options.table_schema = DataflowUtils.get_schema(options.pipeline_name, options.stage, options.dataflow_bucket_name)
    options.schema_fields = [field.split(':')[0] for field in options.table_schema.split(', ')]

    current_time = datetime.now()
    file_paths = DataflowUtils.generate_file_paths(options, current_time)

    try:
        p1 = beam.Pipeline(options=options)
        data = (p1
            | 'Create File Paths' >> beam.Create(file_paths)
            | 'find files' >> fileio.MatchAll()
            | 'read files' >> fileio.ReadMatches()
            | 'Process Files' >> beam.ParDo(ProcessFile(options))
            | 'Data String To BigQuery Row' >> beam.Map(DataflowUtils.to_bigquery_row)
        )

        _ = data | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            f'{options.target_project}.{options.target_dataset}.{options.target_table}',
            custom_gcs_temp_location=options.temp_folder,
            schema=options.table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            additional_bq_parameters={
                'timePartitioning': {
                    'type': 'DAY',
                    'requirePartitionFilter': False
                }
            }
        )

        read_count = data | 'Count Read Records' >> beam.transforms.combiners.Count.Globally()

        def fail_on_zero_records(count):
            if count == 0:
                raise RuntimeError("ERROR: No se leyeron ni escribieron registros.")
            return count

        read_count = read_count | 'Fail if Zero Records' >> beam.Map(fail_on_zero_records)

        control_data = read_count | 'Create Control Record' >> beam.Map(
            lambda count: DataflowUtils.create_control_record(options, count)
        )

        control_data = control_data | 'Control String To BigQuery Row' >> beam.Map(DataflowUtils.to_bigquery_row)

        _ = control_data | 'Write Control Totals' >> beam.io.WriteToBigQuery(
            f'{options.target_project}.{options.target_dataset}.records_control',
            custom_gcs_temp_location=options.temp_folder,
            schema=options.control_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            additional_bq_parameters={
                'timePartitioning': {
                    'type': 'DAY',
                    'requirePartitionFilter': False
                }
            }
        )

        result = p1.run()
        result.wait_until_finish()

        if (not hasattr(result, 'has_job')  or result.has_job):
            p2 = beam.Pipeline(options=options)
            query = (f'SELECT * FROM `{options.target_project}.{options.target_dataset}.{options.target_table}` '
                     f'WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP("{current_time.strftime("%Y-%m-%d")}")')

            written_data = p2 | 'Check written data' >> beam.io.ReadFromBigQuery(
                query=query,
                project=options.target_project,
                use_standard_sql=True,
                gcs_location=options.temp_folder
            )

            _ = written_data | 'Count Records' >> beam.transforms.combiners.Count.Globally()

            # control_data = written_count | 'Create Control Record' >> beam.Map(
            #     lambda written: DataflowUtils.create_control_record2(
            #         options,
            #         read_count=0,
            #         written_count=written
            #     )
            # )
            #
            # control_data | 'print' >> beam.Map(print)
            #
            #
            # _ = control_data | 'Write Control Totals' >> beam.io.WriteToBigQuery(
            #     f'{options.target_project}.{options.target_dataset}.records_control',
            #     custom_gcs_temp_location=options.temp_folder,
            #     schema=options.control_schema,
            #     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            #     additional_bq_parameters={
            #         'timePartitioning': {
            #             'type': 'DAY',
            #             'requirePartitionFilter': False
            #         }
            #     }
            # )

            p2.run()

    except Exception as e:
        print(f"error en la ejecución principal: {e}")
            

if __name__ == '__main__':
    # log_client = google.cloud.logging.Client()
    # logger = log_client.logger('motor_dwh')
    #
    # resource = google.cloud.logging.Resource(
    #     type="dataflow_step",
    #     labels={
    #         "project_id": "labuniformes",
    #         "job_name": "test_job",
    #         "region": "us-east4"
    #     }
    # )
    #
    # logger.log_text(
    #     "hola mundo", severity="ERROR", resource=resource, labels={ "service": "dataflow"}
    # )
    
    run()
    