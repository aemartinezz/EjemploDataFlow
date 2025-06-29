"""
 * Copyright (c) 2025 - Liverpool. All rights reserved
 *
 * Grupo de Asesores Profesionales en Servicios de Integracion {GAPSI} - CDMX - 2025
 *
"""

from apache_beam.options.pipeline_options import PipelineOptions

"""
 * @author: Jaime Arturo Chávez
 * @updated: 
 * @description: starter class
 * @since-version: 1.0
"""

class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--dataflow_bucket_name",
            type=str,
            help="query without where segmentation",
            required=False,
            default='crp-qas-data-platform-bkt01'
        )
        parser.add_argument(
            "--dataflow_project",
            type=str,
            help="Platform project name",
            required=False,
            default='crp-qas-data-platform'
        )
        parser.add_argument(
            "--target_project",
            type=str,
            help="target project id",
            required=False,
            default='labuniformes'
        )
        parser.add_argument(
            "--target_dataset",
            type=str,
            help="target dataset",
            required=False,
            default='demo_dwh'
        )
        parser.add_argument(
            "--target_table",
            type=str,
            help="target table",
            required=False,
            default='orders'
        )
        parser.add_argument(
            "--pipeline_name",
            type=str,
            help="pipeline_name",
            required=False,
            default='job5085_qas'
        )
        parser.add_argument(
            "--stage",
            type=str,
            help="stage (dev/qa/prod)",
            required=False,
            default='qas'
        )
        parser.add_argument(
            "--temp_folder",
            type=str,
            help="temp folder",
            required=False,
            default='gs://crp-qas-data-platform-bkt01/temp/dev'
        )
        parser.add_argument(
            "--api_host",
            type=str,
            help="request url",
            required=False,
            default='qat-api.liverpool.com.mx/api/ordermanagement/orders'
            # default = 'ms-pipeline-62hxxzcprq-uc.a.run.app/api/pipelines/dummydatagenerator/orders'
        )
        parser.add_argument(
            "--api_protocol",
            type=str,
            help="Protocolo",
            required=False,
            default='https'
        )
        parser.add_argument(
            "--api_method",
            type=str,
            help="Method",
            required=False,
            default='GET'
        )
        parser.add_argument(
            "--api_port",
            type=str,
            help="Method",
            required=False,
            default=""
        )
        parser.add_argument(
            "--api_headers",
            type=str,
            help="Request headers json",
            required=False,
            default='[{"name":"apikey", "type":"secret", "value":"ORDERS_APIKEY"}]'
        )
        parser.add_argument(
            "--api_queryparams",
            type=str,
            help="Request params json",
            required=False,
            # default='[{"name": "start_update_date", "value": "", "type": "dynamic"}, {"name": "end_update_date", "value": "", "type": "dynamic"}, {"name": "size", "value": "2000", "type": "static"}, {"name": "page", "value": "0", "type": "static"}]'
            default='[{"name":"start_update_date","type":"dynamic","value":"2025-05-05T16:27:00"},{"name":"end_update_date","type":"dynamic","value":"2025-05-05T16:47:00"},{"name":"size","type":"static","value":"200"},{"name":"page","type":"static","value":"0"}]'
        )
        parser.add_argument(
            "--api_pathparams",
            type=str,
            help="Method",
            required=False,
            default=''
        )
        parser.add_argument(
            "--dictionary",
            type=str,
            help="Dictionary definition",
            required=False,
            default=None
        )
        parser.add_argument(
            "--control_schema",
            type=str,
            help="Control schema definition",
            required=False,
            default=None
        )
        parser.add_argument(
            "--run_id",
            type=str,
            help="dag run id",
            required=False,
            default='manual__2025-04-11T07:45:36.415024+00:00'
        )
        parser.add_argument(
            "--control_schema_path",
            type=str,
            help="control_schema_path",
            required=False,
            default='gs://crp-qas-data-platform-bkt01/registro_cifras_control.json'
        )
        parser.add_argument(
            "--full_ingest_size",
            type=str,
            help="full_ingest_size",
            required=False,
            default=2000
        )

