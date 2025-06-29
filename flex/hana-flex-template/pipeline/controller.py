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
            default='new'
        )
        parser.add_argument(
            "--target_table",
            type=str,
            help="target table",
            required=False,
            default='/AIF/CFUNC_DET'
        )
        parser.add_argument(
            "--pipeline_id",
            type=str,
            help="pipeline_id",
            required=False,
            default='5117'
        )
        parser.add_argument(
            "--pipeline_name",
            type=str,
            help="pipeline_name",
            required=False,
            default='job5117_qas'
        )
        parser.add_argument(
            "--query",
            type=str,
            help="query",
            required=False,
            default='select MANDT, NS, IFNAME, IFVER, IDFUNC, FUNCICON, CREATEUSER, CREATE_DATE, CREATE_TIME from "{schema}"."/AIF/CFUNC_DET"'
        )
        parser.add_argument(
            "--secret_name_origin",
            type=str,
            help="Secret name origin",
            required=False,
            default = 'HANNA_SECRET'
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
            "--limit_init_load",
            type=str,
            help="Limite de tamaño para carga incial",
            required=False,
            default='1000000'
        )
        parser.add_argument(
            "--control_schema_name",
            type=str,
            help="nombre schema cifras control",
            required=False,
            default='registro_cifras_control.json'
        )
        parser.add_argument(
            "--debug",
            type=str,
            help="debug",
            required=False,
            default=False
        )
        parser.add_argument(
            "--host",
            type=str,
            help="host",
            required=False,
            default=''
        )
        parser.add_argument(
            "--port",
            type=str,
            help="port",
            required=False,
            default=''
        )
        parser.add_argument(
            "--username",
            type=str,
            help="username",
            required=False,
            default=''
        )
        parser.add_argument(
            "--password",
            type=str,
            help="password",
            required=False,
            default=''
        )
        parser.add_argument(
            "--database",
            type=str,
            help="database",
            required=False,
            default=''
        )
        parser.add_argument(
            "--run_id",
            type=str,
            help="run_id",
            required=False,
            default='manual__2025-04-11T07:45:36.415024+00:00'
        )
        parser.add_argument(
            "--schema",
            type=str,
            help="schema",
            required=False,
            default=''
        )
