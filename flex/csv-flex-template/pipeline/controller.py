"""
 * Copyright (c) 2025 - Liverpool. All rights reserved
 *
 * Grupo de Asesores Profesionales en Servicios de Integracion {GAPSI} - CDMX - 2025
 *
"""

from apache_beam.options.pipeline_options import PipelineOptions

"""
Author: Jaime Arturo Chávez
Updated: 2025/05/07
Version: 1.0
Descripción: Define the options arguments for the pipeline.
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
            default='test_file'
        )
        parser.add_argument(
            "--pipeline_name",
            type=str,
            help="pipeline_name",
            required=False,
            default='job5013_qas'
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
            "--origin_bucket",
            type=str,
            help="Bucket de origen",
            required=False,
            default='gs://crp-qas-data-platform-bkt03/source_csv/'
        )
        parser.add_argument(
            "--prefix",
            type=str,
            help="prefix",
            required=False,
            default='TEST'
        )
        parser.add_argument(
            "--filename",
            type=str,
            help="filename",
            required=False,
            default='FILEJAPUC'
        )
        parser.add_argument(
            "--sufix",
            type=str,
            help="sufix",
            required=False,
            default='DATE'
        )
        parser.add_argument(
            "--separator",
            type=str,
            help="separator",
            required=False,
            default='_'
        )
        parser.add_argument(
            "--ext",
            type=str,
            help="ext",
            required=False,
            default='.csv'
        )
        parser.add_argument(
            "--datetime_format",
            type=str,
            help="datetime_format",
            required=False,
            # default='%Y-%m-%d-%H-%M'
            default=''
        )
        parser.add_argument(
            "--delimitator",
            type=str,
            help="delimitator",
            required=False,
            default=','
        )
        parser.add_argument(
            "--date_field_name",
            type=str,
            help="date_field_name",
            required=False,
            default='last-updated'
        )
        parser.add_argument(
            "--allow_multi_date",
            type=str,
            help="allow_multi_date",
            required=False,
            default=False
        )
        parser.add_argument(
            "--allow_accum_date",
            type=str,
            help="allow_accum_date",
            required=False,
            default=False
        )
        parser.add_argument(
            "--allow_multi_file",
            type=str,
            help="allow_multi_file",
            required=False,
            default=False
        )
        parser.add_argument(
            "--table_schema",
            type=str,
            help="table_schema",
            required=False,
            default=''
        )
        parser.add_argument(
            "--uri",
            type=str,
            help="uri",
            required=False,
            default=''
        )
        parser.add_argument(
            "--schema_fields",
            type=str,
            help="schema_fields",
            required=False,
            default=''
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

