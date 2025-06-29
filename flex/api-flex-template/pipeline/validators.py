"""
 * Copyright (c) 2025 - Liverpool. All rights reserved
 *
 * Grupo de Asesores Profesionales en Servicios de Integracion {GAPSI} - CDMX - 2025
 *
"""

import logging

"""
 * @author: Jaime Arturo Chávez
 * @updated: 14-03-25
 * @description: starter class
 * @since-version: 1.0
"""

logger = logging.getLogger(__name__)

def validate_args(options):
    success = True
    description = ''

    if not options.dataflow_bucket_name:
        success = False
        description += 'dataflow_bucket_name, '

    if not options.dataflow_project:
        success = False
        description += 'dataflow_project, '

    if not options.target_project:
        success = False
        description += 'target_project, '
    
    if not options.target_dataset:
        success = False
        description += 'target_dataset, '

    if not options.target_table:
        success = False
        description += 'target_table, '

    if not options.pipeline_name:
        success = False
        description += 'pipeline_name, '

    if not options.stage:
        success = False
        description += 'stage, '

    if not options.temp_folder:
        success = False
        description += 'temp_folder'

    if not options.api_host:
        success = False
        description += 'api_host'

    if not options.api_protocol:
        success = False
        description += 'api_protocol'

    if not options.api_method:
        success = False
        description += 'api_method'

    if not options.api_port:
        success = False
        description += 'api_port'

    if not options.api_headers:
        success = False
        description += 'api_headers'

    if not options.api_queryparams:
        success = False
        description += 'api_queryparams'

    if not options.api_pathparams:
        success = False
        description += 'api_pathparams'


    if not success:
        logger.info(f"No se recibieron parametros: {description}")

