"""
 * Copyright (c) 2025 - Liverpool. All rights reserved
 *
 * Grupo de Asesores Profesionales en Servicios de Integracion {GAPSI} - CDMX - 2025
 *
"""

import logging

"""
Author: Jaime Arturo Chávez
Updated: 2025/05/07
Version: 1.0
Description: This module provides validation functionality for pipeline arguments.
It ensures all required fields are present and properly configured before pipeline execution.
It ensures they are not empty or None. Any other value (including False, 0, etc.)
    is considered valid.
"""

logger = logging.getLogger(__name__)

def validate_args(options):

    required_fields = [
        'dataflow_bucket_name',
        'dataflow_project',     
        'target_project',      
        'target_dataset',       
        'target_table',        
        'pipeline_name',        
        'stage',               
        'temp_folder',      
        'origin_bucket',       
        'prefix',               
        'filename',            
        'sufix',               
        'separator',           
        'ext',                 
        'datetime_format',    
        'delimitator',          
        'date_field_name',      
        'allow_multi_date',   
        'allow_accum_date',     
        'allow_multi_file',     
        'run_id',               
        'control_schema_path'   
    ]

    missing_fields = [
        field for field in required_fields 
        if getattr(options, field, None) is None or getattr(options, field, None) == ""
    ]

    if missing_fields:
        logger.info(f"these fields are missing: {', '.join(missing_fields)}")

