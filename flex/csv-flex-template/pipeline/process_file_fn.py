"""
 * Copyright (c) 2025 - Liverpool. All rights reserved
 *
 * Grupo de Asesores Profesionales en Servicios de Integracion {GAPSI} - CDMX - 2025
 *
"""

import csv
import io
from typing import Iterator, Dict, Any
from apache_beam import DoFn
from pipeline.extract_rows_fn import ExtractRows

"""
Author: Jaime Arturo Chávez
Updated: 2025/05/07
Version: 1.0
Description: This class handles file-level processing by:
    - Opening and reading CSV files
    - Managing CSV reader configuration
    - Delegating row processing to ExtractRows
"""

class ProcessFile(DoFn):
    def __init__(self, options: 'PipelineOptions', *args, **kwargs) -> None:
        DoFn.__init__(self)
        self.options = options
        
    def process(self, element):
            with io.TextIOWrapper(element.open()) as text_file:
                reader = csv.reader(text_file, delimiter=self.options.delimitator)
                processor = ExtractRows(self.options)
                
                for row in reader:
                    yield from (
                        result for result in processor.process(row)
                        if result
                    )
