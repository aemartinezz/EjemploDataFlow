"""
 * Copyright (c) 2025 - Liverpool. All rights reserved
 *
 * Grupo de Asesores Profesionales en Servicios de Integracion {GAPSI} - CDMX - 2025
 *
"""

from typing import Dict, List, Iterator
from apache_beam import DoFn

"""
Author: Jaime Arturo Chávez
Updated: 2025/05/07
Version: 1.0
Description: This class handles CSV row processing by:
    - Processing the header row
    - Cleaning and validating input data
    - Generating dictionaries with required fields
"""

class ExtractRows(DoFn):
    def __init__(self, options: 'PipelineOptions', *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.options = options
        self.schema_fields = options.schema_fields
        self._is_header_processed = False
        self._headers: List[str] = []

    @property
    def headers(self) -> List[str]:
        return self._headers

    @staticmethod
    def clean_field(value: str) -> str:
        return value.strip().strip('"').strip()

    def process(self, element: List[str]) -> Iterator[Dict[str, str]]:
        if not self._is_header_processed:
            self._headers = [self.clean_field(h) for h in element]
            self._is_header_processed = True
            return

        if len(element) != len(self.headers):
            return

        result = {
            header: self.clean_field(value)
            for header, value in zip(self.headers, element)
            if header in self.schema_fields
        }
        
        if result:
            yield result
