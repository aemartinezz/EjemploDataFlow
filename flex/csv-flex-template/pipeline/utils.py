"""
 * Copyright (c) 2025 - Liverpool. All rights reserved
 *
 * Grupo de Asesores Profesionales en Servicios de Integracion {GAPSI} - CDMX - 2025
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from pathlib import Path

from google.cloud.exceptions import NotFound
from google.cloud.secretmanager import SecretManagerServiceClient
from google.cloud.storage import Client as GCS_Client
from google.cloud import bigquery

"""
Author: Jaime Arturo Chávez
Updated: 2025/05/07
Version: 1.0
Description: Utility class for Dataflow pipeline operations.
"""

class DataflowUtils:
    
    @staticmethod
    def default_converter(obj: Any) -> str:
        """Convert datetime objects to ISO format for JSON serialization."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type '{type(obj).__name__}' is not JSON serializable")

    @staticmethod
    def to_bigquery_row(element: Dict[str, Any]) -> Dict[str, Any]:
        """Convert dictionary to BigQuery compatible row format."""
        return json.loads(json.dumps(element, default=DataflowUtils.default_converter))

    @staticmethod
    def get_secret(dataflow_project: str, secret_name_origin: str) -> str:
        """Retrieve secret from Secret Manager."""
        client = SecretManagerServiceClient()
        secret_name = f"projects/{dataflow_project}/secrets/{secret_name_origin}/versions/latest"
        return client.access_secret_version(name=secret_name).payload.data.decode("UTF-8")

    @classmethod
    def get_conn_info(cls, dataflow_project: str, secret_name_origin: str) -> str:
        """Get connection information from secret."""
        secret = cls.get_secret(dataflow_project, secret_name_origin)
        return (
            f'host={secret["host"]} '
            f'port={secret["port"]} '
            f'dbname={secret["database"]} '
            f'user={secret["username"]} '
            f'password={secret["password"]}'
        )

    @staticmethod
    def get_data_dict_file(pipeline_name: str, stage: str, dataflow_bucket_name: str) -> Dict[str, Any]:
        """Retrieve data dictionary file from GCS."""
        client = GCS_Client()
        bucket = client.get_bucket(dataflow_bucket_name)
        blob = bucket.blob(f"dictionaries/{stage}/file/{pipeline_name}.json")
        return json.loads(blob.download_as_string().decode('utf-8'))

    @staticmethod
    def get_control_schema(control_schema_path: str) -> Dict[str, Any]:
        """Get control schema from GCS."""
        path_parts = control_schema_path.split('/')
        bucket_name = path_parts[2]
        folder = '/'.join(path_parts[3:]).rstrip('/')

        client = GCS_Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(folder)
        return json.loads(blob.download_as_string().decode('utf-8'))

    @classmethod
    def get_schema(cls, pipeline_name: str, stage: str, dataflow_bucket_name: str) -> str:
        """Generate schema string from data dictionary."""
        data = cls.get_data_dict_file(pipeline_name, stage, dataflow_bucket_name)
        return ', '.join(f'{elem["name"]}:{elem["type"]}' for elem in data)

    @staticmethod
    def create_control_record(options: Any, count: int) -> Dict[str, Union[str, int]]:
        return {
            'pipeline_name': options.pipeline_name,
            'run_id': options.run_id,
            'records_read': count,
            'records_written': count,
            'execution_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

    @staticmethod
    def create_control_record2(options: Any, read_count: int, written_count: int) -> Dict[str, Union[str, int]]:
        return {
            'pipeline_name': options.pipeline_name,
            'run_id': options.run_id,
            'records_read': read_count,
            'records_written': written_count,
            'execution_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

    @staticmethod
    def get_start_date(options: Any) -> Optional[datetime]:
        """Get start date from BigQuery records control table."""
        try:
            client = bigquery.Client(project=options.dataflow_project)
            table_id = f'{options.target_project}.{options.target_dataset}.records_control'

            try:
                client.get_table(table_id)
                query = (
                    f'SELECT execution_time FROM `{table_id}` '
                    f'WHERE pipeline_name = "{options.pipeline_name}" '
                    'ORDER BY execution_time DESC LIMIT 1'
                )
                results = client.query(query).result()
                return next(iter(results), {}).get('execution_time')

            except Exception as e:
                logging.info("Previous record does not exist")
                return None

        except Exception as e:
            logging.error(f"Error in get_start_date: {e}")
            return None

    @staticmethod
    def get_base_file_name(options: Any) -> str:
        """Generate base file name from options."""
        elements = []
        for attr in ['prefix', 'filename', 'sufix']:
            value = getattr(options, attr, '')
            if value:
                elements.append(value)
        return options.separator.join(elements)

    @classmethod
    def generate_file_paths(cls, options: Any, current_time: datetime) -> List[str]:
        """Generate list of file paths based on options and time range."""
        file_paths = []
        start_date = None

        if options.datetime_format and options.datetime_format.strip():
            start_date = cls.get_start_date(options)
            if start_date is None:
                start_date = datetime.strptime('2020-01-01-00-00', options.datetime_format)

        client = GCS_Client()
        bucket_name = options.origin_bucket.split('/')[2]
        bucket = client.get_bucket(bucket_name)
        folder = '/'.join(options.origin_bucket.split('/')[3:]).rstrip('/')
        base_filename = cls.get_base_file_name(options)
        has_date = bool(options.datetime_format and options.datetime_format.strip())

        for blob in bucket.list_blobs(prefix=folder):
            try:
                if not blob.name.startswith(f"{folder}/{base_filename}"):
                    continue

                if has_date:
                    file_date_str = blob.name.split(options.separator)[-1]
                    file_date = datetime.strptime(file_date_str, options.datetime_format)

                    if start_date < file_date <= current_time:
                        file_paths.append(f'gs://{bucket_name}/{blob.name}')
                else:
                    expected_filename = f"{folder}/{base_filename}{options.ext}"
                    if blob.name == expected_filename:
                        file_paths.append(f'gs://{bucket_name}/{blob.name}')

            except (ValueError, IndexError):
                continue

        return file_paths
