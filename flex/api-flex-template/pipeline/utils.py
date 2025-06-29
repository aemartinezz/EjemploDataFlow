"""
 * Copyright (c) 2025 - Liverpool. All rights reserved
 *
 * Grupo de Asesores Profesionales en Servicios de Integracion {GAPSI} - CDMX - 2025
 *
"""

import json
import logging
from datetime import datetime, timedelta
from google.cloud.secretmanager import SecretManagerServiceClient
from google.cloud.storage import Client as GCS_Client
from google.cloud import bigquery
from jsonpath import JSONPath

"""
 * @author: Jaime Arturo Chávez
 * @updated: 
 * @description: starter class
 * @since-version: 1.0
"""


def default_converter(o):
    if isinstance(o, datetime):
        return o.isoformat()
    raise TypeError("Object of type '%s' is not JSON serializable" % type(o).__name__)


def to_bigquery_row(element):
    return json.loads(json.dumps(element, default=default_converter))


def get_secret(dataflow_project, secret_name_origin):
    secret_client = SecretManagerServiceClient()
    secret_name = f"projects/{dataflow_project}/secrets/{secret_name_origin}/versions/latest"
    return secret_client.access_secret_version(name=secret_name).payload.data.decode("UTF-8")


def get_conn_info(dataflow_project, secret_name_origin):
    secret = get_secret(dataflow_project, secret_name_origin)
    conn_info = (f'host={secret["host"]} port={secret["port"]} dbname={secret["database"]} user={secret["username"]} '
                f'password={secret["password"]}')
    return conn_info


def get_data_dict_file(pipeline_name, stage, dataflow_bucket_name):
    gcs_client = GCS_Client()
    bucket = gcs_client.get_bucket(f'{dataflow_bucket_name}')
    blob = bucket.blob(f"dictionaries/{stage}/api/{pipeline_name}.json")
    return json.loads(blob.download_as_string().decode('utf-8'))


def get_control_schema(control_schema_path):
    bucket_name = control_schema_path.split('/')[2]
    folder = '/'.join(control_schema_path.split('/')[3:]).rstrip('/')

    gcs_client = GCS_Client()
    bucket = gcs_client.get_bucket(bucket_name)
    blob = bucket.blob(folder)
    return json.loads(blob.download_as_string().decode('utf-8'))


def get_schema(pipeline_name, stage, dataflow_bucket_name):
    data = get_data_dict_file(pipeline_name, stage, dataflow_bucket_name)
    schema = ''
    for index, elem in enumerate(data):
        schema += f'{elem["name"]}:{elem["type"]}'
        if index < len(data) - 1:
            schema += ', '
    return schema


def create_control_record(options, read_count):
    return {
        'pipeline_name': options.pipeline_name,
        'run_id': options.run_id,
        'records_read': read_count,
        'records_written': read_count,
        'execution_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    
def process_json(json_string, dataflow_project):
    processed = {}
    input_json = json.loads(json_string)

    for elem in input_json:
        if elem['type'] == 'secret':
            secret = get_secret(dataflow_project, elem['value'])
            processed[elem["name"]] = secret

        elif elem['type'] == 'static':
            processed[elem["name"]] = elem["value"]
        else:
            processed[elem["name"]] = elem["value"]
    return processed


def get_jsonpath_dict(dictionary):
    json_paths = {}
    for elem in dictionary:
        json_paths.update({elem['name']: elem['jsonPath']})
    return json_paths


def filter_by_jsonpath(input_orders, dictionary):
    output_orders = []
    for order in input_orders:
        new_order = {}
        for key, value in dictionary.items():
            new_order.update({key: JSONPath(value).parse(order)[0]})
        output_orders.append(new_order)
    return output_orders


def get_start_date(options):
    try:
        client = bigquery.Client(project=options.dataflow_project)
        table_id = f'{options.target_project}.{options.target_dataset}.records_control'
        
        try:
            
            client.get_table(table_id)
            query = f'SELECT execution_time FROM `{table_id}` WHERE pipeline_name = "{options.pipeline_name}" ORDER BY execution_time DESC LIMIT 1'
            query_job = client.query(query)
            results = query_job.result()
            execution_time = next(iter(results), {}).get('execution_time')
            return execution_time

        except Exception as e:
            logging.info(f"Previous record does not exist")
            # logging.info('error', e)
            return None

    except Exception as e:
        logging.error(f"Error in get_new_query: {e}")
        raise RuntimeError(f"Error in get_start_date: {str(e)}")
