"""
 * Copyright (c) 2025 - Liverpool. All rights reserved
 *
 * Grupo de Asesores Profesionales en Servicios de Integracion {GAPSI} - CDMX - 2025
 *
"""

import json
import logging
from datetime import datetime
from google.cloud.secretmanager import SecretManagerServiceClient
from google.cloud.storage import Client as GCS_Client
from google.cloud import bigquery
from hdbcli import dbapi

"""
 * @author: Jaime Arturo Chávez
 * @updated: 
 * @description: starter class
 * @since-version: 1.0
"""

def default_converter(o):
    if isinstance(o, datetime):
        return o.isoformat()
        # return o.strftime("%Y-%m-%dT%H:%M:%SZ")
    # Si tienes otros tipos que necesiten conversión especial para BQ, añádelos aquí.
    # Por ejemplo, si tuvieras Decimal y BQ espera float o string:
    # if isinstance(o, decimal.Decimal):
    #     return float(o) # o str(o)
    raise TypeError("Object of type '%s' is not JSON serializable" % type(o).__name__)


def to_bigquery_row(element: dict) -> dict:
    """
    Prepara un diccionario para ser escrito en BigQuery.
    Convierte tipos de datos específicos (como datetime) a formatos compatibles con BigQuery.
    Evita la costosa doble conversión JSON.
    """
    if not isinstance(element, dict):
        logging.error(f"Elemento no es un diccionario: {type(element)}")
        # Decide cómo manejar esto: retornar el elemento, un dict vacío, o lanzar error
        return element

    processed_element = {} # Crear un nuevo diccionario es más seguro
    for key, value in element.items():
        if isinstance(value, datetime):
            processed_element[key] = value.isoformat()
        # Si tienes otros tipos que default_converter manejaba y necesitan conversión,
        # añade lógica aquí. Ejemplo:
        # elif isinstance(value, decimal.Decimal):
        #     processed_element[key] = float(value)
        elif value is not None:
            # Si no es un tipo especial y no es None, se pasa tal cual.
            # BigQuery maneja bien int, float, str, bool, bytes, list, dict (compatibles con JSON).
            # Si algún valor pudiera ser de un tipo no compatible directamente Y que
            # antes se \"arreglaba\" con json.dumps + default_converter, considera
            # convertirlo a string como fallback o manejarlo explícitamente.
            if not isinstance(value, (str, int, float, bool, bytes, list, dict)):
                try:
                    # Esta llamada es si default_converter es más complejo y quieres reusarlo
                    # para tipos no datetime. Si default_converter SOLO hace datetime,
                    # entonces esta llamada no es necesaria aquí, ya que datetime ya se manejó.
                    # Si default_converter es solo para datetime, un simple str(value) podría ser suficiente
                    # o manejar el tipo específico.
                    # processed_element[key] = default_converter(value) # Comentado si default_converter es solo para datetime
                    logging.warning(f"Valor para la clave '{key}' es de tipo no básico y no datetime: {type(value)}. Convirtiendo a string.")
                    processed_element[key] = str(value) # Fallback general
                except TypeError: # En caso que default_converter falle para este tipo
                    logging.error(f"No se pudo convertir (TypeError) el valor para la clave '{key}': {value}. Convirtiendo a string.")
                    processed_element[key] = str(value)
            else:
                processed_element[key] = value # Tipos básicos se mantienen
        else:
            processed_element[key] = None # Nones se mantienen

    return processed_element


def get_secret(dataflow_project, secret_name_origin):
    secret_client = SecretManagerServiceClient()
    secret_name = f"projects/{dataflow_project}/secrets/{secret_name_origin}/versions/latest"
    return json.loads(secret_client.access_secret_version(name=secret_name).payload.data.decode("UTF-8"))


def get_conn_info(dataflow_project, secret_name_origin):
    secret = get_secret(dataflow_project, secret_name_origin)
    conn_info = (f'host={secret["host"]} port={secret["port"]} dbname={secret["database"]} user={secret["username"]} '
                f'password={secret["password"]}')
    return conn_info


def get_data_dict_file(pipeline_name, stage, dataflow_bucket_name):
    gcs_client = GCS_Client()
    bucket = gcs_client.get_bucket(f'{dataflow_bucket_name}')
    blob = bucket.blob(f"dictionaries/{stage}/db/{pipeline_name}.json")
    return json.loads(blob.download_as_string().decode('utf-8'))


def get_control_schema(dataflow_bucket_name, schema_name):
    gcs_client = GCS_Client()
    bucket = gcs_client.get_bucket(f'{dataflow_bucket_name}')
    blob = bucket.blob(f"{schema_name}")
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


def get_new_query(options):    
    current_time = (datetime.now()).strftime('%Y-%m-%d %H:%M:%S')

    try:
        client = bigquery.Client(project=options.dataflow_project)
        table_id = f'{options.target_project}.{options.target_dataset}.records_control'
        
        try:
            client.get_table(table_id)
            query = f'SELECT execution_time FROM `{table_id}` WHERE pipeline_name = "{options.pipeline_name}" ORDER BY execution_time DESC LIMIT 1'
            query_job = client.query(query)
            results = query_job.result()
            execution_time = next(iter(results), {}).get('execution_time')

            if execution_time is not None:
                return f"{options.query} WHERE CREATE_DATE > '{execution_time} -0600' AND CREATE_DATE <= '{current_time} -0600'"
            else:
                logging.info(f"Previous record does not exist")
                return f"{options.query} WHERE CREATE_DATE <= '{current_time} -0600' limit {options.limit_init_load}"

        except Exception as e:
            logging.info(f"Table does not exist")
            return f"{options.query} WHERE CREATE_DATE <= '{current_time} -0600' limit {options.limit_init_load}"

    except Exception as e:
        logging.error(f"Error in get_new_query: {e}")
        raise RuntimeError(f"Error in get_new_query: {str(e)}")


def read_from_hana(options):
    conn = dbapi.connect(
        address=options.host,
        port=options.port,
        user=options.username,
        password=options.password,
        databaseName=options.database,
        encrypt='true',
        sslValidateCertificate='false'
    )
    cursor = conn.cursor()
    cursor.execute(options.query)
    columns = [desc[0] for desc in cursor.description]

    # Establecer el tamaño del array del cursor para optimizar la recuperación de datos de la red
    # Ajustado a 5000 para mitigar problemas de OOM
    fetch_size_hardcoded = 5000
    cursor.arraysize = fetch_size_hardcoded
    logging.info(f"Reading from HANA with fixed fetch_size: {fetch_size_hardcoded}")

    while True:
        rows = cursor.fetchmany(fetch_size_hardcoded)
        if not rows:
            break
        for row in rows:
            row_dict = dict(zip(columns, row))
            yield row_dict
    
    cursor.close()
    conn.close()


