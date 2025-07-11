"""
 * Copyright (c) 2025 - Liverpool. All rights reserved
 *
 * Grupo de Asesores Profesionales en Servicios de Integracion {GAPSI} - CDMX - 2025
 *
"""

import datetime
import json
import requests
import os
import time
import subprocess
from airflow import models
from airflow.exceptions import AirflowFailException
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

"""
 * @author: Jorge Puc Carrillo
 * @updated:
 * @description: hana dag template
 * @since-version: 1.0
"""

#Datos Generales
job_name = '<JOB_NAME>'
pipeline_id = '<PIPELINE_ID>'
pipeline_name = '<PIPELINE_NAME>'
flex_template = 'api-flex-template.json'
stage = '<STAGE>'
dataflow_bucket_name = '<DATAFLOW_BUCKET_NAME>'
dataflow_project = '<DATAFLOW_PROJECT>'
target_project = '<TARGET_PROJECT>'
target_table = '<TARGET_TABLE>'
target_dataset = '<TARGET_DATASET>'
datetime_start = '<DATETIME_START>'
service_account = '<SERVICE_ACCOUNT>'
subnetwork = '<SUBNETWORK>'
region = '<REGION>'
network_tag = '<NETWORK_TAG>'
status_url = '<STATUS_URL>'
control_schema_path = 'gs://crp-qas-data-platform-bkt01/registro_cifras_control.json'

#Datos Especificos
api_method = '<API_METHOD>'
api_host = '<API_HOST>'
api_headers = '<API_HEADERS>'
api_port = '<API_PORT>'
api_pathparams = '<API_PATH_PARAMS>'
api_queryparams = '<API_QUERY_PARAMS>'
api_protocol = '<API_PROTOCOL>'


default_dag_args = {
    "start_date": datetime.datetime(2025, 2, 10, 12, 59),
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
}


def get_token(**kwargs):
    command = ['gcloud', 'auth', 'print-access-token']
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        token = result.stdout.strip()
        print("Output:", token)
        kwargs['ti'].xcom_push(key='token', value=token)
    except subprocess.CalledProcessError as e:
        print("An error occurred:")
        print(e.stderr)
        raise AirflowFailException


def trigger_job(**kwargs):
    run_id = kwargs['dag_run'].run_id
    token = kwargs['ti'].xcom_pull(task_ids='get_token', key='token')
    url = f'https://dataflow.googleapis.com/v1b3/projects/{dataflow_project}/locations/{region}/flexTemplates:launch'
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    data = {
        "launch_parameter": {
            "jobName": f'{job_name}',
            "containerSpecGcsPath": f'gs://{dataflow_bucket_name}/flex-template/{flex_template}',
            "environment": {
                # "additionalExperiments": [f"use_network_tags={network_tag}}"],
                "additionalExperiments": [],
                "additionalUserLabels": {},
                "ipConfiguration": "WORKER_IP_PRIVATE",
                "subnetwork": f'https://www.googleapis.com/compute/v1/{subnetwork}'
            },
            "parameters": {
                "run_id": f"{run_id}",
                "dataflow_bucket_name": f'{dataflow_bucket_name}',
                "dataflow_project": f'{dataflow_project}',
                'target_project': f'{target_project}',
                'target_dataset': f'{target_dataset}',
                'target_table': f'{target_table}',
                "machine_type": 'n1-standard-1',
                'pipeline_id': f'{pipeline_id}',
                'pipeline_name': f'{pipeline_name}',
                "service_account_email": f'{service_account}',
                'stage': f'{stage}',
                "staging_location": f'gs://{dataflow_bucket_name}/staging/{stage}',
                "temp_folder": f'gs://{dataflow_bucket_name}/temp/{stage}',
                'api_method': f'{api_method}',
                'api_host': f'{api_host}',
                'api_headers': f'{api_headers}',
                'api_port': f'{api_port}',
                'api_pathparams': f'{api_pathparams}',
                'api_queryparams': f'{api_queryparams}',
                'api_protocol': f'{api_protocol}'
            }
        }
    }

    try:
        response = requests.post(url, headers=headers, data=json.dumps(data))
        json_response = response.json()
        print(json_response)
        response.raise_for_status()
        kwargs['ti'].xcom_push(key='job_id', value=json_response['job']['id'])
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        raise AirflowFailException
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}")
        raise AirflowFailException
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error occurred: {timeout_err}")
        raise AirflowFailException
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred: {req_err}")
        raise AirflowFailException
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise AirflowFailException


def check_status(**kwargs):
    job_id = kwargs['ti'].xcom_pull(task_ids='execute_dataflow_job', key='job_id')
    token = kwargs['ti'].xcom_pull(task_ids='get_token', key='token')
    status = None
    if job_id is None:
        print('job_id is missing')
        raise AirflowFailException
    job_url = f'https://dataflow.googleapis.com/v1b3/projects/{dataflow_project}/locations/{region}/jobs/{job_id}'
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    while status is None:
        time.sleep(60)
        response = requests.get(job_url, headers=headers).json()
        if response['currentState'] == 'JOB_STATE_FAILED':
            status = 'error'
        elif response['currentState'] == 'JOB_STATE_DONE':
            status = 'ok'
        print(f"Current status: {response['currentState']}")
    change_status_url = f'{status_url}/api/pipelines/status/'
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    data = {
        "pipeline_id": f'{pipeline_id}',
        "status": f'{status}'
    }
    response = requests.put(change_status_url, headers=headers, data=json.dumps(data)).json()
    print(response)


with models.DAG(
    f"{pipeline_name}",
    schedule_interval=None,
    description='Pipeline para leer API y cargar en bigquery',
    default_args=default_dag_args,
) as dag:
    get_token = PythonOperator(
        task_id="get_token",
        python_callable=get_token,
        provide_context=True,
    )
    create_request = PythonOperator(
        task_id="execute_dataflow_job",
        python_callable=trigger_job,
        provide_context=True,
    )
    status = PythonOperator(
        task_id="check_status",
        python_callable=check_status,
        provide_context=True,
    )

get_token >> create_request >> status
