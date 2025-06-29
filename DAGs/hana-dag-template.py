"""
 * Copyright (c) 2025 - Liverpool. All rights reserved
 *
 * Grupo de Asesores Profesionales en Servicios de Integracion {GAPSI} - CDMX - 2025
 *
"""

import json
import pendulum
import requests
import os
import time
import subprocess
from airflow import models
from airflow.exceptions import AirflowFailException
from airflow.operators.python_operator import PythonOperator
from google.cloud import pubsub_v1

"""
 * @author: Jaime Arturo Chavez
 * @updated: 
 * @description: hana dag template
 * @since-version: 1.0
"""

#Datos Generales
originType = "db"
job_name = '<JOB_NAME>'
pipeline_id = '<PIPELINE_ID>'
pipeline_name = '<PIPELINE_NAME>'    
flex_template = 'hana-flex-template.json'        
stage = '<STAGE>'
dataflow_bucket_name = '<DATAFLOW_BUCKET_NAME>'
dataflow_project = '<DATAFLOW_PROJECT>'    
target_project = '<TARGET_PROJECT>'  
target_dataset =  '<TARGET_DATASET>' 
target_table = '<TARGET_TABLE>'  
datetime_start = '<DATETIME_START>' 
tz = 'America/Mexico_City'
service_account = '<SERVICE_ACCOUNT>' 
subnetwork = '<SUBNETWORK>' 
region = '<REGION>' 
network_tag = '<NETWORK_TAG>' 
status_url = '<STATUS_URL>' 
control_schema_path = '<CONTROL_SCHEMA_PATH>'
environment = '<ENVIRONMENT>'
env_project = f'crp-{environment}-data-platform'
env_topic = f'{environment}-pipelines-status'

#Datos Especificos
query = '<QUERY>'     
limit_init_load = '1000000' 
secret_name_origin =  '<SECRET_NAME_ORIGIN>'


interval = None if pendulum.now().in_timezone(tz).diff(pendulum.parse(datetime_start, tz=tz)).in_minutes() > 15 else '*/5 * * * *'

default_dag_args = {
    "start_date": pendulum.parse(datetime_start, tz=tz),
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

if pendulum.now().in_timezone(tz).diff(pendulum.parse(datetime_start, tz=tz)).in_minutes() <= 15:
    default_dag_args['end_date'] = pendulum.parse(datetime_start, tz=tz).add(minutes=4)


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
                "tempLocation": f"gs://{dataflow_bucket_name}/temp/{stage}/{job_name}",
                "additionalExperiments": f"use_network_tags={network_tag}",
                "ipConfiguration": "WORKER_IP_PRIVATE",
                "maxWorkers": 3,
                "numWorkers": 1,
                "subnetwork": f'https://www.googleapis.com/compute/v1/{subnetwork}'
            },
            "parameters": {
                #"run_id": f"{run_id}",
                "dataflow_bucket_name": f'{dataflow_bucket_name}',
                "dataflow_project": f'{dataflow_project}',
                'target_project': f'{target_project}',
                'target_dataset': f'{target_dataset}',
                'target_table': f'{target_table}',
                #'control_schema_path': f'{control_schema_path}',
                "machine_type": 'n1-standard-1',
                'pipeline_id': f'{pipeline_id}',
                'pipeline_name': f'{pipeline_name}',
                'query': f'{query}',
                'secret_name_origin': f'{secret_name_origin}',
                "service_account_email": f'{service_account}',
                'stage': f'{stage}',
                "staging_location": f'gs://{dataflow_bucket_name}/staging/{stage}',
                "temp_folder": f'gs://{dataflow_bucket_name}/temp/{stage}/{job_name}',
                "limit_init_load": f'{limit_init_load}'
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
        elif response['currentState'] == 'JOB_STATE_CANCELLED':
            status = 'cancelled'
        elif response['currentState'] == 'JOB_STATE_DONE':
            status = 'ok'

        print(f"Current response.status: {response['currentState']}")

    print(f"Current status: {status}")
    if status != None and (status == 'error' or status == 'cancelled' or status == 'ok'):

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(env_project, env_topic)
    
        message_data = {
            "pipelineId": f'{pipeline_id}',
            "status": f'{status}',
            "stage": f'{stage}',
            "originType": originType,
        }
        message_data = json.dumps(message_data).encode('utf-8')
    
        future = publisher.publish(topic_path, message_data)
        print(f"Published message to {topic_path}")
    
        try:
            future.result()
        except Exception as e:
            print(f"Failed to publish message: {e}")
            raise

    print(f"Message published successfully.")


with models.DAG(
    f"{pipeline_name}",
    schedule_interval=interval,
    description='Pipeline para leer tablas de Hana y cargar en bigquery',
    default_args=default_dag_args,
    max_active_runs=1,
    catchup=False,
) as dag:
    get_token = PythonOperator(
        task_id="get_token",
        python_callable=get_token,
        provide_context=True,
    )
    create_request = PythonOperator(
        task_id="execute_dataflow_job",
        python_callable=trigger_job,
        provide_context = True,
    )

    status = PythonOperator(
        task_id="check_status",
        python_callable=check_status,
        provide_context=True,
    )

get_token >> create_request >> status
