"""
 * Copyright (c) 2025 - Liverpool. All rights reserved
 *
 * Grupo de Asesores Profesionales en Servicios de Integracion {GAPSI} - CDMX - 2025
 *
"""

import apache_beam as beam
import math
import requests
import time
from pipeline.utils import *

"""
 * @author: Jaime Arturo Chávez
 * @updated: 
 * @description: MakeRequest class
 * @since-version: 1.0
"""

class MakeRequestFn(beam.DoFn):
    def __init__(self, options, *unused_args, **unused_kwargs):
        super().__init__(*unused_args, **unused_kwargs)
        self.options = options

    def process(self, element):
        headers = {}
        params = {}
        current_time = datetime.now()
        orders = []
        page = 0
        limit = 1

        dictionary = get_jsonpath_dict(self.options.dictionary)

        if self.options.api_pathparams is not None and self.options.api_pathparams != '' and self.options.api_pathparams != '{}':
            pathparams = process_json(self.options.api_pathparams, self.options.dataflow_project)
            for key, value in pathparams.items():
                self.options.api_host = self.options.api_host.replace(f'<{key}>', value)

        if self.options.api_headers is not None and self.options.api_headers != "" and self.options.api_headers != "{}":
            headers = process_json(self.options.api_headers, self.options.dataflow_project)

        headers['Accept'] = '*/*'
        headers['Accept-Encoding'] = 'gzip, deflate, br'
        headers['Connection'] = 'keep-alive'
        headers['User-Agent'] = 'PostmanRuntime/7.43.4'

        if self.options.api_queryparams is not None and self.options.api_queryparams != "" and self.options.api_queryparams != "{}":
            params = process_json(self.options.api_queryparams, self.options.dataflow_project)

            params['start_update_date'] = None
            start_date = get_start_date(self.options)

            if start_date is not None:
                params['start_update_date'] = start_date.strftime('%Y-%m-%dT%H:%M:%S')
            else:
                params['end_update_date'] = current_time.strftime('%Y-%m-%dT%H:%M:%S')
                params['size'] = self.options.full_ingest_size

        url = f"{self.options.api_protocol}://{self.options.api_host}"

        while page < limit:
            params['page'] = page

            try:
                if self.options.api_method == 'POST':
                    response = requests.post(url, headers=headers, params=params)
                    print(response.status_code)
                else:
                    response = requests.get(url, headers=headers, params=params)
                    print(response.status_code)

                if response.status_code == 200 or response.status_code == 201:
                    limit = math.ceil(response.json()['total'] / int(params['size']))
                    data = json.loads(response.content.decode('utf-8'))
                    filtered = filter_by_jsonpath(data['orders'], dictionary)
                    orders.extend(filtered)
                    page += 1
                elif response.status_code == 502:
                    time.sleep(30)
                    continue
                else:
                    logging.error(f"Request failed with status code: {response.status_code}")
                    logging.error(f"Response content: {response.content.decode('utf-8')}")
                    logging.error(f"Request URL: {url}")
                    raise RuntimeError(f"Request failed with status code: {response.status_code}")

            except requests.exceptions.RequestException as e:
                logging.error(f"Request error occurred while calling {url}")
                logging.error(f"Error details: {str(e)}")
                raise RuntimeError(f"Request error: {str(e)}")

            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse JSON response from {url}")
                logging.error(f"Error details: {str(e)}")
                raise RuntimeError(f"JSON decode error: {str(e)}")

            except Exception as e:
                logging.error(f"Unexpected error while processing request to {url}")
                logging.error(f"Error type: {type(e).__name__}")
                logging.error(f"Error details: {str(e)}")
                raise RuntimeError(f"Unexpected error: {str(e)}")
        logging.info(f"Total orders collected: {len(orders)}")
        return orders
