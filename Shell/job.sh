#!/bin/bash

# Set variables
DAG_ID="<DAG_ID>"  
WEB_SERVER_URL="<WEB_SERVER_URL>"
ACCESS_TOKEN=$(gcloud auth application-default print-access-token)  # Obtain access token

# Trigger the DAG run
TRIGGER_RESPONSE=$(curl -s -X POST "$WEB_SERVER_URL/api/v1/dags/$DAG_ID/dagRuns" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{}')

# Check the response
if [ $? -eq 0 ]; then
  echo "DAG run triggered successfully."
else
  echo "Failed to trigger DAG run."
  exit 1  # Return 1 for error
fi

# Extract the DAG run ID from the response
DAG_RUN_ID=$(echo $TRIGGER_RESPONSE | jq -r '.dag_run_id')

# Loop to check the status every 60 seconds
while true; do
  sleep 60  # Wait for 60 seconds

  # Get the latest DAG run status
  STATUS_RESPONSE=$(curl -s -X GET "$WEB_SERVER_URL/api/v1/dags/$DAG_ID/dagRuns/$DAG_RUN_ID" \
    -H "Authorization: Bearer $ACCESS_TOKEN" \
    -H "Content-Type: application/json")

  # Extract the state of the DAG run
  DAG_STATE=$(echo $STATUS_RESPONSE | jq -r '.state')

  if [ "$DAG_STATE" == "success" ]; then
    echo "DAG executed successfully."
    exit 0  # Return 0 for success
  elif [ "$DAG_STATE" == "failed" ]; then
    echo "DAG execution failed."
    exit 1  # Return 1 for error
  fi
done