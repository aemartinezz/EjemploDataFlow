Flex templates para Dataflow

- Hana
- API
- CSV


Para hacer build de una nueva versión de forma manual, seguir los siguientes pasos:

1. docker build -t <respositorio de artifact registry> .
2. docker push <respositorio de artifact registry>
3. gcloud dataflow flex-template build gs://<path para archivo json> --image "<respositorio de artifact registry>:latest" --sdk-language "PYTHON" --metadata-file spec/metadata.json  
