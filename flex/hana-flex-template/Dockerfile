FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest

RUN mkdir -p /dataflow/template
WORKDIR /dataflow/template

COPY . /dataflow/template

RUN apt-get update \
    && apt-get install -y libffi-dev git \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache -r requirements.txt 
     

ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="/dataflow/template/setup.py"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/dataflow/template/main.py"
ENV FLEX_TEMPLATE_PYTHON_PY_OPTIONS=""
ENV FLEX_TEMPLATE_PYTHON_EXTRA_PACKAGES=""

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]