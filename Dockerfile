FROM imranq2/spark_python:0.1.25
# https://github.com/imranq2/docker.spark_python
USER root

ENV PYTHONPATH=/sam

COPY Pipfile* /sam/
WORKDIR /sam

RUN df -h # for space monitoring
RUN pipenv sync --dev --system

COPY . /sam

RUN df -h # for space monitoring
RUN mkdir -p /fhir && chmod 777 /fhir
RUN mkdir -p /.local/share/virtualenvs && chmod 777 /.local/share/virtualenvs
