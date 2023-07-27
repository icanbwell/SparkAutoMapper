FROM imranq2/helix.spark:3.3.0.29-slim
# https://github.com/icanbwell/helix.spark
USER root

ENV PYTHONPATH=/sam
ENV CLASSPATH=/sam/jars:$CLASSPATH

COPY Pipfile* /sam/
WORKDIR /sam

RUN df -h # for space monitoring
RUN pipenv sync --dev --system

# COPY ./jars/* /opt/bitnami/spark/jars/
# COPY ./conf/* /opt/bitnami/spark/conf/

# override entrypoint to remove extra logging
RUN mv /opt/minimal_entrypoint.sh /opt/entrypoint.sh

COPY . /sam

# run pre-commit once so it installs all the hooks and subsequent runs are fast
# RUN pre-commit install
RUN df -h # for space monitoring
RUN mkdir -p /fhir && chmod 777 /fhir
RUN mkdir -p /.local/share/virtualenvs && chmod 777 /.local/share/virtualenvs
# USER 1001
