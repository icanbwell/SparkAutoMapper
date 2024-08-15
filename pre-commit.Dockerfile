FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y git && \
    pip install pipenv

COPY ${project_root}/Pipfile* ./

RUN pipenv lock --dev && \
    pipenv sync --dev --system --verbose

WORKDIR /sourcecode
RUN git config --global --add safe.directory /sourcecode
CMD pre-commit run --all-files
