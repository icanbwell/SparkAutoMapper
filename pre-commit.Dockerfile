FROM python:3.7.10-slim-buster

RUN apt-get update && \
    apt-get install -y git && \
    pip install pipenv

COPY ${project_root}/Pipfile* ./
RUN pipenv sync --dev --system --verbose

WORKDIR /sourcecode
CMD pre-commit run --all-files --verbose
