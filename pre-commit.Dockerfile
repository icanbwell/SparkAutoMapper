FROM python:3.7-slim

RUN apt-get update && \
    apt-get install -y git && \
    apt-get install build-essential -y && \
    apt-get install -y curl && \
    apt-get install -y sudo && \
    pip install pipenv

ENV PATH="/home/linuxbrew/.linuxbrew/bin:${PATH}"

RUN /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)" && \
    brew install gcc && \
    brew install watchman && \
    pip install pyre-check

COPY ${project_root}/Pipfile* ./
RUN pipenv sync --dev --system

WORKDIR /sourcecode

CMD pre-commit run --all-files
