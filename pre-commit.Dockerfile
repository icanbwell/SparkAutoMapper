FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y git && \
    pip install pipenv

COPY ${project_root}/Pipfile* ./

RUN pipenv sync --system --dev --verbose

WORKDIR /sourcecode
# --system (not --global) so the setting applies to every user in the image,
# including the non-root user below and any UID injected at runtime via
# `docker run --user`, none of which read root's ~/.gitconfig.
RUN git config --system --add safe.directory /sourcecode

# Don't run as root (Aikido CKV_DOCKER_3, issue 3775915). pre-commit only needs
# to read the source tree and write formatter output back to the bind-mounted
# /sourcecode, which is owned by the host user -- so pre-commit-hook overrides
# this UID at runtime with `--user "$(id -u):$(id -g)" -e HOME=/tmp`. Without
# that override the bind mount is not writable and `pre-commit run --all-files`
# fails as soon as black/autoflake rewrite a file.
RUN useradd --create-home --uid 1000 precommit
USER precommit

CMD ["pre-commit", "run", "--all-files"]
