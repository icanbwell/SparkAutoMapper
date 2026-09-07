FROM python:3.12-slim
# NOTE: unlike spark.Dockerfile this image is NOT a helix.spark image, so the
# CIE-8032 private-ECR migration does not apply to it. It is deliberately left
# on Docker Hub's python:3.12-slim.
#
# CUSTOM-RULE-2300 ("not sourced from root.io ECR mirror") therefore stays OPEN
# on this line and needs an Aikido exception -- it is not fixable here.
# The only FROM that clears 2300 would be:
#   FROM 856965016623.dkr.ecr.us-east-1.amazonaws.com/root-mirror/python:3.12-slim
# Verified by aikido_full_scan on 2026-09-08: that line clears both
# CUSTOM-RULE-559 and CUSTOM-RULE-2300 (2576 still fires). It is NOT used
# because an org-wide search found zero working root-mirror/*-slim consumers
# (every root-mirror user in the org is on -alpine), so that tag likely does not
# exist, and there are no AWS credentials available to confirm it. Switching
# would trade a proven base image for an unverifiable one.
# Mirroring can be requested via icanbwell/aikido-image-sync if it is ever wanted.

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
