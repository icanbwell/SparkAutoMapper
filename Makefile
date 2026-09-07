LANG=en_US.utf-8

export LANG

AWS_SERVICES_PROFILE ?= services
AWS_SERVICES_REGION ?= us-east-1
AWS_SERVICES_REGISTRY ?= 856965016623.dkr.ecr.us-east-1.amazonaws.com

# Locally, devs authenticate with `aws sso login --profile services`. In CI the
# runner already holds ambient credentials from configure-aws-credentials, and
# passing a non-existent profile there would fail, so drop the flag when CI is set.
ifdef CI
AWS_PROFILE_FLAG :=
else
AWS_PROFILE_FLAG := --profile $(AWS_SERVICES_PROFILE)
endif

.PHONY: ecr-login
ecr-login: ## Logs docker in to the private ECR holding the helix.spark base image
	aws ecr get-login-password --region $(AWS_SERVICES_REGION) $(AWS_PROFILE_FLAG) \
		| docker login --username AWS --password-stdin $(AWS_SERVICES_REGISTRY)

Pipfile.lock: Pipfile
	docker compose run --rm --name spark_auto_mapper dev \
		/bin/bash -lc 'pipenv lock --clear --dev'

.PHONY:devdocker
devdocker: ecr-login ## Builds the docker for dev
	docker compose build --no-cache

.PHONY:init
init: devdocker up setup-pre-commit  ## Initializes the local developer environment

.PHONY: up
up: ecr-login Pipfile.lock
	docker compose up --build -d

.PHONY: down
down: ## Brings down all the services in docker-compose
	export DOCKER_CLIENT_TIMEOUT=300 && export COMPOSE_HTTP_TIMEOUT=300
	docker compose down --remove-orphans && \
	docker system prune -f

.PHONY:clean-pre-commit
clean-pre-commit: ## removes pre-commit hook
	rm -f .git/hooks/pre-commit

.PHONY:setup-pre-commit
setup-pre-commit: Pipfile.lock
	cp ./pre-commit-hook ./.git/hooks/pre-commit

.PHONY:run-pre-commit
run-pre-commit: setup-pre-commit
	./.git/hooks/pre-commit

.PHONY:update
update: Pipfile.lock setup-pre-commit  ## Updates all the packages using Pipfile
	docker compose run --rm --name sam_pipenv dev pipenv sync --dev && \
	make pipenv-setup && \
	make devdocker

.PHONY:tests
tests: up
	docker compose run --rm --name sam_tests dev pytest tests

.PHONY: sphinx-html
sphinx-html:
	docker compose run --rm --name spark_auto_mapper dev make -C docsrc html
	@echo "copy html to docs... why? https://github.com/sphinx-doc/sphinx/issues/3382#issuecomment-470772316"
	@rm -rf docs/*
	@touch docs/.nojekyll
	cp -a docsrc/_build/html/. docs

.PHONY:pipenv-setup
pipenv-setup:devdocker ## Run pipenv-setup to update setup.py with latest dependencies
	docker compose run --rm --name spark_pipeline_framework dev sh -c "pipenv run pipenv install --skip-lock --categories \"pipenvsetup\" && pipenv run pipenv-setup sync --pipfile" && \
	make run-pre-commit


.PHONY:shell
shell:devdocker ## Brings up the bash shell in dev docker
	docker compose run --rm --name sam_shell dev /bin/bash

.PHONY:build
build: ecr-login ## Builds the docker for dev
	docker compose build --progress=plain --parallel
