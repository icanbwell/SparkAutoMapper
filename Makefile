LANG=en_US.utf-8

export LANG

Pipfile.lock: Pipfile
	docker compose run --rm --name spark_auto_mapper dev sh -c "rm -f Pipfile.lock && pipenv lock --dev"

.PHONY:devdocker
devdocker: ## Builds the docker for dev
	docker compose build --no-cache

.PHONY:init
init: devdocker up setup-pre-commit  ## Initializes the local developer environment

.PHONY: up
up: Pipfile.lock
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
build: ## Builds the docker for dev
	docker compose build --progress=plain --parallel
