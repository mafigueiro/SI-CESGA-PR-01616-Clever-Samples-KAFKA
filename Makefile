BASE_PROJECT_NAME := clever
PROJECT_NAME := samples_etl
TAG := dev
DOCKER_REGISTRY := harbor.gradiant.org
DOCKER_IMAGE_TAG := $(DOCKER_REGISTRY)/$(BASE_PROJECT_NAME)/$(PROJECT_NAME):$(TAG)
DOCKER_CONTAINER_NAME := $(BASE_PROJECT_NAME)-$(PROJECT_NAME)
CONDA_ENV_NAME := $(shell grep "name: " ./tools/conda-env.yml | awk  '{print $$2}')
APP_CONFIG_PATH := /home/mafigueiro/IdeaProjects/Clever/SI-CESGA-PR-01616-Clever-Samples-KAFKA/src/config/config.yml
PIP_REQUIREMENTS := ./requirements.txt
PIP_TEST_REQUIREMENTS := ./tests/requirements.txt

COMPOSE_VERSION := $(shell docker-compose --version 2>/dev/null)
ifeq (,$(shell which docker-compose))
	COMPOSE=docker compose
else
	COMPOSE=docker-compose
endif

uv-init: ## initialize uv project
	uv init

uv-sync: ## Sync uv environment with requirements
	uv sync

pip-install-requirements: ## pip install requirements for app
	uv add -r $(PIP_REQUIREMENTS)

pip-install-test-requirements: ## pip install requirements for tests
	uv add -r $(PIP_TEST_REQUIREMENTS)

pip-install-all: ## pip install requirements for app and tests
	uv add -r $(PIP_REQUIREMENTS) -r $(PIP_TEST_REQUIREMENTS)

test: ## run tests
	uv run pytest -sv .

setup-git-hooks: ## copy git hooks to .git/hooks directory (overwrite if exist)
	\cp tools/git-hooks/* .git/hooks/

deploy-rabbitmq-compose:
	cd tools/rabbitmq/ && $(COMPOSE) up -d

teardown-rabbitmq-compose:
	cd tools/rabbitmq/ && $(COMPOSE) down

run:  ## Run on host
	APP_CONFIG_PATH=$(APP_CONFIG_PATH) uv run python -m src

run-producer:
	APP_CONFIG_PATH=$(APP_CONFIG_PATH) \
	uv run python tools/producer/producer_csv_to_json.py tools/producer/datos.csv 999999999

# Si llamas con número -> solo esas filas
run-producer-%:
	APP_CONFIG_PATH=$(APP_CONFIG_PATH) \
	uv run python tools/producer/producer_csv_to_json.py tools/producer/datos.csv $*


black-format-code:
	uv run black . -t py312 --line-length=80 --exclude='(.*env.*)'

ruff:
	uv run ruff check .

check-code-vulnerabilities-bandit:
	uv run bandit -r ./src

start-database-compose:
	cd tools/ && $(COMPOSE) up -d

stop-database-compose:
	cd tools/ && $(COMPOSE) down -v

deploy-rabbit-and-start-database-compose:
	cd tools/ && $(COMPOSE) up -d
	cd tools/rabbitmq/ && $(COMPOSE) up -d

teardown-rabbit-and-stop-database-compose:
	cd tools/ && $(COMPOSE) down -v
	cd tools/rabbitmq/ && $(COMPOSE) down -v

docker-build: ## run docker build to create docker image
	docker build . -t $(DOCKER_IMAGE_TAG)

docker-build-and-push-to-registry: ## run docker build to create docker image and push to registry
	docker build . -t $(DOCKER_IMAGE_TAG); \
        docker push $(DOCKER_IMAGE_TAG)

docker-run-dev: ## run docker image in dev mode (with network=host and using the local .env)
	docker run --rm --net=host --env-file=.env --name $(DOCKER_CONTAINER_NAME) -t $(DOCKER_IMAGE_TAG)

docker-run-test: ## run tests on docker image in dev mode (with network=host and using the local .env)
	docker run --rm --net=host --env-file=.env --name $(DOCKER_CONTAINER_NAME) -t $(DOCKER_IMAGE_TAG) \
		python -m pytest -sv .

docker-clean: ## remove docker image
	docker rmi $(DOCKER_IMAGE_TAG) || exit 0


docker-build-with-pyinstaller: ## run docker build to create docker image using pyinstaller to get a lighter image
	echo "Remember to add hidden imports to ./tools/scripts/minimal_deployment/make_all_in_one_binary.sh if ModuleNotFoundError errors appear when running the application."
	docker build -f ./tools/dockerfiles/minimal_deployment.Dockerfile -t $(DOCKER_IMAGE_TAG) .

docker-build-with-pyinstaller-cython: ## run docker build to create docker image using pyinstaller to get a lighter and faster image
	echo "###################################################################################################################"
	echo "### The usage of Cython here is experimental, as we are using a prerelease of Cython compatible with fastapi."
	echo "### Read https://github.com/tiangolo/fastapi/issues/1921 for more info."
	echo "###################################################################################################################"
	echo "Remember to complete setup.py with the archives you need to cythonize"
	echo "Remember to add hidden imports to ./tools/scripts/minimal_deployment/make_all_in_one_binary.sh if ModuleNotFoundError errors appear when running the application."
	docker build -f ./tools/dockerfiles/minimal_deployment.Dockerfile --build-arg USE_CYTHON=use_cython -t $(DOCKER_IMAGE_TAG) .


help:  ## This help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
