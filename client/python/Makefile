#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

# .SILENT:

# Configures the shell for recipes to use bash, enabling bash commands and ensuring
# that recipes exit on any command failure (including within pipes).
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

# Version information
VERSION ?= $(shell cat pyproject.toml | grep version | sed 's/version *= *"\(.*\)"/\1/')
BUILD_DATE := $(shell date -u +"%Y-%m-%dT%H:%M:%S%:z")
GIT_COMMIT := $(shell git rev-parse HEAD)
POETRY_VERSION := $(shell cat pyproject.toml | grep requires-poetry | sed 's/requires-poetry *= *"\(.*\)"/\1/')

# Variables
VENV_DIR := .venv

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-30s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: version
version: ## Print version information.
	@echo "Apache Polaris version: ${VERSION}"
	@echo "Build date: ${BUILD_DATE}"
	@echo "Git commit: ${GIT_COMMIT}"
	@echo "Poetry version: ${POETRY_VERSION}"

# Target to create the virtual environment directory
$(VENV_DIR):
	@echo "Setting up Python virtual environment at $(VENV_DIR)..."
	python3 -m venv $(VENV_DIR)
	@echo "Virtual environment created."

.PHONY: setup-env
setup-env: $(VENV_DIR) install-poetry-deps

.PHONY: install-poetry-deps
install-poetry-deps:
	@echo "Installing Poetry and project dependencies into $(VENV_DIR)..."
	# Ensure pip is up-to-date within the venv
	$(VENV_DIR)/bin/pip install --upgrade pip
	# Install poetry if not already present
	@if [ ! -f "$(VENV_DIR)/bin/poetry" ]; then \
		$(VENV_DIR)/bin/pip install --upgrade "poetry${POETRY_VERSION}"; \
	fi
	# Install needed dependencies using poetry
	$(VENV_DIR)/bin/poetry install --all-extras
	@echo "Poetry and dependencies installed."

.PHONY: regenerate-client
regenerate-client: ## Regenerate the client code
	../templates/regenerate.sh

.PHONY: test-client
test-client: setup-env ## Run client tests
	SCRIPT_DIR="non-existing-mock-directory" $(VENV_DIR)/bin/poetry run pytest test/

.PHONY: test-integration
test-integration: setup-env ## Run integration tests
	docker compose -f docker-compose.yml kill
	docker compose -f docker-compose.yml rm -f
	docker compose -f docker-compose.yml up -d
	@echo "Waiting for Polaris HTTP health check to pass..."
	@until curl -s -f http://localhost:8182/q/health > /dev/null; do \
		sleep 2; \
		echo "Still waiting for HTTP 200 from /q/health..."; \
	done
	@echo "Polaris is healthy. Starting integration tests..."
	$(VENV_DIR)/bin/poetry run pytest integration_tests/ ${PYTEST_ARGS}

.PHONY: lint
lint: setup-env ## Run linting checks
	$(VENV_DIR)/bin/poetry run pre-commit run --files integration_tests/* cli/*

.PHONY: clean-venv
clean-venv:
	@echo "Attempting to remove virtual environment directory: $(VENV_DIR)..."
	# SAFETY CHECK: Ensure VENV_DIR is not empty and exists before attempting to remove
	@if [ -n "$(VENV_DIR)" ] && [ -d "$(VENV_DIR)" ]; then \
		rm -rf "$(VENV_DIR)"; \
		echo "Virtual environment removed."; \
	else \
		echo "Virtual environment directory '$(VENV_DIR)' not found or VENV_DIR is empty. No action taken."; \
	fi

.PHONY: clean
clean: clean-venv ## Cleanup
	@echo "Cleaning up Python cache files..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
