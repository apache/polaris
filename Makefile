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

# Configures the shell for recipes to use bash, enabling bash commands and ensuring
# that recipes exit on any command failure (including within pipes).
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

## Variables
BUILD_IMAGE ?= true
DOCKER ?= docker
MINIKUBE_PROFILE ?= minikube
DEPENDENCIES ?= ct helm helm-docs java21 git
OPTIONAL_DEPENDENCIES := jq kubectl minikube
VENV_DIR := .venv
PYTHON_CLIENT_DIR := client/python
ACTIVATE_AND_CD = source $(VENV_DIR)/bin/activate && cd $(PYTHON_CLIENT_DIR)

## Version information
BUILD_VERSION := $(shell cat version.txt)
GIT_COMMIT := $(shell git rev-parse HEAD)
POETRY_VERSION := $(shell cat client/python/pyproject.toml | grep requires-poetry | sed 's/requires-poetry *= *"\(.*\)"/\1/')

##@ General

.PHONY: help
help: ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9\.-]+:.*?##/ { printf "  \033[36m%-40s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: version
version: ## Display version information
	@echo "Build version: ${BUILD_VERSION}"
	@echo "Git commit: ${GIT_COMMIT}"
	@echo "Poetry version: ${POETRY_VERSION}"

##@ Polaris Build

.PHONY: build
build: build-server build-admin ## Build Polaris server, admin, and container images

build-server: DEPENDENCIES := java21 $(DOCKER)
.PHONY: build-server
build-server: check-dependencies ## Build Polaris server and container image
	@echo "--- Building Polaris server ---"
	@./gradlew \
		:polaris-server:assemble \
		:polaris-server:quarkusAppPartsBuild --rerun \
		-Dquarkus.container-image.build=$(BUILD_IMAGE) \
		-Dquarkus.docker.executable-name=$(DOCKER)
	@echo "--- Polaris server build complete ---"

build-admin: DEPENDENCIES := java21 $(DOCKER)
.PHONY: build-admin
build-admin: check-dependencies ## Build Polaris admin and container image
	@echo "--- Building Polaris admin ---"
	@./gradlew \
		:polaris-admin:assemble \
		:polaris-admin:quarkusAppPartsBuild --rerun \
		-Dquarkus.container-image.build=$(BUILD_IMAGE) \
		-Dquarkus.docker.executable-name=$(DOCKER)
	@echo "--- Polaris admin build complete ---"

build-spark-plugin-3.5-2.12: DEPENDENCIES := java21
.PHONY: build-spark-plugin-3.5-2.12
build-spark-plugin-3.5-2.12: check-dependencies ## Build Spark plugin v3.5 with Scala v2.12
	@echo "--- Building Spark plugin v3.5 with Scala v2.12 ---"
	@./gradlew \
		:polaris-spark-3.5_2.12:assemble
	@echo "--- Spark plugin v3.5 with Scala v2.12 build complete ---"

build-spark-plugin-3.5-2.13: DEPENDENCIES := java21
.PHONY: build-spark-plugin-3.5-2.13
build-spark-plugin-3.5-2.13: check-dependencies ## Build Spark plugin v3.5 with Scala v2.13
	@echo "--- Building Spark plugin v3.5 with Scala v2.13 ---"
	@./gradlew \
		:polaris-spark-3.5_2.13:assemble
	@echo "--- Spark plugin v3.5 with Scala v2.13 build complete ---"

build-cleanup: DEPENDENCIES := java21
.PHONY: build-cleanup
build-cleanup: check-dependencies ## Clean build artifacts
	@echo "--- Cleaning up build artifacts ---"
	@./gradlew clean
	@echo "--- Build artifacts cleaned ---"

spotless-apply: DEPENDENCIES := java21
.PHONY: spotless-apply
spotless-apply: check-dependencies ## Apply code formatting using Spotless Gradle plugin.
	@echo "--- Applying Spotless formatting ---"
	@./gradlew spotlessApply
	@echo "--- Spotless formatting applied ---"

##@ Polaris Client

# Target to create the virtual environment directory
$(VENV_DIR):
	@echo "Setting up Python virtual environment at $(VENV_DIR)..."
	@python3 -m venv $(VENV_DIR)
	@echo "Virtual environment created."

.PHONY: client-install-dependencies
client-install-dependencies: $(VENV_DIR)
	@echo "Installing Poetry and project dependencies into $(VENV_DIR)..."
	@$(VENV_DIR)/bin/pip install --upgrade pip
	@if [ ! -f "$(VENV_DIR)/bin/poetry" ]; then \
		$(VENV_DIR)/bin/pip install --upgrade "poetry$(POETRY_VERSION)"; \
	fi
	@$(ACTIVATE_AND_CD) && poetry install --all-extras
	@echo "Poetry and dependencies installed."

.PHONY: client-setup-env
client-setup-env: $(VENV_DIR) client-install-dependencies

.PHONY: client-lint
client-lint: client-setup-env ## Run linting checks for Polaris client
	@echo "--- Running client linting checks ---"
	@$(ACTIVATE_AND_CD) && poetry run pre-commit run --files integration_tests/* python/cli/*
	@echo "--- Client linting checks complete ---"

.PHONY: client-regenerate
client-regenerate: client-setup-env ## Regenerate the client code
	@echo "--- Regenerating client code ---"
	@client/templates/regenerate.sh
	@echo "--- Client code regeneration complete ---"

.PHONY: client-unit-test
client-unit-test: client-setup-env ## Run client unit tests
	@echo "--- Running client unit tests ---"
	@$(ACTIVATE_AND_CD) && SCRIPT_DIR="non-existing-mock-directory" poetry run pytest test/
	@echo "--- Client unit tests complete ---"

.PHONY: client-integration-test
client-integration-test: client-setup-env ## Run client integration tests
	@echo "--- Starting client integration tests ---"
	@echo "Ensuring Docker Compose services are stopped and removed..."
	@$(CONTAINER_TOOL) compose -f $(PYTHON_CLIENT_DIR)/docker-compose.yml kill || true # `|| true` prevents make from failing if containers don't exist
	@$(CONTAINER_TOOL) compose -f $(PYTHON_CLIENT_DIR)/docker-compose.yml rm -f || true # `|| true` prevents make from failing if containers don't exist
	@echo "Bringing up Docker Compose services in detached mode..."
	@$(CONTAINER_TOOL) compose -f $(PYTHON_CLIENT_DIR)/docker-compose.yml up -d
	@echo "Waiting for Polaris HTTP health check to pass..."
	@until curl -s -f http://localhost:8182/q/health > /dev/null; do \
		echo "Still waiting for HTTP 200 from /q/health (sleeping 2s)..."; \
		sleep 2; \
	done
	@echo "Polaris is healthy. Starting integration tests..."
	@$(ACTIVATE_AND_CD) && poetry run pytest integration_tests/
	@echo "--- Client integration tests complete ---"
	@echo "Tearing down Docker Compose services..."
	@$(CONTAINER_TOOL) compose -f $(PYTHON_CLIENT_DIR)/docker-compose.yml down || true # Ensure teardown even if tests fail

.PHONY: client-cleanup
client-cleanup: ## Cleanup virtual environment and Python cache files
	@echo "--- Cleaning up virtual environment and Python cache files ---"
	@echo "Attempting to remove virtual environment directory: $(VENV_DIR)..."
	@if [ -n "$(VENV_DIR)" ] && [ -d "$(VENV_DIR)" ]; then \
		rm -rf "$(VENV_DIR)"; \
		echo "Virtual environment removed."; \
	else \
		echo "Virtual environment directory '$(VENV_DIR)' not found or VENV_DIR is empty. No action taken."; \
	fi
	@echo "Cleaning up Python cache files..."
	@find $(PYTHON_CLIENT_DIR) -type f -name "*.pyc" -delete
	@find $(PYTHON_CLIENT_DIR) -type d -name "__pycache__" -delete
	@echo "--- Virtual environment and Python cache cleanup complete ---"

##@ Helm

helm-doc-generate: DEPENDENCIES := helm-docs
.PHONY: helm-doc-generate
helm-doc-generate: check-dependencies ## Generate Helm chart documentation
	@echo "--- Generating Helm documentation ---"
	@helm-docs --chart-search-root=helm
	@cp helm/polaris/README.md site/content/in-dev/unreleased/helm.md
	@echo "--- Helm documentation generated and copied ---"

helm-unittest: DEPENDENCIES := helm
.PHONY: helm-unittest
helm-unittest: check-dependencies ## Run Helm chart unittest
	@echo "--- Running Helm chart unittest ---"
	@helm unittest helm/polaris
	@echo "--- Helm chart unittest complete ---"

helm-lint: DEPENDENCIES := ct
.PHONY: helm-lint
helm-lint: check-dependencies ## Run Helm chart lint check
	@echo "--- Running Helm chart linting ---"
	@ct lint --charts helm/polaris
	@echo "--- Helm chart linting complete ---"

##@ Minikube

minikube-start-cluster: DEPENDENCIES := minikube $(DOCKER)
.PHONY: minikube-start-cluster
minikube-start-cluster: check-dependencies ## Start the Minikube cluster
	@echo "--- Checking Minikube cluster status ---"
	@if minikube status -p $(MINIKUBE_PROFILE) --format "{{.Host}}" | grep -q "Running"; then \
		echo "--- Minikube cluster is already running. Skipping start ---"; \
	else \
		echo "--- Starting Minikube cluster ---"; \
		if [ "$(DOCKER)" = "podman" ]; then \
			minikube start -p $(MINIKUBE_PROFILE) --driver=$(DOCKER) --container-runtime=cri-o; \
		else \
			minikube start -p $(MINIKUBE_PROFILE) --driver=$(DOCKER); \
		fi; \
		echo "--- Minikube cluster started ---"; \
	fi

minikube-stop-cluster: DEPENDENCIES := minikube $(DOCKER)
.PHONY: minikube-stop-cluster
minikube-stop-cluster: check-dependencies ## Stop the Minikube cluster
	@echo "--- Checking Minikube cluster status ---"
	@if minikube status -p $(MINIKUBE_PROFILE) --format "{{.Host}}" | grep -q "Running"; then \
		echo "--- Stopping Minikube cluster ---"; \
		minikube stop -p $(MINIKUBE_PROFILE); \
		echo "--- Minikube cluster stopped ---"; \
	else \
		echo "--- Minikube cluster is already stopped or does not exist. Skipping stop ---"; \
	fi

minikube-load-images: DEPENDENCIES := minikube $(DOCKER)
.PHONY: minikube-load-images
minikube-load-images: minikube-start-cluster check-dependencies ## Load local Docker images into the Minikube cluster
	@echo "--- Loading images into Minikube cluster ---"
	@minikube image load -p $(MINIKUBE_PROFILE) docker.io/apache/polaris:latest
	@minikube image tag -p $(MINIKUBE_PROFILE) docker.io/apache/polaris:latest docker.io/apache/polaris:$(BUILD_VERSION)
	@minikube image load -p $(MINIKUBE_PROFILE) docker.io/apache/polaris-admin-tool:latest
	@minikube image tag -p $(MINIKUBE_PROFILE) docker.io/apache/polaris-admin-tool:latest docker.io/apache/polaris-admin-tool:$(BUILD_VERSION)
	@echo "--- Images loaded into Minikube cluster ---"

minikube-cleanup: DEPENDENCIES := minikube $(DOCKER)
.PHONY: minikube-cleanup
minikube-cleanup: check-dependencies ## Cleanup the Minikube cluster
	@echo "--- Checking Minikube cluster status ---"
	@if minikube status -p $(MINIKUBE_PROFILE) >/dev/null 2>&1; then \
		echo "--- Cleanup Minikube cluster ---"; \
		minikube delete -p $(MINIKUBE_PROFILE); \
		echo "--- Minikube cluster removed ---"; \
	else \
		echo "--- Minikube cluster does not exist. Skipping cleanup ---"; \
	fi

##@ Pre-commit

.PHONY: pre-commit
pre-commit: spotless-apply helm-doc-generate client-lint ## Run tasks for pre-commit

##@ Dependencies

.PHONY: check-dependencies
check-dependencies: ## Check if all requested dependencies are present
	@echo "--- Checking for requested dependencies ---"
	@for dependency in $(DEPENDENCIES); do \
		echo "Checking for $$dependency..."; \
		if [ "$$dependency" = "java21" ]; then \
			if java --version | head -n1 | cut -d' ' -f2 | grep -q '^21\.'; then \
			echo "Java 21 is installed."; \
			else \
				echo "Java 21 is NOT installed."; \
				echo "--- ERROR: Dependency 'Java 21' is missing. Please install it to proceed. Exiting. ---"; \
				exit 1; \
			fi ; \
		elif command -v $$dependency >/dev/null 2>&1; then \
			echo "$$dependency is installed."; \
		else \
			echo "$$dependency is NOT installed."; \
			echo "--- ERROR: Dependency '$$dependency' is missing. Please install it to proceed. Exiting. ---"; \
			exit 1; \
		fi; \
	done
	@echo "--- All checks complete. ---"

.PHONY: check-brew
check-brew:
	@echo "--- Checking Homebrew installation ---"
	@if command -v brew >/dev/null 2>&1; then \
		echo "--- Homebrew is installed ---"; \
	else \
		echo "--- Homebrew is not installed. Aborting ---"; \
		exit 1; \
	fi

.PHONY: install-dependencies-brew
install-dependencies-brew: check-brew ## Install dependencies if not present via Brew
	@echo "--- Checking and installing dependencies for this target ---"
	@for dependency in $(DEPENDENCIES); do \
		case $$dependency in \
			java21) \
				if java -version 2>&1 | grep -q '21'; then \
					:; \
				else \
					echo "Java 21 is not installed. Installing openjdk@21 and jenv..."; \
					brew install openjdk@21 jenv; \
					$(shell brew --prefix jenv)/bin/jenv add $(shell brew --prefix openjdk@21); \
					jenv local 21; \
					echo "Java 21 installed."; \
				fi ;; \
			docker|podman) \
				if command -v $$dependency >/dev/null 2>&1; then \
					:; \
				else \
					echo "$$dependency is not installed. Manual installation required"; \
				fi ;; \
			ct) \
				if command -v ct >/dev/null 2>&1; then \
					:; \
				else \
					echo "ct is not installed. Installing with Homebrew..."; \
					brew install chart-testing; \
					echo "ct installed."; \
				fi ;; \
			*) \
				if command -v $$dependency >/dev/null 2>&1; then \
					:; \
				else \
					echo "$$dependency is not installed. Installing with Homebrew..."; \
					brew install $$dependency; \
					echo "$$dependency installed."; \
				fi ;; \
		esac; \
	done
	@echo "--- All requested dependencies checked/installed ---"

install-optional-dependencies-brew: DEPENDENCIES := $(OPTIONAL_DEPENDENCIES)
.PHONY: install-optional-dependencies-brew
install-optional-dependencies-brew: install-dependencies-brew ## Install optional dependencies if not present via Brew
