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
IMAGE_TAG ?= postgres-latest
BINARIES := git docker helm-docs jq java21 ct helm

##@ General

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-30s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: check-brew
check-brew:
	@echo "--- Checking Homebrew installation ---"
	@if command -v brew >/dev/null 2>&1; then \
		echo "--- Homebrew is installed ---"; \
	else \
		echo "--- Homebrew is not installed. Aborting ---"; \
		exit 1; \
	fi

##@ Polaris Build

.PHONY: build
build: build-server build-admin ## Build Polaris server, admin, and container images

.PHONY: build-server
build-server: setup-binaries ## Build Polaris server and container image
	@echo "--- Building Polaris server ---"
	@./gradlew \
		:polaris-server:assemble \
		:polaris-server:quarkusAppPartsBuild --rerun \
		-Dquarkus.container-image.tag=$(IMAGE_TAG) \
		-Dquarkus.container-image.build=$(BUILD_IMAGE)
	@echo "--- Polaris server build complete ---"

.PHONY: build-admin
build-admin: setup-binaries ## Build Polaris admin and container image
	@echo "--- Building Polaris admin ---"
	@./gradlew \
		:polaris-admin:assemble \
		:polaris-admin:quarkusAppPartsBuild --rerun \
		-Dquarkus.container-image.tag=$(IMAGE_TAG) \
		-Dquarkus.container-image.build=$(BUILD_IMAGE)
	@echo "--- Polaris admin build complete ---"

.PHONY: build-cleanup
build-cleanup: setup-binaries ## Clean build artifacts
	@echo "--- Cleaning up build artifacts ---"
	@./gradlew clean
	@echo "--- Build artifacts cleaned ---"

.PHONY: spotless-apply
spotless-apply: setup-binaries ## Apply code formatting using Spotless Gradle plugin.
	@echo "--- Applying Spotless formatting ---"
	@./gradlew spotlessApply
	@echo "--- Spotless formatting applied ---"

##@ Helm

.PHONY: helm-doc-generate
helm-doc-generate: setup-binaries ## Generate Helm chart documentation
	@echo "--- Generating Helm documentation ---"
	@helm-docs --chart-search-root=helm
	@cp helm/polaris/README.md site/content/in-dev/unreleased/helm.md
	@echo "--- Helm documentation generated and copied ---"

.PHONY: helm-unittest
helm-unittest: setup-binaries ## Run Helm chart unittest
	@echo "--- Running Helm chart unittest ---"
	@helm unittest helm/polaris
	@echo "--- Helm chart unittest complete ---"

.PHONY: helm-lint
helm-lint: setup-binaries ## Run Helm chart lint check
	@echo "--- Running Helm chart linting ---"
	@ct lint --charts helm/polaris
	@echo "--- Helm chart linting complete ---"

##@ Pre-commit

.PHONY: pre-commit
pre-commit: spotless-apply helm-doc-generate ## Run tasks for pre-commit

##@ Dependencies

.PHONY: setup-binaries
setup-binaries: check-brew ## Install required binaries if not present
	@echo "--- Checking and installing required binaries ---"
	@for bin in $(BINARIES); do \
		case $$bin in \
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
			docker) \
				if command -v docker >/dev/null 2>&1; then \
					:; \
				else \
					echo "docker is not installed. Installing with Homebrew..."; \
					brew install --cask docker; \
					echo "docker installed."; \
				fi ;; \
			ct) \
				if command -v ct >/dev/null 2>&1; then \
					:; \
				else \
					echo "ct is not installed. Installing with Homebrew..."; \
					brew install --cask chart-testing; \
					echo "ct installed."; \
				fi ;; \
			*) \
				if command -v $$bin >/dev/null 2>&1; then \
					:; \
				else \
					echo "$$bin is not installed. Installing with Homebrew..."; \
					brew install $$bin; \
					echo "$$bin installed."; \
				fi ;; \
		esac; \
	done
	@echo "--- All required binaries checked/installed ---"

