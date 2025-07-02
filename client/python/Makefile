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

VENV_DIR := .venv

.PHONY: regenerate-client test-integration lint setup-env install-poetry-deps clean clean-venv

# Target to create the virtual environment directory
$(VENV_DIR):
	@echo "Setting up Python virtual environment at $(VENV_DIR)..."
	python3 -m venv $(VENV_DIR)
	@echo "Virtual environment created."

# Target to ensure the virtual environment and poetry/dependencies are set up
setup-env: $(VENV_DIR) install-poetry-deps

# Target to install poetry and project dependencies
install-poetry-deps: # This target doesn't *create* $(VENV_DIR), but *uses* it.
	@echo "Installing Poetry and project dependencies into $(VENV_DIR)..."
	# Ensure pip is up-to-date within the venv
	$(VENV_DIR)/bin/pip install --upgrade pip
	# Install poetry if not already present
	@if [ ! -f "$(VENV_DIR)/bin/poetry" ]; then \
		$(VENV_DIR)/bin/pip install poetry; \
	fi
	# Install needed dependencies using poetry
	$(VENV_DIR)/bin/poetry install --all-extras
	@echo "Poetry and dependencies installed."

# Target to regenerate the client code
regenerate-client:
	../templates/regenerate.sh

# Target to run integration tests
test-integration: setup-env
	docker compose -f docker-compose.yml kill
	docker compose -f docker-compose.yml rm -f
	docker compose -f docker-compose.yml up -d
	@echo "Waiting for Polaris HTTP health check to pass..."
	@until curl -s -f http://localhost:8182/q/health > /dev/null; do \
		sleep 2; \
		echo "Still waiting for HTTP 200 from /q/health..."; \
	done
	@echo "Polaris is healthy. Starting integration tests..."
	poetry run pytest integration_tests/ ${PYTEST_ARGS}

# Target to run linting checks
lint: setup-env
	$(VENV_DIR)/bin/poetry run pre-commit run --files integration_tests/* cli/*

# Clean targets
clean: clean-venv
	@echo "Cleaning up Python cache files..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete

clean-venv:
	@echo "Attempting to remove virtual environment directory: $(VENV_DIR)..."
	# SAFETY CHECK: Ensure VENV_DIR is not empty and exists before attempting to remove
	@if [ -n "$(VENV_DIR)" ] && [ -d "$(VENV_DIR)" ]; then \
		rm -rf "$(VENV_DIR)"; \
		echo "Virtual environment removed."; \
	else \
		echo "Virtual environment directory '$(VENV_DIR)' not found or VENV_DIR is empty. No action taken."; \
	fi
