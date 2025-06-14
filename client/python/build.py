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
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import os.path
import subprocess
from pathlib import Path

# Paths
PROJECT_ROOT = Path(__file__).parent
SPEC_DIR = os.path.join(PROJECT_ROOT, "spec")
POLARIS_MANAGEMENT_SPEC = os.path.join(SPEC_DIR, "polaris-management-service.yml")
ICEBERG_CATALOG_SPEC = os.path.join(SPEC_DIR, "iceberg-rest-catalog-open-api.yaml")
POLARIS_CATALOG_SPEC = os.path.join(SPEC_DIR, "polaris-catalog-service.yaml")
OPEN_API_GENERATOR_IGNORE = os.path.join(PROJECT_ROOT, ".openapi-generator-ignore")

# Open API Generator Configs
PACKAGE_NAME_POLARIS_MANAGEMENT = "--additional-properties=packageName=polaris.management"
PACKAGE_NAME_POLARIS_CATALOG = "--additional-properties=packageName=polaris.catalog"
PYTHON_VERSION = "--additional-properties=pythonVersion=3.9"

def generate_polaris_management_client() -> None:
    subprocess.check_call([
        "openapi-generator-cli", "generate",
        "-i", POLARIS_MANAGEMENT_SPEC,
        "-g", "python",
        "-o", PROJECT_ROOT,
        PACKAGE_NAME_POLARIS_MANAGEMENT,
        "--additional-properties=apiNamePrefix=polaris",
        PYTHON_VERSION,
        "--ignore-file-override", OPEN_API_GENERATOR_IGNORE
    ],
    stdout=subprocess.DEVNULL)

def generate_polaris_catalog_client() -> None:
    subprocess.check_call([
        "openapi-generator-cli", "generate",
        "-i", POLARIS_CATALOG_SPEC,
        "-g", "python",
        "-o", PROJECT_ROOT,
        PACKAGE_NAME_POLARIS_CATALOG,
        "--additional-properties=apiNameSuffix=",
        PYTHON_VERSION,
        "--skip-validate-spec",
        "--ignore-file-override", OPEN_API_GENERATOR_IGNORE
    ],
    stdout=subprocess.DEVNULL)

def generate_iceberg_catalog_client() -> None:
    subprocess.check_call([
        "openapi-generator-cli", "generate",
        "-i", ICEBERG_CATALOG_SPEC,
        "-g", "python",
        "-o", PROJECT_ROOT,
        PACKAGE_NAME_POLARIS_CATALOG,
        "--additional-properties=apiNameSuffix=",
        "--additional-properties=apiNamePrefix=Iceberg",
        PYTHON_VERSION,
        "--skip-validate-spec",
        "--ignore-file-override", OPEN_API_GENERATOR_IGNORE
    ],
    stdout=subprocess.DEVNULL)

def build() -> None:
    generate_polaris_management_client()
    generate_polaris_catalog_client()
    generate_iceberg_catalog_client()

if __name__ == "__main__":
    build()