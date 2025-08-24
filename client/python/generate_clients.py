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

import sys
import os.path
import subprocess
from pathlib import Path
import fnmatch
import logging
import argparse

# Paths
CLIENT_DIR = Path(__file__).parent
PROJECT_ROOT = CLIENT_DIR.parent.parent
HEADER_DIR = CLIENT_DIR.parent / "templates"
SPEC_DIR = os.path.join(PROJECT_ROOT, "spec")
POLARIS_MANAGEMENT_SPEC = os.path.join(SPEC_DIR, "polaris-management-service.yml")
ICEBERG_CATALOG_SPEC = os.path.join(SPEC_DIR, "iceberg-rest-catalog-open-api.yaml")
POLARIS_CATALOG_SPEC = os.path.join(SPEC_DIR, "polaris-catalog-service.yaml")
OPEN_API_GENERATOR_IGNORE = os.path.join(CLIENT_DIR, ".openapi-generator-ignore")

# Open API Generator Configs
PACKAGE_NAME_POLARIS_MANAGEMENT = (
    "--additional-properties=packageName=polaris.management"
)
PACKAGE_NAME_POLARIS_CATALOG = "--additional-properties=packageName=polaris.catalog"
PYTHON_VERSION = "--additional-properties=pythonVersion=3.9"

# Cleanup
KEEP_TEST_FILES = [
    Path("test/test_cli_parsing.py"),
]
EXCLUDE_PATHS = [
    Path(".gitignore"),
    Path(".openapi-generator/"),
    Path(".openapi-generator-ignore"),
    Path(".pytest_cache/"),
    Path("test/test_cli_parsing.py"),
    Path("cli/"),
    Path("polaris/__pycache__/"),
    Path("polaris/catalog/__pycache__/"),
    Path("polaris/catalog/models/__pycache__/"),
    Path("polaris/catalog/api/__pycache__/"),
    Path("polaris/management/__pycache__/"),
    Path("polaris/management/models/__pycache__/"),
    Path("polaris/management/api/__pycache__/"),    
    Path("integration_tests/"),
    Path(".github/workflows/python.yml"),
    Path(".gitlab-ci.yml"),
    Path("pyproject.toml"),
    Path("requirements.txt"),
    Path("test-requirements.txt"),
    Path("setup.py"),
    Path(".DS_Store"),
    Path("Makefile"),
    Path("poetry.lock"),
    Path("docker-compose.yml"),
    Path(".pre-commit-config.yaml"),
    Path("README.md"),
    Path("generate_clients.py"),
    Path(".venv"),
]
EXCLUDE_EXTENSIONS = [
    "json",
    "iml",
    "keep",
    "gitignore",
]

logger = logging.getLogger(__name__)


def clean_old_tests() -> None:
    logger.info("Deleting old tests...")
    test_dir = CLIENT_DIR / "test"
    if not test_dir.exists():
        logger.info(f"Test directory {test_dir} does not exist, skipping test cleanup.")
        return

    for item in test_dir.rglob("*"):
        if item.is_file():
            # Check if the file should be kept relative to CLIENT_DIR
            relative_path = item.relative_to(CLIENT_DIR)
            if relative_path not in KEEP_TEST_FILES:
                try:
                    os.remove(item)
                    logger.debug(f"{relative_path}: removed")
                except OSError as e:
                    logger.error(f"Error removing {relative_path}: {e}")
            else:
                logger.debug(f"{relative_path}: skipped")

    init_py_to_delete = CLIENT_DIR / "test" / "__init__.py"
    if init_py_to_delete.exists():
        try:
            os.remove(init_py_to_delete)
            logger.debug(f"{init_py_to_delete.relative_to(CLIENT_DIR)}: removed")
        except OSError as e:
            logger.error(f"Error removing {init_py_to_delete.relative_to(CLIENT_DIR)}: {e}")
    logger.info("Old test deletion complete.")


def generate_polaris_management_client() -> None:
    subprocess.check_call(
        [
            "openapi-generator-cli",
            "generate",
            "-i",
            POLARIS_MANAGEMENT_SPEC,
            "-g",
            "python",
            "-o",
            CLIENT_DIR,
            PACKAGE_NAME_POLARIS_MANAGEMENT,
            "--additional-properties=apiNamePrefix=polaris",
            PYTHON_VERSION,
            "--additional-properties=generateSourceCodeOnly=true",
            "--skip-validate-spec",
            "--ignore-file-override",
            OPEN_API_GENERATOR_IGNORE,
            "--global-property=apiDocs=false,modelDocs=false,modelTests=false,apiTests=false",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def generate_polaris_catalog_client() -> None:
    subprocess.check_call(
        [
            "openapi-generator-cli",
            "generate",
            "-i",
            POLARIS_CATALOG_SPEC,
            "-g",
            "python",
            "-o",
            CLIENT_DIR,
            PACKAGE_NAME_POLARIS_CATALOG,
            "--additional-properties=apiNameSuffix=",
            PYTHON_VERSION,
            "--additional-properties=generateSourceCodeOnly=true",
            "--skip-validate-spec",
            "--ignore-file-override",
            OPEN_API_GENERATOR_IGNORE,
            "--global-property=apiDocs=false,modelDocs=false,modelTests=false,apiTests=false",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def generate_iceberg_catalog_client() -> None:
    subprocess.check_call(
        [
            "openapi-generator-cli",
            "generate",
            "-i",
            ICEBERG_CATALOG_SPEC,
            "-g",
            "python",
            "-o",
            CLIENT_DIR,
            PACKAGE_NAME_POLARIS_CATALOG,
            "--additional-properties=apiNameSuffix=",
            "--additional-properties=apiNamePrefix=Iceberg",
            PYTHON_VERSION,
            "--additional-properties=generateSourceCodeOnly=true",
            "--skip-validate-spec",
            "--ignore-file-override",
            OPEN_API_GENERATOR_IGNORE,
            "--global-property=apiDocs=false,modelDocs=false,modelTests=false,apiTests=false",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _prepend_header_to_file(file_path: Path, header_file_path: Path) -> None:
    try:
        with open(header_file_path, "r") as hf:
            header_content = hf.read()
        with open(file_path, "r+") as f:
            original_content = f.read()
            f.seek(0)
            f.write(header_content + original_content)
    except IOError as e:
        logger.error(f"Error prepending header to {file_path}: {e}")


def prepend_licenses() -> None:
    logger.info("Re-applying license headers...")
    for file_path in CLIENT_DIR.rglob("*"):
        if file_path.is_file():
            relative_file_path = file_path.relative_to(CLIENT_DIR)
            file_extension = ""
            # If it's a "dotfile" like .keep
            if (
                relative_file_path.name.startswith(".")
                and "." not in relative_file_path.name[1:]
            ):
                # e.g., for '.keep', this is 'keep'
                file_extension = relative_file_path.name.lstrip(".")
            else:
                # For standard files like generate_clients.py
                file_extension = file_path.suffix.lstrip(".")

            # Check if extension is excluded
            if file_extension in EXCLUDE_EXTENSIONS:
                logger.debug(f"{relative_file_path}: skipped (extension excluded)")
                continue

            is_excluded = False
            # Combine EXCLUDE_PATHS and KEEP_TEST_FILES for comprehensive exclusion check
            # Convert Path objects in EXCLUDE_PATHS to strings for fnmatch compatibility
            # Ensure patterns ending with '/' are handled for directory matching
            all_exclude_patterns = [
                str(p) + ("/" if p.is_dir() else "") for p in EXCLUDE_PATHS
            ] + [str(p) for p in KEEP_TEST_FILES]

            for exclude_pattern_str in all_exclude_patterns:
                # Handle direct file match or if the file is within an excluded directory
                if fnmatch.fnmatch(str(relative_file_path), exclude_pattern_str) or (
                    exclude_pattern_str.endswith("/")
                    and str(relative_file_path).startswith(exclude_pattern_str)
                ):
                    is_excluded = True
                    break

            if is_excluded:
                logger.debug(f"{relative_file_path}: skipped (path excluded)")
                continue

            header_file_path = HEADER_DIR / f"header-{file_extension}.txt"

            if header_file_path.is_file():
                _prepend_header_to_file(file_path, header_file_path)
                logger.debug(f"{relative_file_path}: updated")
            else:
                logger.error(f"No header compatible with file {relative_file_path}")
                sys.exit(2)
    logger.info("License fix complete.")


def build() -> None:
    clean_old_tests()
    generate_polaris_management_client()
    generate_polaris_catalog_client()
    generate_iceberg_catalog_client()
    prepend_licenses()


def main():
    parser = argparse.ArgumentParser(description="Generate Polaris Python clients.")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose output for debugging.",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format="%(message)s")
    else:
        logging.basicConfig(level=logging.INFO, format="%(message)s")

    build()


if __name__ == "__main__":
    main()