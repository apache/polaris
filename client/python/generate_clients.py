#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import sys
import os.path
import subprocess
from pathlib import Path
import logging
import argparse
import shutil
import ast

# Paths
CLIENT_DIR = Path(__file__).parent
HEADER_DIR = CLIENT_DIR / "templates"
SPEC_DIR = CLIENT_DIR / "spec"
POLARIS_MANAGEMENT_SPEC = SPEC_DIR / "polaris-management-service.yml"
ICEBERG_CATALOG_SPEC = SPEC_DIR / "iceberg-rest-catalog-open-api.yaml"
POLARIS_CATALOG_SPEC = SPEC_DIR / "polaris-catalog-service.yaml"
OPEN_API_GENERATOR_IGNORE = CLIENT_DIR / ".openapi-generator-ignore"

# Open API Generator Configs
PACKAGE_NAME_POLARIS_MANAGEMENT = (
    "--additional-properties=packageName=apache_polaris.sdk.management"
)
PACKAGE_NAME_POLARIS_CATALOG = (
    "--additional-properties=packageName=apache_polaris.sdk.catalog"
)
PYTHON_VERSION = "--additional-properties=pythonVersion=3.10"

# Cleanup
EXCLUDE_PATHS = [
    Path("apache_polaris/cli"),
    Path("integration_tests"),
    Path("tests"),
    Path("pyproject.toml"),
    Path("requirements.txt"),
    Path("docker-compose.yml"),
    Path("README.md"),
    Path("generate_clients.py"),
    Path("hatch_build.py"),
    Path("templates"),
    Path("spec"),
    Path("LICENSE"),
    Path("NOTICE"),
]
EXCLUDE_EXTENSIONS = [
    "json",
    "iml",
    "keep",
    "gitignore",
    "pyc",
    "lock",
]

logger = logging.getLogger(__name__)


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
            str(CLIENT_DIR),
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
            str(CLIENT_DIR),
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
            str(CLIENT_DIR),
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


def _prepend_header_to_file(file_path: Path, header_file_path: Path) -> bool:
    try:
        with open(header_file_path, "r") as hf:
            header_content = hf.read()
        with open(file_path, "r") as f:
            original_content = f.read()
        if original_content.startswith(header_content):
            return False
        with open(file_path, "w") as f:
            f.write(header_content + original_content)
        return True
    except IOError as e:
        logger.error(f"Error prepending header to {file_path}: {e}")
        return False


def prepend_licenses() -> None:
    logger.info("Re-applying license headers...")

    all_excluded_paths = set(EXCLUDE_PATHS)
    tmp_dirs = {
        "__pycache__",
        "venv",
        ".venv",
        "build",
        "dist",
        ".pytest_cache",
        ".ruff_cache",
        ".mypy_cache",
    }

    for root, dirs, files in os.walk(CLIENT_DIR):
        relative_root = Path(root).relative_to(CLIENT_DIR)
        # Prune dirs in-place to prevent os.walk from entering them to reduce I/O
        ## Prune hidden dirs
        ## Prune temp (cache/builds/venv) dirs
        ## Prune manual exclusion dirs
        dirs[:] = [
            d
            for d in dirs
            if not (
                d.startswith(".")
                or d in tmp_dirs
                or (relative_root / d) in all_excluded_paths
            )
        ]
        for file in files:
            file_path = Path(root) / file
            relative_file_path = file_path.relative_to(CLIENT_DIR)
            if file.startswith(".") and "." not in file[1:]:
                file_extension = file.lstrip(".")
            else:
                file_extension = file_path.suffix.lstrip(".")
            if file_extension in EXCLUDE_EXTENSIONS:
                logger.debug(f"{relative_file_path}: skipped (extension excluded)")
                continue
            if file.startswith(".") or relative_file_path in all_excluded_paths:
                logger.debug(f"{relative_file_path}: skipped (file excluded)")
                continue
            header_file_path = HEADER_DIR / f"header-{file_extension}.txt"
            if header_file_path.is_file():
                if _prepend_header_to_file(file_path, header_file_path):
                    logger.debug(f"{relative_file_path}: updated")
                else:
                    logger.debug(
                        f"{relative_file_path}: skipped (header already present)"
                    )
            else:
                logger.error(f"No header file found for {relative_file_path}")
                sys.exit(2)
    logger.info("License fix complete.")


def prepare_spec_dir() -> None:
    logger.info("Preparing spec directory...")
    spec_dir = Path(SPEC_DIR)
    spec_source_dir = CLIENT_DIR.parent.parent / "spec"

    if spec_source_dir.is_dir():
        logger.info(f"Copying spec directory from {spec_source_dir} to {spec_dir}")
        if spec_dir.exists():
            shutil.rmtree(spec_dir)
        shutil.copytree(spec_source_dir, spec_dir)
        logger.info("Spec directory copied to ensure it is up-to-date.")
    elif not spec_dir.is_dir():
        # This will be hit during an sdist build if spec directory wasn't in the package,
        # and we can't find the source to copy from.
        logger.error(
            "Fatal: spec directory is missing and the source to copy it from was not found."
        )
        sys.exit(1)
    else:
        # This is the case for sdist where the spec dir is already there and we don't have
        # the source to copy from.
        logger.info("Source spec directory not found, using existing spec directory.")


def build() -> None:
    prepare_spec_dir()
    generate_polaris_management_client()
    generate_polaris_catalog_client()
    generate_iceberg_catalog_client()
    fix_catalog_models_init()
    prepend_licenses()


def fix_catalog_models_init() -> None:
    """
    Regenerate the `apache_polaris.sdk.catalog.models.__init__.py` file by consolidating
    imports for all model classes found under `apache_polaris/sdk/catalog/models`.

    This ensures that rerunning the OpenAPI Generator (which overwrites `__init__.py`)
    does not cause missing imports for earlier generated model files.
    """
    logger.info("Fixing catalog models __init__.py...")
    models_dir = CLIENT_DIR / "apache_polaris" / "sdk" / "catalog" / "models"
    init_py = models_dir / "__init__.py"

    # Get all python files in the models directory except __init__.py
    model_files = [
        f for f in models_dir.glob("*.py") if f.is_file() and f.name != "__init__.py"
    ]

    # Generate import statements
    imports = []
    for model_file in sorted(model_files):
        module_name = model_file.stem
        with open(model_file, "r") as f:
            tree = ast.parse(f.read())
        class_name = None
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                # Find the first class that doesn't start with an underscore
                if not node.name.startswith("_"):
                    class_name = node.name
                    break
        if class_name:
            imports.append(
                f"from apache_polaris.sdk.catalog.models.{module_name} import {class_name}"
            )
        else:
            logger.warning(f"Could not find a suitable class in {model_file}")

    # Write the new __init__.py
    with open(init_py, "w") as f:
        f.write("\n".join(sorted(imports)))
    logger.info("Catalog models __init__.py fixed.")


def main() -> None:
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
