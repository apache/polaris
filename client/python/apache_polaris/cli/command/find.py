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
from dataclasses import dataclass
from typing import Optional, List

from apache_polaris.cli.command import Command
from apache_polaris.cli.command.utils import (
    get_catalog_api_client,
    crawl_namespace,
    resolve_identifier,
    is_fuzzy_match,
    handle_api_exception,
)
from apache_polaris.sdk.catalog import IcebergCatalogAPI
from apache_polaris.sdk.management import PolarisDefaultApi
from apache_polaris.cli.options.option_tree import Argument
from apache_polaris.cli.constants import Arguments


@dataclass
class FindCommand(Command):
    identifier: str
    catalog_name: Optional[str] = None
    _results_count: int = 0

    def validate(self) -> None:
        if not self.identifier:
            raise Exception(
                f"Missing required argument: {Argument.to_flag_name(Arguments.IDENTIFIER)}"
            )

    def execute(self, api: PolarisDefaultApi) -> None:
        print(f"Searching fpr '{self.identifier}'...")
        target_catalog, target_ns, target_leaf = resolve_identifier(self.identifier)
        # If catalog_name is provided, rebuilt the target_ns as first part will now be namespace instead of catalog
        if self.catalog_name and target_catalog:
            target_ns = [target_catalog] + target_ns
            target_catalog = None
        # Global entities search
        if not self.catalog_name and not target_catalog:
            self._find_management_entities(api, target_leaf)
        # Scoped search
        catalogs_to_search = []
        if self.catalog_name:
            catalogs_to_search = [self.catalog_name]
        elif target_catalog:
            catalogs_to_search = [target_catalog]
        else:
            try:
                catalogs_to_search = [
                    catalog.name for catalog in api.list_catalogs() or []
                ]
            except Exception as e:
                handle_api_exception("Catalog Listig", e)
        if catalogs_to_search:
            for catalog_name in catalogs_to_search:
                self._find_in_catalog(api, catalog_name, target_ns, target_leaf)
        # Summary
        if self._results_count > 0:
            print(f"Found {self._results_count} matching identifiers.")
        else:
            print("No identifiers found matching '{self.identifier}'.")

    def _find_management_entities(self, api: PolarisDefaultApi, name: str) -> None:
        # Check principals
        try:
            for principal in api.list_principals().principals or []:
                if is_fuzzy_match(name, principal.name):
                    print(f". {'[Principal]':<30} {principal.name}")
                    self._results_count += 1
        except Exception as e:
            handle_api_exception("Principals", e)
        # Check principal Roles
        try:
            for principal_role in api.list_principal_roles().roles or []:
                if is_fuzzy_match(name, principal_role.name):
                    print(f". {'[Principal Role]':<30} {principal_role.name}")
                    self._results_count += 1
        except Exception as e:
            handle_api_exception("Principal Roles", e)
        # Check catalogs
        try:
            for catalog in api.list_catalogs().catalogs or []:
                if is_fuzzy_match(name, catalog.name):
                    print(f". {'[Catalog]':<30} {catalog.name}")
                    self._results_count += 1
        except Exception as e:
            handle_api_exception("Catalogs", e)

    def _find_in_catalog(
        self,
        api: PolarisDefaultApi,
        catalog_name: str,
        namespace: Optional[List[str]],
        leaf_name: str,
    ) -> None:
        catalog_api = IcebergCatalogAPI(get_catalog_api_client(api))
        # Check catalog roles
        try:
            pass
        except Exception as e:
            handle_api_exception(f"Catalog Roles ({catalog_name})", e)
        # Search namespaces recursively
        for entity_type, path in crawl_namespace(
            catalog_api=catalog_api,
            catalog_name=catalog_name,
            start_ns=namespace if namespace else None,
            on_error=handle_api_exception,
        ):
            # Check if path matches to ns + fuzzy leaf
            if is_fuzzy_match(leaf_name, path[-1]):
                label = f"{entity_type.capitalize()}"
                print(f"  {label:<30} {catalog_name}.{'.'.join(path)}")
                self._results_count += 1
