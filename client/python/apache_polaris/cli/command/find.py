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
from dataclasses import dataclass, field
from typing import Optional, List, Tuple

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


@dataclass
class FindCommand(Command):
    identifier: str
    catalog_name: Optional[str] = None
    _results_count: int = 0
    _current_context: Optional[str] = field(default=None, repr=False)

    def validate(self) -> None:
        if not self.identifier.strip():
            raise Exception("The search identifier cannot be empty.")

    def execute(self, api: PolarisDefaultApi) -> None:
        print(f"Searching for '{self.identifier}'...")
        # If catalog name is provided, the identifier is a namespace path + leaf
        if self.catalog_name:
            parts = self.identifier.split(".")
            target_catalog = None
            target_ns = parts[:-1]
            target_leaf = parts[-1]
        else:
            # Otherwise, use standard resolution to guess catalog vs namesapce for first part
            target_catalog, target_ns, target_leaf = resolve_identifier(self.identifier)
        # Recombine if catalog_name is explicitly provided
        if self.catalog_name and target_catalog:
            target_ns = [target_catalog] + target_ns
            target_catalog = None
        # Determine which catalogs to search
        catalogs_to_search, effective_ns = self._resolve_search_scope(
            api, target_catalog, target_ns
        )
        if not catalogs_to_search:
            print("No catalogs found to search.")
            return
        # Global entities search
        if not self.catalog_name and not target_catalog:
            self._find_global_entities(api, target_leaf)
        # Scoped search
        if catalogs_to_search:
            catalog_api = IcebergCatalogAPI(get_catalog_api_client(api))
            for catalog_name in catalogs_to_search:
                self._find_in_catalog(
                    api, catalog_api, catalog_name, effective_ns, target_leaf
                )
        # Summary
        if self._results_count > 0:
            print(f"Found {self._results_count} matching identifiers.")
        else:
            print(f"No identifiers found matching '{self.identifier}'.")

    def _resolve_search_scope(
        self,
        api: PolarisDefaultApi,
        target_catalog: Optional[str],
        target_ns: List[str],
    ) -> Tuple[Optional[List[str]], List[str]]:
        if self.catalog_name:
            try:
                api.get_catalog(self.catalog_name)
                return [self.catalog_name], target_ns
            except Exception as e:
                if getattr(e, "status", None) == 404:
                    print(f"Catalog '{self.catalog_name}' not found.")
                    return None, []
                handle_api_exception("Catalog Access", e)
                return [], target_ns
        if target_catalog:
            try:
                api.get_catalog(target_catalog)
                return [target_catalog], target_ns
            except Exception as e:
                if getattr(e, "status", None) != 404:
                    handle_api_exception("Catalog Access", e)
                # If not found, target_catalog is likely a top-level namespace
                # Fallback to search all catalogs
                effective_ns = [target_catalog] + target_ns
                try:
                    all_catalogs = [
                        catalog.name for catalog in api.list_catalogs().catalogs or []
                    ]
                    return all_catalogs, effective_ns
                except Exception as list_e:
                    handle_api_exception("Catalog Listing", list_e)
                    return [], effective_ns
        try:
            return [
                catalog.name for catalog in api.list_catalogs().catalogs or []
            ], target_ns
        except Exception as e:
            handle_api_exception("Catalog Listing", e)
            return [], target_ns

    def _print_result(self, context: str, label: str, value: str) -> None:
        if self._current_context != context:
            print(f"[{context}]")
            self._current_context = context
        print(f"  {label + ':':<20} {value}")
        self._results_count += 1

    def _find_global_entities(self, api: PolarisDefaultApi, name: str) -> None:
        context = "Global"
        # Check principals
        try:
            for principal in api.list_principals().principals or []:
                if is_fuzzy_match(name, principal.name):
                    self._print_result(context, "Principal", principal.name)
        except Exception as e:
            handle_api_exception("Principal", e)
        # Check principal roles
        try:
            for principal_role in api.list_principal_roles().roles or []:
                if is_fuzzy_match(name, principal_role.name):
                    self._print_result(context, "Principal Role", principal_role.name)
        except Exception as e:
            handle_api_exception("Principal Role", e)
        # Check catalogs
        try:
            for catalog in api.list_catalogs().catalogs or []:
                if is_fuzzy_match(name, catalog.name):
                    self._print_result(context, "Catalog", catalog.name)
        except Exception as e:
            handle_api_exception("Catalog", e)

    def _find_in_catalog(
        self,
        api: PolarisDefaultApi,
        catalog_api: IcebergCatalogAPI,
        catalog_name: str,
        namespace: Optional[List[str]],
        leaf_name: str,
    ) -> None:
        context = f"Catalog: {catalog_name}"

        # Helper function to suppress HTTP code 404 during find
        def on_crawl_error(label: str, e: Exception) -> None:
            if getattr(e, "status", None) != 404:
                handle_api_exception(label, e)

        # Check catalog roles
        if not namespace:
            try:
                for catalog_role in api.list_catalog_roles(catalog_name).roles or []:
                    if is_fuzzy_match(leaf_name, catalog_role.name):
                        self._print_result(context, "Catalog Role", catalog_role.name)
            except Exception as e:
                handle_api_exception(f"Catalog Role ({catalog_name})", e)
        # Search namespaces recursively
        for entity_type, path in crawl_namespace(
            catalog_api=catalog_api,
            catalog_name=catalog_name,
            start_ns=namespace if namespace else None,
            on_error=on_crawl_error,
        ):
            # Check if path matches to ns + fuzzy leaf
            if is_fuzzy_match(leaf_name, path[-1]):
                self._print_result(context, entity_type.capitalize(), ".".join(path))
