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


@dataclass
class FindCommand(Command):
    identifier: str
    catalog_name: Optional[str] = None
    _results_count: int = 0
    _current_context: Optional[str] = field(default=None, repr=False)

    def validate(self) -> None:
        pass

    def execute(self, api: PolarisDefaultApi) -> None:
        print(f"Searching for '{self.identifier}'...")
        # Resolve path
        target_catalog, target_ns, target_leaf = resolve_identifier(self.identifier)
        # Recombine if catalog_name is explicitly provided
        if self.catalog_name and target_catalog:
            target_ns = [target_catalog] + target_ns
            target_catalog = None
        # Global entities search
        if not self.catalog_name and not target_catalog:
            self._find_global_entities(api, target_leaf)
        # Scoped search
        catalogs_to_search = []
        if self.catalog_name:
            catalogs_to_search = [self.catalog_name]
        elif target_catalog:
            # Verify if target_catalog actually exists
            try:
                api.get_catalog(catalog_name=target_catalog)
                catalogs_to_search = [target_catalog]
            except Exception as e:
                if getattr(e, "status", None) == 404:
                    # not a real catalog, treat it as a namespace
                    target_ns = [target_catalog] + target_ns
                    target_catalog = None
                    try:
                        catalogs_to_search = [
                            catalog.name
                            for catalog in api.list_catalogs().catalogs or []
                        ]
                    except Exception as list_e:
                        handle_api_exception("Catalog Listing", list_e)
                else:
                    handle_api_exception("Catalog Access", e)
        else:
            try:
                catalogs_to_search = [
                    catalog.name for catalog in api.list_catalogs().catalogs or []
                ]
            except Exception as e:
                handle_api_exception("Catalog Listing", e)
        if catalogs_to_search:
            for catalog_name in catalogs_to_search:
                self._find_in_catalog(api, catalog_name, target_ns, target_leaf)
        # Summary
        if self._results_count > 0:
            print(f"Found {self._results_count} matching identifiers.")
        else:
            print(f"No identifiers found matching '{self.identifier}'.")

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
            handle_api_exception("Principals", e)
        # Check principal roles
        try:
            for principal_role in api.list_principal_roles().roles or []:
                if is_fuzzy_match(name, principal_role.name):
                    self._print_result(context, "Principal Rolee", principal_role.name)
        except Exception as e:
            handle_api_exception("Principal Role", e)
        # Check catalogs
        try:
            for catalog in api.list_catalogs().catalogs or []:
                if is_fuzzy_match(name, catalog.name):
                    self._print_result(context, "Catalog", catalog.name)
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
