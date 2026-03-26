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
from collections import Counter
from dataclasses import dataclass, field
from typing import Optional, List, Tuple

from apache_polaris.cli.command import Command
from apache_polaris.cli.command.utils import (
    get_catalog_api_client,
    crawl_namespace,
    is_fuzzy_match,
    handle_api_exception,
)
from apache_polaris.cli.constants import EntityType
from apache_polaris.sdk.catalog import IcebergCatalogAPI
from apache_polaris.sdk.management import PolarisDefaultApi


@dataclass
class FindCommand(Command):
    """
    A Command implemtation to represent `polaris find`. It searches for an identifier across global entities (principals, roles, catalogs)
    and within catalog resources (namespaces, tables, views) using fuzzy matching.

    Example commands:
        * ./polaris find my_table
        * ./polaris find ns1.my_table
        * ./polaris find --catalog my_catalog my_table
        * ./polaris find my_table --type table
    """

    identifier: str
    catalog_name: Optional[str] = None
    type_filter: Optional[str] = None
    _current_context: Optional[str] = field(default=None, repr=False)
    _type_counts: Counter = field(default_factory=Counter, repr=False)

    def validate(self) -> None:
        if not self.identifier.strip():
            raise Exception("The search identifier cannot be empty.")

    def execute(self, api: PolarisDefaultApi) -> None:
        print(f"Searching for '{self.identifier}'...")
        parts = self.identifier.split(".")
        target_ns = parts[:-1]
        target_leaf = parts[-1]

        # Global entities search
        if not self.catalog_name:
            global_entity_types = {
                EntityType.PRINCIPAL.value,
                EntityType.PRINCIPAL_ROLE.value,
                EntityType.CATALOG.value,
            }
            if not self.type_filter or self.type_filter in global_entity_types:
                self._find_global_entities(api, target_leaf)

        # Determine which catalogs to search
        catalog_entity_types = {
            EntityType.CATALOG_ROLE.value,
            EntityType.NAMESPACE.value,
            EntityType.TABLE.value,
            EntityType.VIEW.value,
        }
        if not self.type_filter or self.type_filter in catalog_entity_types:
            catalogs_to_search, effective_ns = self._resolve_search_scope(
                api, target_ns
            )
            # Quick fail if catalog is not resolvable
            if catalogs_to_search is None:
                return
            # Scoped search
            if catalogs_to_search:
                catalog_api = IcebergCatalogAPI(get_catalog_api_client(api))
                for catalog_name in catalogs_to_search:
                    self._find_in_catalog(
                        api, catalog_api, catalog_name, effective_ns, target_leaf
                    )
            elif self.catalog_name:
                print("No catalogs found to search.")
        else:
            catalogs_to_search = []

        # Summary
        total_results = sum(self._type_counts.values())
        if total_results > 0:
            types_str = ", ".join(
                f"{count} {entity_type if count == 1 else entity_type + 's'}"
                for entity_type, count in sorted(self._type_counts.items())
            )
            print(f"\nFound {total_results} matches ({types_str}).")
        else:
            print(f"No identifiers found matching '{self.identifier}'.")

    def _resolve_search_scope(
        self,
        api: PolarisDefaultApi,
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
        try:
            return [
                catalog.name for catalog in api.list_catalogs().catalogs or []
            ], target_ns
        except Exception as e:
            handle_api_exception("Catalog Listing", e)
            return [], target_ns

    def _print_result(self, context: str, entity_type: EntityType, value: str) -> None:
        if self._current_context != context:
            if self._current_context is not None:
                print()
            print(f"[{context}]")
            self._current_context = context
        label_str = str(entity_type)
        print(f"  {label_str + ':':<20} {value}")
        self._type_counts[label_str] += 1

    def _find_global_entities(self, api: PolarisDefaultApi, name: str) -> None:
        context = "Global"
        # Check principals
        if not self.type_filter or self.type_filter == EntityType.PRINCIPAL.value:
            try:
                for principal in api.list_principals().principals or []:
                    if is_fuzzy_match(name, principal.name):
                        self._print_result(
                            context, EntityType.PRINCIPAL, principal.name
                        )
            except Exception as e:
                handle_api_exception("Principal Listing", e)

        # Check principal roles
        if not self.type_filter or self.type_filter == EntityType.PRINCIPAL_ROLE.value:
            try:
                for principal_role in api.list_principal_roles().roles or []:
                    if is_fuzzy_match(name, principal_role.name):
                        self._print_result(
                            context, EntityType.PRINCIPAL_ROLE, principal_role.name
                        )
            except Exception as e:
                handle_api_exception("Principal Role Listing", e)
        # Check catalogs
        if not self.type_filter or self.type_filter == EntityType.CATALOG.value:
            try:
                for catalog in api.list_catalogs().catalogs or []:
                    if is_fuzzy_match(name, catalog.name):
                        self._print_result(context, EntityType.CATALOG, catalog.name)
            except Exception as e:
                handle_api_exception("Catalog Listing", e)

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
        if not namespace and (
            not self.type_filter or self.type_filter == EntityType.CATALOG_ROLE.value
        ):
            try:
                for catalog_role in api.list_catalog_roles(catalog_name).roles or []:
                    if is_fuzzy_match(leaf_name, catalog_role.name):
                        self._print_result(
                            context, EntityType.CATALOG_ROLE, catalog_role.name
                        )
            except Exception as e:
                handle_api_exception(f"Catalog Role ({catalog_name})", e)
        # Search namespaces recursively
        catalog_resource_types = {
            EntityType.NAMESPACE.value,
            EntityType.TABLE.value,
            EntityType.VIEW.value,
        }
        if not self.type_filter or self.type_filter in catalog_resource_types:
            for entity_type, path in crawl_namespace(
                catalog_api=catalog_api,
                catalog_name=catalog_name,
                start_ns=namespace if namespace else None,
                on_error=on_crawl_error,
                entity_type_filter=self.type_filter,
            ):
                # Check if path matches to ns + fuzzy leaf
                if is_fuzzy_match(leaf_name, path[-1]):
                    self._print_result(context, entity_type, ".".join(path))
