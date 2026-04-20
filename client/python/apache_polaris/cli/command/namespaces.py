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
import json
from dataclasses import dataclass, field
from pydantic import StrictStr
from typing import Dict, Optional, List, cast

from apache_polaris.cli.command import Command
from apache_polaris.cli.command.utils import get_catalog_api_client
from apache_polaris.cli.constants import Subcommands, Arguments, UNIT_SEPARATOR
from apache_polaris.cli.options.option_tree import Argument
from apache_polaris.sdk.catalog import IcebergCatalogAPI, CreateNamespaceRequest
from apache_polaris.sdk.management import PolarisDefaultApi
from apache_polaris.sdk.catalog.api.policy_api import PolicyAPI


@dataclass
class NamespacesCommand(Command):
    """
    A Command implementation to represent `polaris namespaces`. The instance attributes correspond to parameters
    that can be provided to various subcommands, except `namespaces_subcommand` which represents the subcommand
    itself.

    Example commands:
        * ./polaris namespaces create --catalog my_schema my_namespace
        * ./polaris namespaces list --catalog my_catalog
        * ./polaris namespaces delete --catalog my_catalog my_namespace.inner
    """

    namespaces_subcommand: str
    catalog: Optional[str] = None
    namespace: Optional[List[StrictStr]] = None
    parent: Optional[List[StrictStr]] = None
    location: Optional[str] = None
    properties: Optional[Dict[str, StrictStr]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.properties is None:
            self.properties = {}

    def validate(self) -> None:
        if not self.catalog:
            raise Exception(
                f"Missing required argument: {Argument.to_flag_name(Arguments.CATALOG)}"
            )
        if self.namespaces_subcommand in {
            Subcommands.CREATE,
            Subcommands.DELETE,
            Subcommands.GET,
            Subcommands.SUMMARIZE,
        }:
            if not self.namespace:
                raise Exception(
                    f"Missing required argument: {Argument.to_flag_name(Arguments.NAMESPACE)}"
                )

    def execute(self, api: PolarisDefaultApi) -> None:
        catalog_api_client = get_catalog_api_client(api)
        catalog_api = IcebergCatalogAPI(catalog_api_client)

        catalog_name = cast(str, self.catalog)
        namespace = cast(List[str], self.namespace)

        if self.namespaces_subcommand == Subcommands.CREATE:
            req_properties = self.properties or {}
            if self.location:
                req_properties = {**req_properties, Arguments.LOCATION: self.location}
            request = CreateNamespaceRequest(
                namespace=namespace,
                properties=req_properties,
            )
            catalog_api.create_namespace(
                prefix=catalog_name, create_namespace_request=request
            )
        elif self.namespaces_subcommand == Subcommands.LIST:
            if self.parent:
                result = catalog_api.list_namespaces(
                    prefix=catalog_name, parent=UNIT_SEPARATOR.join(self.parent)
                )
            else:
                result = catalog_api.list_namespaces(prefix=catalog_name)
            for namespace_item in result.namespaces:
                print(json.dumps({"namespace": ".".join(namespace_item)}))
        elif self.namespaces_subcommand == Subcommands.DELETE:
            catalog_api.drop_namespace(
                prefix=catalog_name,
                namespace=UNIT_SEPARATOR.join(namespace),
            )
        elif self.namespaces_subcommand == Subcommands.GET:
            print(
                catalog_api.load_namespace_metadata(
                    prefix=catalog_name,
                    namespace=UNIT_SEPARATOR.join(namespace),
                ).to_json()
            )
        elif self.namespaces_subcommand == Subcommands.SUMMARIZE:
            self._generate_summary(catalog_api)
        else:
            raise Exception(f"{self.namespaces_subcommand} is not supported in the CLI")

    def _generate_summary(self, catalog_api: IcebergCatalogAPI) -> None:
        catalog_name = cast(str, self.catalog)
        namespace = cast(List[str], self.namespace)
        ns_str = UNIT_SEPARATOR.join(namespace)

        print(f"Namespace: {'.'.join(namespace)}")
        print("-" * 80)
        # Metadata
        print("Metadata")
        print(f"  {'Level:':<30} {len(namespace)}")
        if len(namespace) > 1:
            print(f"  {'Parent:':<30} {'.'.join(namespace[:-1])}")
        sub_ns = (
            catalog_api.list_namespaces(prefix=catalog_name, parent=ns_str).namespaces
            or []
        )
        print(f"  {'Sub-namespaces:':<30} {len(sub_ns)}")
        tables = (
            catalog_api.list_tables(prefix=catalog_name, namespace=ns_str).identifiers
            or []
        )
        print(f"  {'Tables:':<30} {len(tables)}")
        views = (
            catalog_api.list_views(prefix=catalog_name, namespace=ns_str).identifiers
            or []
        )
        print(f"  {'Views:':<30} {len(views)}")

        # Effective policies
        policy_api = PolicyAPI(catalog_api.api_client)
        policies_resp = policy_api.get_applicable_policies(
            prefix=catalog_name, namespace=ns_str
        )
        print("\nEffective Policies")
        applicable_policies = policies_resp.applicable_policies or []
        if applicable_policies:
            for policy in sorted(applicable_policies, key=lambda x: x.name):
                source = "Direct"
                if policy.inherited:
                    target = (
                        ".".join(policy.namespace) if policy.namespace else "Catalog"
                    )
                    source = f"Inherited from {target}"
                print(f"  - {policy.name} ({source})")
        else:
            print("  No policies apply to this namespace")
        print("-" * 80)
