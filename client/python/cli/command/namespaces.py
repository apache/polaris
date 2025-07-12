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
import re
from dataclasses import dataclass
from typing import Dict, Optional, List

from pydantic import StrictStr

from cli.command import Command
from cli.constants import Subcommands, Arguments, UNIT_SEPARATOR
from cli.options.option_tree import Argument
from polaris.catalog import IcebergCatalogAPI, CreateNamespaceRequest, ApiClient, Configuration
from polaris.management import PolarisDefaultApi


@dataclass
class NamespacesCommand(Command):
    """
    A Command implementation to represent `polaris namespaces`. The instance attributes correspond to parameters
    that can be provided to various subcommands

    Example commands:
        * ./polaris namespaces create --catalog my_schema my_namespace
        * ./polaris namespaces list --catalog my_catalog
        * ./polaris namespaces delete --catalog my_catalog my_namespace.inner
    """

    namespaces_subcommand: str
    catalog: str
    namespace: List[StrictStr]
    parent: List[StrictStr]
    location: str
    properties: Optional[Dict[str, StrictStr]]

    def validate(self):
        if not self.catalog:
            raise Exception(
                f"Missing required argument: {Argument.to_flag_name(Arguments.CATALOG)}"
            )

    def _get_catalog_api(self, api: PolarisDefaultApi):
        """
        Convert a management API to a catalog API
        """
        catalog_host = re.match(
            r"(https?://.+)/api/management", api.api_client.configuration.host
        ).group(1)
        configuration = Configuration(
            host=f"{catalog_host}/api/catalog",
            username=api.api_client.configuration.username,
            password=api.api_client.configuration.password,
            access_token=api.api_client.configuration.access_token,
        )
        return IcebergCatalogAPI(ApiClient(configuration))

    def execute(self, api: PolarisDefaultApi) -> None:
        catalog_api = self._get_catalog_api(api)
        if self.namespaces_subcommand == Subcommands.CREATE:
            req_properties = self.properties or {}
            if self.location:
                req_properties = {**req_properties, Arguments.LOCATION: self.location}
            request = CreateNamespaceRequest(
                namespace=self.namespace, properties=req_properties
            )
            catalog_api.create_namespace(
                prefix=self.catalog, create_namespace_request=request
            )
        elif self.namespaces_subcommand == Subcommands.LIST:
            if self.parent is not None:
                result = catalog_api.list_namespaces(
                    prefix=self.catalog, parent=UNIT_SEPARATOR.join(self.parent)
                )
            else:
                result = catalog_api.list_namespaces(prefix=self.catalog)
            for namespace in result.namespaces:
                print(json.dumps({"namespace": ".".join(namespace)}))
        elif self.namespaces_subcommand == Subcommands.DELETE:
            catalog_api.drop_namespace(
                prefix=self.catalog, namespace=UNIT_SEPARATOR.join(self.namespace)
            )
        elif self.namespaces_subcommand == Subcommands.GET:
            print(
                catalog_api.load_namespace_metadata(
                    prefix=self.catalog, namespace=UNIT_SEPARATOR.join(self.namespace)
                ).to_json()
            )
        else:
            raise Exception(f"{self.namespaces_subcommand} is not supported in the CLI")
