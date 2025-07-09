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
from dataclasses import dataclass
from typing import Dict, Optional, List

from pydantic import StrictStr

from cli.command import Command
from cli.constants import Subcommands
from polaris.management import (
    PolarisDefaultApi,
    CreatePrincipalRequest,
    Principal,
    PrincipalWithCredentials,
    UpdatePrincipalRequest,
)


@dataclass
class PrincipalsCommand(Command):
    """
    A Command implementation to represent `polaris principals`. The instance attributes correspond to parameters
    that can be provided to various subcommands, except `principals_subcommand` which represents the subcommand
    itself.

    Example commands:
        * ./polaris principals create user
        * ./polaris principals access user
        * ./polaris principals list
        * ./polaris principals list --principal-role filter-to-this-role
    """

    principals_subcommand: str
    type: str
    principal_name: str
    client_id: str
    principal_role: str
    properties: Optional[Dict[str, StrictStr]]
    set_properties: Dict[str, StrictStr]
    remove_properties: List[str]

    def _get_catalogs(self, api: PolarisDefaultApi):
        for catalog in api.list_catalogs().catalogs:
            yield catalog.to_dict()["name"]

    def _get_principal_roles(self, api: PolarisDefaultApi):
        for principal_role in api.list_principal_roles_assigned(
            self.principal_name
        ).roles:
            yield principal_role.to_dict()["name"]

    def _get_catalog_roles(
        self, api: PolarisDefaultApi, principal_role_name: str, catalog_name: str
    ):
        for catalog_role in api.list_catalog_roles_for_principal_role(
            principal_role_name, catalog_name
        ).roles:
            yield catalog_role.to_dict()["name"]

    def _get_privileges(
        self, api: PolarisDefaultApi, catalog_name: str, catalog_role_name: str
    ):
        for grant in api.list_grants_for_catalog_role(
            catalog_name, catalog_role_name
        ).grants:
            yield grant.to_dict()

    def build_credential_json(
        self, principal_with_credentials: PrincipalWithCredentials
    ):
        credentials = principal_with_credentials.credentials
        return json.dumps(
            {
                "clientId": credentials.client_id,
                "clientSecret": credentials.client_secret.get_secret_value(),
            }
        )

    def validate(self):
        pass

    def execute(self, api: PolarisDefaultApi) -> None:
        if self.principals_subcommand == Subcommands.CREATE:
            request = CreatePrincipalRequest(
                principal=Principal(
                    type=self.type.upper(),
                    name=self.principal_name,
                    client_id=self.client_id,
                    properties=self.properties,
                )
            )
            print(self.build_credential_json(api.create_principal(request)))
        elif self.principals_subcommand == Subcommands.DELETE:
            api.delete_principal(self.principal_name)
        elif self.principals_subcommand == Subcommands.GET:
            print(api.get_principal(self.principal_name).to_json())
        elif self.principals_subcommand == Subcommands.LIST:
            if self.principal_role:
                for principal in api.list_assignee_principals_for_principal_role(
                    self.principal_role
                ).principals:
                    print(principal.to_json())
            else:
                for principal in api.list_principals().principals:
                    print(principal.to_json())
        elif self.principals_subcommand == Subcommands.ROTATE_CREDENTIALS:
            print(
                self.build_credential_json(api.rotate_credentials(self.principal_name))
            )
        elif self.principals_subcommand == Subcommands.UPDATE:
            principal = api.get_principal(self.principal_name)
            new_properties = principal.properties or {}

            # Add or update all entries specified in set_properties
            if self.set_properties:
                new_properties = {**new_properties, **self.set_properties}

            # Remove all keys specified in remove_properties
            if self.remove_properties:
                for to_remove in self.remove_properties:
                    new_properties.pop(to_remove, None)

            request = UpdatePrincipalRequest(
                current_entity_version=principal.entity_version,
                properties=new_properties,
            )
            api.update_principal(self.principal_name, request)
        elif self.principals_subcommand == Subcommands.ACCESS:
            principal = api.get_principal(self.principal_name).to_dict()["name"]
            principal_roles = self._get_principal_roles(api)

            # Initialize the result structure
            result = {"principal": principal, "principal_roles": []}

            # Construct the result structure for each principal role
            for principal_role in principal_roles:
                role_data = {"name": principal_role, "catalog_roles": []}
                # For each catalog role, get associated privileges
                for catalog in self._get_catalogs(api):
                    catalog_roles = self._get_catalog_roles(
                        api, principal_role, catalog
                    )
                    for catalog_role in catalog_roles:
                        catalog_data = {
                            "name": catalog_role,
                            "catalog": catalog,
                            "privileges": [],
                        }
                        catalog_data["privileges"] = list(
                            self._get_privileges(
                                api, catalog_data["catalog"], catalog_role
                            )
                        )
                        role_data["catalog_roles"].append(catalog_data)
                result["principal_roles"].append(role_data)
            print(json.dumps(result))
        else:
            raise Exception(f"{self.principals_subcommand} is not supported in the CLI")
