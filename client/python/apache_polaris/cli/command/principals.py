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
from typing import Dict, Optional, List, Iterator, Any, cast

from pydantic import StrictStr

from apache_polaris.cli.command import Command
from apache_polaris.cli.constants import Subcommands, Arguments
from apache_polaris.cli.options.option_tree import Argument
from apache_polaris.sdk.management import (
    PolarisDefaultApi,
    CreatePrincipalRequest,
    Principal,
    PrincipalWithCredentials,
    UpdatePrincipalRequest,
    ResetPrincipalRequest,
)
from apache_polaris.cli.command.utils import format_timestamp


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
    type: Optional[str] = None
    principal_name: Optional[str] = None
    client_id: Optional[str] = None
    principal_role: Optional[str] = None
    properties: Optional[Dict[str, StrictStr]] = field(default_factory=dict)
    set_properties: Optional[Dict[str, StrictStr]] = field(default_factory=dict)
    remove_properties: Optional[List[str]] = None
    new_client_id: Optional[str] = None
    new_client_secret: Optional[str] = None

    def __post_init__(self) -> None:
        if self.properties is None:
            self.properties = {}
        if self.set_properties is None:
            self.set_properties = {}

    def _get_catalogs(self, api: PolarisDefaultApi) -> Iterator[str]:
        for catalog in api.list_catalogs().catalogs:
            yield catalog.to_dict()["name"]

    def _get_principal_roles(self, api: PolarisDefaultApi) -> Iterator[str]:
        for principal_role in api.list_principal_roles_assigned(
            cast(str, self.principal_name)
        ).roles:
            yield principal_role.to_dict()["name"]

    def _get_catalog_roles(
        self, api: PolarisDefaultApi, principal_role_name: str, catalog_name: str
    ) -> Iterator[str]:
        for catalog_role in api.list_catalog_roles_for_principal_role(
            principal_role_name, catalog_name
        ).roles:
            yield catalog_role.to_dict()["name"]

    def _get_privileges(
        self, api: PolarisDefaultApi, catalog_name: str, catalog_role_name: str
    ) -> Iterator[Dict[str, Any]]:
        for grant in api.list_grants_for_catalog_role(
            catalog_name, catalog_role_name
        ).grants:
            yield grant.to_dict()

    def build_credential_json(
        self, principal_with_credentials: PrincipalWithCredentials
    ) -> str:
        credentials = principal_with_credentials.credentials
        return json.dumps(
            {
                "clientId": credentials.client_id,
                "clientSecret": credentials.client_secret.get_secret_value(),
            }
        )

    def validate(self) -> None:
        if self.principals_subcommand in {
            Subcommands.CREATE,
            Subcommands.DELETE,
            Subcommands.GET,
            Subcommands.UPDATE,
            Subcommands.ROTATE_CREDENTIALS,
            Subcommands.ACCESS,
            Subcommands.RESET,
            Subcommands.SUMMARIZE,
        }:
            if not self.principal_name:
                raise Exception(
                    f"Missing required argument: {Argument.to_flag_name(Arguments.PRINCIPAL)}"
                )

    def execute(self, api: PolarisDefaultApi) -> None:
        principal_name = cast(str, self.principal_name)

        if self.principals_subcommand == Subcommands.CREATE:
            request = CreatePrincipalRequest(
                principal=Principal(
                    type=self.type.upper() if self.type else None,
                    name=principal_name,
                    client_id=self.client_id,
                    properties=self.properties,
                )
            )
            print(self.build_credential_json(api.create_principal(request)))
        elif self.principals_subcommand == Subcommands.DELETE:
            api.delete_principal(principal_name)
        elif self.principals_subcommand == Subcommands.GET:
            print(api.get_principal(principal_name).to_json())
        elif self.principals_subcommand == Subcommands.LIST:
            if self.principal_role:
                for principal in api.list_assignee_principals_for_principal_role(
                    cast(str, self.principal_role)
                ).principals:
                    print(principal.to_json())
            else:
                for principal in api.list_principals().principals:
                    print(principal.to_json())
        elif self.principals_subcommand == Subcommands.ROTATE_CREDENTIALS:
            print(self.build_credential_json(api.rotate_credentials(principal_name)))
        elif self.principals_subcommand == Subcommands.UPDATE:
            principal = api.get_principal(principal_name)
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
            api.update_principal(principal_name, request)
        elif self.principals_subcommand == Subcommands.ACCESS:
            principal_obj_name = api.get_principal(principal_name).to_dict()["name"]
            principal_roles = self._get_principal_roles(api)

            # Initialize the result structure
            result = {"principal": principal_obj_name, "principal_roles": []}

            # Construct the result structure for each principal role
            for principal_role in principal_roles:
                role_data: Dict[str, Any] = {
                    "name": principal_role,
                    "catalog_roles": [],
                }
                # For each catalog role, get associated privileges
                for catalog in self._get_catalogs(api):
                    catalog_roles = self._get_catalog_roles(
                        api, principal_role, catalog
                    )
                    for catalog_role in catalog_roles:
                        catalog_data: Dict[str, Any] = {
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
        elif self.principals_subcommand == Subcommands.RESET:
            if self.new_client_id or self.new_client_secret:
                request = ResetPrincipalRequest(
                    clientId=self.new_client_id, clientSecret=self.new_client_secret
                )
                print(
                    self.build_credential_json(
                        api.reset_credentials(principal_name, request)
                    )
                )
            else:
                print(
                    self.build_credential_json(
                        api.reset_credentials(principal_name, None)
                    )
                )
        elif self.principals_subcommand == Subcommands.SUMMARIZE:
            self._generate_summary(api)
        else:
            raise Exception(f"{self.principals_subcommand} is not supported in the CLI")

    def _generate_summary(self, api: PolarisDefaultApi) -> None:
        principal_name = cast(str, self.principal_name)
        print(f"Principal: {principal_name}")
        print("-" * 80)
        # Metadata
        principal = api.get_principal(principal_name)
        print("Metadata")
        print(f"  {'Client ID:':<30} {principal.client_id}")
        print(f"  {'Created:':<30} {format_timestamp(principal.create_timestamp)}")
        print(
            f"  {'Modified:':<30} {format_timestamp(principal.last_update_timestamp)}"
        )
        print(f"  {'Version:':<30} {principal.entity_version}")

        # Assigned Roles
        principal_roles = list(self._get_principal_roles(api))
        print("\nAssigned Roles")
        if principal_roles:
            for principal_role in principal_roles:
                print(f"  - {principal_role}")
        else:
            print("  No principal roles assigned")

        catalogs = api.list_catalogs().catalogs or []
        accessible_catalogs = []
        for catalog in catalogs:
            if any(
                api.list_catalog_roles_for_principal_role(role_name, catalog.name).roles
                for role_name in principal_roles
            ):
                accessible_catalogs.append(catalog.name)

        # Accessible Catalogs
        print("\nAccessible Catalogs")
        if accessible_catalogs:
            for catalog in sorted(accessible_catalogs):
                print(f"  - {catalog}")
        else:
            print("  No accessible catalogs")
        print("-" * 80)
