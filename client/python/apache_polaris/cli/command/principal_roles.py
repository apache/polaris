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
from pydantic import StrictStr
from typing import Dict, Optional, List, cast

from apache_polaris.cli.command import Command
from apache_polaris.cli.constants import Subcommands, Arguments
from apache_polaris.cli.options.option_tree import Argument
from apache_polaris.sdk.management import (
    PolarisDefaultApi,
    CreatePrincipalRoleRequest,
    PrincipalRole,
    UpdatePrincipalRoleRequest,
    GrantPrincipalRoleRequest,
)
from apache_polaris.cli.command.utils import format_timestamp


@dataclass
class PrincipalRolesCommand(Command):
    """
    A Command implementation to represent `polaris principal-roles`. The instance attributes correspond to parameters
    that can be provided to various subcommands, except `principal_roles_subcommand` which represents the subcommand
    itself.

    Example commands:
        * ./polaris principal-roles create user_role
        * ./polaris principal-roles list --principal user
    """

    principal_roles_subcommand: str
    principal_role_name: str
    principal_name: Optional[str] = None
    catalog_name: Optional[str] = None
    catalog_role_name: Optional[str] = None
    properties: Optional[Dict[str, StrictStr]] = field(default_factory=dict)
    set_properties: Optional[Dict[str, StrictStr]] = field(default_factory=dict)
    remove_properties: Optional[List[str]] = None

    def __post_init__(self) -> None:
        if self.properties is None:
            self.properties = {}
        if self.set_properties is None:
            self.set_properties = {}

    def validate(self) -> None:
        if self.principal_roles_subcommand in {
            Subcommands.CREATE,
            Subcommands.DELETE,
            Subcommands.GET,
            Subcommands.UPDATE,
            Subcommands.GRANT,
            Subcommands.REVOKE,
            Subcommands.SUMMARIZE,
        }:
            if not self.principal_role_name:
                raise Exception(
                    f"Missing required argument: {Argument.to_flag_name(Arguments.PRINCIPAL_ROLE)}"
                )

        if self.principal_roles_subcommand == Subcommands.LIST:
            if self.principal_name and self.catalog_role_name:
                raise Exception(
                    f"You may provide either {Argument.to_flag_name(Arguments.PRINCIPAL)} or"
                    f" {Argument.to_flag_name(Arguments.CATALOG_ROLE)}, but not both"
                )
        if self.principal_roles_subcommand in {Subcommands.GRANT, Subcommands.REVOKE}:
            if not self.principal_name:
                raise Exception(
                    f"Missing required argument for {self.principal_roles_subcommand}:"
                    f" {Argument.to_flag_name(Arguments.PRINCIPAL)}"
                )

    def execute(self, api: PolarisDefaultApi) -> None:
        principal_role_name = cast(str, self.principal_role_name)

        if self.principal_roles_subcommand == Subcommands.CREATE:
            request = CreatePrincipalRoleRequest(
                principal_role=PrincipalRole(
                    name=principal_role_name, properties=self.properties
                )
            )
            api.create_principal_role(request)
        elif self.principal_roles_subcommand == Subcommands.DELETE:
            api.delete_principal_role(principal_role_name)
        elif self.principal_roles_subcommand == Subcommands.GET:
            print(api.get_principal_role(principal_role_name).to_json())
        elif self.principal_roles_subcommand == Subcommands.LIST:
            if self.catalog_role_name:
                for principal_role in api.list_principal_roles(
                    self.catalog_role_name
                ).roles:
                    print(principal_role.to_json())
            elif self.principal_name:
                for principal_role in api.list_principal_roles_assigned(
                    self.principal_name
                ).roles:
                    print(principal_role.to_json())
            else:
                for principal_role in api.list_principal_roles().roles:
                    print(principal_role.to_json())
        elif self.principal_roles_subcommand == Subcommands.UPDATE:
            principal_role = api.get_principal_role(principal_role_name)
            new_properties = principal_role.properties or {}

            # Add or update all entries specified in set_properties
            if self.set_properties:
                new_properties = {**new_properties, **self.set_properties}

            # Remove all keys specified in remove_properties
            if self.remove_properties:
                for to_remove in self.remove_properties:
                    new_properties.pop(to_remove, None)

            request = UpdatePrincipalRoleRequest(
                current_entity_version=principal_role.entity_version,
                properties=new_properties,
            )
            api.update_principal_role(principal_role_name, request)
        elif self.principal_roles_subcommand == Subcommands.GRANT:
            request = GrantPrincipalRoleRequest(
                principal_role=PrincipalRole(name=principal_role_name),
            )
            api.assign_principal_role(cast(str, self.principal_name), request)
        elif self.principal_roles_subcommand == Subcommands.REVOKE:
            api.revoke_principal_role(
                cast(str, self.principal_name), principal_role_name
            )
        elif self.principal_roles_subcommand == Subcommands.SUMMARIZE:
            self._generate_summary(api)
        else:
            raise Exception(
                f"{self.principal_roles_subcommand} is not supported in the CLI"
            )

    def _generate_summary(self, api: PolarisDefaultApi) -> None:
        principal_role_name = cast(str, self.principal_role_name)
        print(f"Principal Role: {principal_role_name}")
        print("-" * 80)
        # Metadata
        role = api.get_principal_role(principal_role_name)
        print("Metadata")
        print(f"  {'Created:':<30} {format_timestamp(role.create_timestamp)}")
        print(f"  {'Modified:':<30} {format_timestamp(role.last_update_timestamp)}")
        print(f"  {'Version:':<30} {role.entity_version}")

        # Assigned Principals
        principals = (
            api.list_assignee_principals_for_principal_role(
                principal_role_name
            ).principals
            or []
        )
        print("\nAssigned Principals")
        if principals:
            for principal in sorted(principals, key=lambda x: x.name):
                print(f"  - {principal.name}")
        else:
            print("  No principals assigned")

        # Assigned Catalog Roles
        assigned_catalog_roles = []
        catalogs = api.list_catalogs().catalogs or []
        for catalog in catalogs:
            catalog_roles = (
                api.list_catalog_roles_for_principal_role(
                    principal_role_name, catalog.name
                ).roles
                or []
            )
            for catalog_role in catalog_roles:
                assigned_catalog_roles.append((catalog_role.name, catalog.name))

        print("\nAssigned Catalog Roles")
        if assigned_catalog_roles:
            for catalog_role_name, catalog_name in sorted(assigned_catalog_roles):
                print(f"  - {catalog_role_name} (Catalog: {catalog_name})")
        else:
            print("  No catalog roles assigned")
        print("-" * 80)
