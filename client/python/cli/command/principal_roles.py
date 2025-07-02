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
from typing import Dict, Optional, List

from pydantic import StrictStr

from cli.command import Command
from cli.constants import Subcommands, Arguments
from cli.options.option_tree import Argument
from polaris.management import PolarisDefaultApi, CreatePrincipalRoleRequest, PrincipalRole, UpdatePrincipalRoleRequest, \
    GrantPrincipalRoleRequest


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
    principal_name: str
    catalog_name: str
    catalog_role_name: str
    properties: Optional[Dict[str, StrictStr]]
    set_properties: Dict[str, StrictStr]
    remove_properties: List[str]

    def validate(self):
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
        if self.principal_roles_subcommand == Subcommands.CREATE:
            request = CreatePrincipalRoleRequest(
                principal_role=PrincipalRole(
                    name=self.principal_role_name, properties=self.properties
                )
            )
            api.create_principal_role(request)
        elif self.principal_roles_subcommand == Subcommands.DELETE:
            api.delete_principal_role(self.principal_role_name)
        elif self.principal_roles_subcommand == Subcommands.GET:
            print(api.get_principal_role(self.principal_role_name).to_json())
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
            principal_role = api.get_principal_role(self.principal_role_name)
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
            api.update_principal_role(self.principal_role_name, request)
        elif self.principal_roles_subcommand == Subcommands.GRANT:
            request = GrantPrincipalRoleRequest(
                principal_role=PrincipalRole(name=self.principal_role_name),
            )
            api.assign_principal_role(self.principal_name, request)
        elif self.principal_roles_subcommand == Subcommands.REVOKE:
            api.revoke_principal_role(self.principal_name, self.principal_role_name)
        else:
            raise Exception(
                f"{self.principal_roles_subcommand} is not supported in the CLI"
            )
