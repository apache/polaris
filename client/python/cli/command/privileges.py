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
from typing import List

from pydantic import StrictStr

from cli.command import Command
from cli.constants import Subcommands, Actions, Arguments
from cli.options.option_tree import Argument
from polaris.management import (
    PolarisDefaultApi,
    AddGrantRequest,
    NamespaceGrant,
    RevokeGrantRequest,
    CatalogGrant,
    TableGrant,
    ViewGrant,
    CatalogPrivilege,
    NamespacePrivilege,
    TablePrivilege,
    ViewPrivilege,
)


@dataclass
class PrivilegesCommand(Command):
    """
    A Command implementation to represent `polaris privileges`. Unlike other commands, `privileges` itself takes two
    parameters -- catalog_name and catalog_role_name. The other instance attributes, besides `privileges_subcommand` and
    `action`, represent parameters provided to either the `grant` or `revoke` action.

    Example commands:
        * ./polaris privileges table grant --catalog c --catalog-role cr --namespace n --table t PRIVILEGE_NAME
        * ./polaris privileges namespace revoke --catalog c --catalog-role cr --namespace n PRIVILEGE_NAME
        * ./polaris privileges list --catalog c --catalog-role cr
    """

    privileges_subcommand: str
    action: str
    catalog_name: str
    catalog_role_name: str
    namespace: List[StrictStr]
    view: str
    table: str
    privilege: str
    cascade: bool

    def validate(self):
        if not self.catalog_name:
            raise Exception(
                f"Missing required argument: {Argument.to_flag_name(Arguments.CATALOG)}"
            )
        if not self.catalog_role_name:
            raise Exception(
                f"Missing required argument: {Argument.to_flag_name(Arguments.CATALOG_ROLE)}"
            )

        if not self.privileges_subcommand:
            raise Exception("A subcommand must be provided")
        if (
            self.privileges_subcommand
            in {Subcommands.NAMESPACE, Subcommands.TABLE, Subcommands.VIEW}
            and not self.namespace
        ):
            raise Exception(
                f"Missing required argument: {Argument.to_flag_name(Arguments.NAMESPACE)}"
            )

        if self.action == Actions.GRANT and self.cascade:
            raise Exception("Unrecognized argument for GRANT: --cascade")

        if self.privileges_subcommand == Subcommands.CATALOG:
            if self.privilege not in {i.value for i in CatalogPrivilege}:
                raise Exception(f"Invalid catalog privilege: {self.privilege}")
        if self.privileges_subcommand == Subcommands.NAMESPACE:
            if self.privilege not in {i.value for i in NamespacePrivilege}:
                raise Exception(f"Invalid namespace privilege: {self.privilege}")
        if self.privileges_subcommand == Subcommands.TABLE:
            if self.privilege not in {i.value for i in TablePrivilege}:
                raise Exception(f"Invalid table privilege: {self.privilege}")
        if self.privileges_subcommand == Subcommands.VIEW:
            if self.privilege not in {i.value for i in ViewPrivilege}:
                raise Exception(f"Invalid view privilege: {self.privilege}")

    def execute(self, api: PolarisDefaultApi) -> None:
        if self.privileges_subcommand == Subcommands.LIST:
            for grant in api.list_grants_for_catalog_role(
                self.catalog_name, self.catalog_role_name
            ).grants:
                print(grant.to_json())
        else:
            grant = None
            if self.privileges_subcommand == Subcommands.CATALOG:
                grant = CatalogGrant(
                    type=Subcommands.CATALOG, privilege=CatalogPrivilege(self.privilege)
                )
            elif self.privileges_subcommand == Subcommands.NAMESPACE:
                grant = NamespaceGrant(
                    type=Subcommands.NAMESPACE,
                    namespace=self.namespace,
                    privilege=NamespacePrivilege(self.privilege),
                )
            elif self.privileges_subcommand == Subcommands.TABLE:
                grant = TableGrant(
                    type=Subcommands.TABLE,
                    namespace=self.namespace,
                    table_name=self.table,
                    privilege=TablePrivilege(self.privilege),
                )
            elif self.privileges_subcommand == Subcommands.VIEW:
                grant = ViewGrant(
                    type=Subcommands.VIEW,
                    namespace=self.namespace,
                    view_name=self.view,
                    privilege=ViewPrivilege(self.privilege),
                )

            if not grant:
                raise Exception(
                    f"{self.privileges_subcommand} is not supported in the CLI"
                )
            elif self.action == Actions.GRANT:
                request = AddGrantRequest(grant=grant)
                api.add_grant_to_catalog_role(
                    self.catalog_name, self.catalog_role_name, request
                )
            elif self.action == Actions.REVOKE:
                request = RevokeGrantRequest(grant=grant)
                api.revoke_grant_from_catalog_role(
                    self.catalog_name, self.catalog_role_name, self.cascade, request
                )
            else:
                raise Exception(f"{self.action} is not supported in the CLI")
