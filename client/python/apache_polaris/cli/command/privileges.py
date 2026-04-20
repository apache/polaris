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
from typing import List, Optional, cast

from pydantic import StrictStr

from apache_polaris.cli.command import Command
from apache_polaris.cli.constants import Subcommands, Actions, Arguments
from apache_polaris.cli.options.option_tree import Argument
from apache_polaris.sdk.management import (
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
    A Command implementation to represent `polaris privileges`. The instance attributes correspond to parameters
    that can be provided to various subcommands, except `privileges_subcommand` which represents the subcommand
    itself.

    Example commands:
        * ./polaris privileges table grant --catalog c --catalog-role cr --namespace n --table t PRIVILEGE_NAME
        * ./polaris privileges namespace revoke --catalog c --catalog-role cr --namespace n PRIVILEGE_NAME
        * ./polaris privileges list --catalog c --catalog-role cr
    """

    privileges_subcommand: str
    action: Optional[str] = None
    catalog_name: Optional[str] = None
    catalog_role_name: Optional[str] = None
    namespace: Optional[List[StrictStr]] = None
    view: Optional[str] = None
    table: Optional[str] = None
    privilege: Optional[str] = None
    cascade: bool = False

    def validate(self) -> None:
        if not self.catalog_name:
            raise Exception(
                f"Missing required argument: {Argument.to_flag_name(Arguments.CATALOG)}"
            )
        if not self.catalog_role_name:
            raise Exception(
                f"Missing required argument: {Argument.to_flag_name(Arguments.CATALOG_ROLE)}"
            )

        if self.privileges_subcommand != Subcommands.LIST:
            if not self.privilege:
                raise Exception(
                    f"Missing required argument: {Argument.to_flag_name(Arguments.PRIVILEGE)}"
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
        catalog_name = cast(str, self.catalog_name)
        role_name = cast(str, self.catalog_role_name)
        namespace = cast(List[str], self.namespace)

        if self.privileges_subcommand == Subcommands.LIST:
            for grant in api.list_grants_for_catalog_role(
                catalog_name, role_name
            ).grants:
                print(grant.to_json())
        else:
            privilege_name = cast(str, self.privilege)
            grant = None
            if self.privileges_subcommand == Subcommands.CATALOG:
                grant = CatalogGrant(
                    type=Subcommands.CATALOG, privilege=CatalogPrivilege(privilege_name)
                )
            elif self.privileges_subcommand == Subcommands.NAMESPACE:
                grant = NamespaceGrant(
                    type=Subcommands.NAMESPACE,
                    namespace=namespace,
                    privilege=NamespacePrivilege(privilege_name),
                )
            elif self.privileges_subcommand == Subcommands.TABLE:
                grant = TableGrant(
                    type=Subcommands.TABLE,
                    namespace=namespace,
                    table_name=self.table,
                    privilege=TablePrivilege(privilege_name),
                )
            elif self.privileges_subcommand == Subcommands.VIEW:
                grant = ViewGrant(
                    type=Subcommands.VIEW,
                    namespace=namespace,
                    view_name=self.view,
                    privilege=ViewPrivilege(privilege_name),
                )

            if not grant:
                raise Exception(
                    f"{self.privileges_subcommand} is not supported in the CLI"
                )
            elif self.action == Actions.GRANT:
                request = AddGrantRequest(grant=grant)
                api.add_grant_to_catalog_role(catalog_name, role_name, request)
            elif self.action == Actions.REVOKE:
                request = RevokeGrantRequest(grant=grant)
                api.revoke_grant_from_catalog_role(
                    catalog_name, role_name, self.cascade, request
                )
            else:
                raise Exception(f"{self.action} is not supported in the CLI")
