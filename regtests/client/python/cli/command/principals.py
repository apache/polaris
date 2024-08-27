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
from typing import Dict, Optional

from pydantic import StrictStr

from cli.command import Command
from cli.constants import Subcommands
from polaris.management import PolarisDefaultApi, CreatePrincipalRequest, Principal, UpdatePrincipalRequest


@dataclass
class PrincipalsCommand(Command):
    """
    A Command implementation to represent `polaris principals`. The instance attributes correspond to parameters
    that can be provided to various subcommands, except `principals_subcommand` which represents the subcommand
    itself.

    Example commands:
        * ./polaris principals create user
        * ./polaris principals list
        * ./polaris principals list --principal-role filter-to-this-role
    """

    principals_subcommand: str
    type: str
    principal_name: str
    client_id: str
    principal_role: str
    properties: Optional[Dict[str, StrictStr]]

    def validate(self):
        pass

    def execute(self, api: PolarisDefaultApi) -> None:
        if self.principals_subcommand == Subcommands.CREATE:
            request = CreatePrincipalRequest(
                principal=Principal(
                    type=self.type.upper(),
                    name=self.principal_name,
                    client_id=self.client_id,
                    properties=self.properties
                )
            )
            print(api.create_principal(request).credentials.to_json())
        elif self.principals_subcommand == Subcommands.DELETE:
            api.delete_principal(self.principal_name)
        elif self.principals_subcommand == Subcommands.GET:
            print(api.get_principal(self.principal_name).to_json())
        elif self.principals_subcommand == Subcommands.LIST:
            if self.principal_role:
                for principal in api.list_assignee_principals_for_principal_role(self.principal_role).principals:
                    print(principal.to_json())
            else:
                for principal in api.list_principals().principals:
                    print(principal.to_json())
        elif self.principals_subcommand == Subcommands.ROTATE_CREDENTIALS:
            print(api.rotate_credentials(self.principal_name).to_json())
        elif self.principals_subcommand == Subcommands.UPDATE:
            principal = api.get_principal(self.principal_name)
            request = UpdatePrincipalRequest(
                current_entity_version=principal.current_entity_version,
                properties=self.properties
            )
            api.update_principal(self.principal_name, request)
        else:
            raise Exception(f"{self.principals_subcommand} is not supported in the CLI")
