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
from typing import List, Optional, cast

from apache_polaris.cli.command import Command
from apache_polaris.cli.command.utils import get_catalog_api_client
from apache_polaris.cli.exceptions import CliError
from apache_polaris.cli.constants import Subcommands, Arguments, UNIT_SEPARATOR
from apache_polaris.cli.options.option_tree import Argument
from apache_polaris.sdk.catalog.api.generic_table_api import GenericTableAPI
from apache_polaris.sdk.management import PolarisDefaultApi


@dataclass
class GenericTableCommand(Command):
    """
    A Command implementation to represent `polaris generic-tables`. It manages generic tables within a Polaris Catalog.

    Example commands:
        * polaris generic-tables list --catalog my_catalog --namespace ns1
        * polaris generic-tables get my_table --catalog my_catalog --namespace ns1
        * polaris generic-tables delete my_table --catalog my_catalog --namespace ns1
    """

    generic_tables_subcommand: str
    catalog_name: Optional[str] = None
    namespace: Optional[List[str]] = field(default_factory=list)
    generic_table_name: Optional[str] = None

    def validate(self) -> None:
        if not self.catalog_name:
            raise CliError(
                f"Missing required argument: {Argument.to_flag_name(Arguments.CATALOG)}"
            )
        if not self.namespace:
            raise CliError(
                f"Missing required argument: {Argument.to_flag_name(Arguments.NAMESPACE)}"
            )
        if (
            self.generic_tables_subcommand == Subcommands.GET
            or self.generic_tables_subcommand == Subcommands.DELETE
        ):
            if not self.generic_table_name or not self.generic_table_name.strip():
                raise CliError("The generic table name cannot be empty.")

    def execute(self, api: PolarisDefaultApi) -> None:
        generic_api = GenericTableAPI(get_catalog_api_client(api))
        catalog_name = cast(str, self.catalog_name)
        namespace_list = cast(List[str], self.namespace)
        generic_table_name = cast(str, self.generic_table_name)
        ns_str = UNIT_SEPARATOR.join(namespace_list)

        if self.generic_tables_subcommand == Subcommands.LIST:
            result = generic_api.list_generic_tables(
                prefix=catalog_name, namespace=ns_str
            )
            for table_identifier in result.identifiers:
                print(table_identifier.to_json())
        elif self.generic_tables_subcommand == Subcommands.GET:
            print(
                generic_api.load_generic_table(
                    prefix=catalog_name,
                    namespace=ns_str,
                    generic_table=generic_table_name,
                ).to_json()
            )
        elif self.generic_tables_subcommand == Subcommands.DELETE:
            namespace_dot = ".".join(namespace_list)
            print(f"Dropping generic table {namespace_dot}.{generic_table_name}...")
            generic_api.drop_generic_table(
                prefix=catalog_name,
                namespace=ns_str,
                generic_table=generic_table_name,
            )
            print(
                f"Dropping generic table {namespace_dot}.{generic_table_name} completed"
            )
