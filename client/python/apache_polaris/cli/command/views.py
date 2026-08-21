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
from apache_polaris.cli.command.utils import (
    format_iceberg_type,
    format_timestamp,
    get_catalog_api_client,
    handle_api_exception,
)
from apache_polaris.cli.exceptions import CliError
from apache_polaris.cli.constants import Subcommands, Arguments, UNIT_SEPARATOR
from apache_polaris.cli.options.option_tree import Argument
from apache_polaris.sdk.catalog import IcebergCatalogAPI
from apache_polaris.sdk.management import PolarisDefaultApi
from prettytable import PrettyTable


@dataclass
class ViewCommand(Command):
    """
    A Command implementation to represent `polaris views`. It manages Iceberg views within a Polaris Catalog.

    Example commands:
        * polaris views list --catalog my_catalog --namespace ns1
        * polaris views get my_view --catalog my_catalog --namespace ns1
        * polaris views summarize my_view --catalog my_catalog --namespace ns1
        * polaris views delete my_view --catalog my_catalog --namespace ns1
    """

    views_subcommand: str
    catalog_name: Optional[str] = None
    namespace: Optional[List[str]] = field(default_factory=list)
    view_name: Optional[str] = None

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
            self.views_subcommand == Subcommands.GET
            or self.views_subcommand == Subcommands.SUMMARIZE
            or self.views_subcommand == Subcommands.DELETE
        ):
            if not self.view_name or not self.view_name.strip():
                raise CliError("The view name cannot be empty.")

    def execute(self, api: PolarisDefaultApi) -> None:
        catalog_api = IcebergCatalogAPI(get_catalog_api_client(api))
        catalog_name = cast(str, self.catalog_name)
        namespace_list = cast(List[str], self.namespace)
        view_name = cast(str, self.view_name)
        ns_str = UNIT_SEPARATOR.join(namespace_list)

        if self.views_subcommand == Subcommands.LIST:
            result = catalog_api.list_views(prefix=catalog_name, namespace=ns_str)
            for view_identifier in result.identifiers:
                print(view_identifier.to_json())
        elif self.views_subcommand == Subcommands.GET:
            print(
                catalog_api.load_view(
                    prefix=catalog_name,
                    namespace=ns_str,
                    view=view_name,
                ).to_json()
            )
        elif self.views_subcommand == Subcommands.DELETE:
            namespace_dot = ".".join(namespace_list)
            print(f"Dropping view {namespace_dot}.{view_name}...")
            catalog_api.drop_view(
                prefix=catalog_name,
                namespace=ns_str,
                view=view_name,
            )
            print(f"Dropping view {namespace_dot}.{view_name} completed")
        elif self.views_subcommand == Subcommands.SUMMARIZE:
            self._generate_summary(catalog_api, ns_str)

    def _generate_summary(self, catalog_api: IcebergCatalogAPI, ns_str: str) -> None:
        catalog_name = cast(str, self.catalog_name)
        namespace_list = cast(List[str], self.namespace)
        view_name = cast(str, self.view_name)

        namespace_dot = ".".join(namespace_list)
        print(f"View: {namespace_dot}.{view_name}")
        print("-" * 80)
        try:
            resp = catalog_api.load_view(
                prefix=catalog_name, namespace=ns_str, view=view_name
            )
            # Metadata
            metadata = resp.metadata
            current_version = next(
                (
                    v
                    for v in metadata.versions
                    if v.version_id == metadata.current_version_id
                ),
                None,
            )
            if current_version is None:
                print("  No matching version found for the current version ID")
            else:
                print("Metadata")
                print(f"  {'Location:':<30} {metadata.location}")
                print(f"  {'Format Version:':<30} {metadata.format_version}")
                print(f"  {'Current Version ID:':<30} {metadata.current_version_id}")
                print(
                    f"  {'Last Updated:':<30} {format_timestamp(current_version.timestamp_ms)}"
                )

                # SQL representations
                print("\nRepresentations")
                for representation in current_version.representations:
                    unwrapped = representation.actual_instance
                    if unwrapped is None:
                        continue
                    print(f"  {'Dialect:':<30} {unwrapped.dialect}")
                    indented_sql = "\n".join(
                        " " * 4 + line for line in unwrapped.sql.splitlines()
                    )
                    print("  SQL:")
                    print(indented_sql)

                # Schema
                print("\nSchema")
                current_schema = next(
                    (
                        schema
                        for schema in metadata.schemas
                        if schema.schema_id == current_version.schema_id
                    ),
                    None,
                )
                if current_schema and current_schema.fields:
                    table = PrettyTable(
                        field_names=["ID", "Field Name", "Type", "Comment"],
                        align="l",
                    )
                    for field in current_schema.fields:
                        type_str = format_iceberg_type(field.type)
                        column_comment = field.doc or ""
                        table.add_row([field.id, field.name, type_str, column_comment])
                    indented_table = "\n".join(
                        " " * 2 + line for line in table.get_string().splitlines()
                    )
                    print(indented_table)
                else:
                    print("  No schema information available")

                # Version history
                print("\nVersion History")
                table = PrettyTable(field_names=["Version ID", "Timestamp"], align="l")
                for entry in metadata.version_log:
                    table.add_row(
                        [entry.version_id, format_timestamp(entry.timestamp_ms)]
                    )
                indented_table = "\n".join(
                    " " * 2 + line for line in table.get_string().splitlines()
                )
                print(indented_table)
        except Exception as e:
            handle_api_exception("View Metadata", e)
        print("-" * 80)
