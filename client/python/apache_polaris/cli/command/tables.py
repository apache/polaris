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

from apache_polaris.cli.command import Command
from apache_polaris.cli.command.utils import get_catalog_api_client
from apache_polaris.cli.constants import Subcommands, Arguments, UNIT_SEPARATOR
from apache_polaris.cli.options.option_tree import Argument
from apache_polaris.sdk.catalog import IcebergCatalogAPI
from apache_polaris.sdk.management import PolarisDefaultApi
from apache_polaris.sdk.catalog.api.policy_api import PolicyAPI
from apache_polaris.cli.command.utils import (
    handle_api_exception,
    format_timestamp,
    format_iceberg_type,
)
from prettytable import PrettyTable


@dataclass
class TableCommand(Command):
    """
    A Command implemtation to represent `polaris tables`. It manages Iceberg tables within a Polaris Catalog.

    Example commands:
        * ./polaris tables list --catalog my_catalog --namespace ns1
        * ./polaris tables get my_table --catalog my_catalog --namespace ns1
        * ./polaris tables summarize my_table --catalog my_catalog --namespace ns1
        * ./polaris tables delete my_table --catalog my_catalog --namesapce ns1
    """

    table_subcommand: str
    catalog_name: str
    namespace: List[str]
    table_name: str

    def validate(self) -> None:
        if not self.catalog_name:
            raise Exception(
                f"Missing required argument: {Argument.to_flag_name(Arguments.CATALOG)}"
            )
        if not self.namespace:
            raise Exception(
                f"Missing required argument: {Argument.to_flag_name(Arguments.NAMESPACE)}"
            )
        if (
            self.table_subcommand == Subcommands.GET
            or self.table_subcommand == Subcommands.SUMMARIZE
            or self.table_subcommand == Subcommands.DELETE
        ):
            if not self.table_name.strip():
                raise Exception("The table name cannot be empty.")

    def execute(self, api: PolarisDefaultApi) -> None:
        catalog_api = IcebergCatalogAPI(get_catalog_api_client(api))
        ns_str = UNIT_SEPARATOR.join(self.namespace)
        if self.table_subcommand == Subcommands.LIST:
            try:
                result = catalog_api.list_tables(
                    prefix=self.catalog_name, namespace=ns_str
                )
                for table_identifier in result.identifiers:
                    print(table_identifier.to_json())
            except Exception as e:
                handle_api_exception(f"Table Listing ({'.'.join(self.namespace)})", e)
        elif self.table_subcommand == Subcommands.GET:
            try:
                print(
                    catalog_api.load_table(
                        prefix=self.catalog_name,
                        namespace=ns_str,
                        table=self.table_name,
                    ).to_json()
                )
            except Exception as e:
                handle_api_exception(f"Table Load ({self.table_name})", e)
        elif self.table_subcommand == Subcommands.DELETE:
            print(
                f"De-registering table {'.'.join(self.namespace)}.{self.table_name}..."
            )
            try:
                catalog_api.drop_table(
                    prefix=self.catalog_name,
                    namespace=ns_str,
                    table=self.table_name,
                    purge_requested=False,
                )
                print(
                    f"De-registering table {'.'.join(self.namespace)}.{self.table_name} completed"
                )
            except Exception as e:
                handle_api_exception(f"Table De-registration ({self.table_name})", e)
        elif self.table_subcommand == Subcommands.SUMMARIZE:
            self._generate_summary(api, catalog_api, ns_str)

    def _generate_summary(
        self, api: PolarisDefaultApi, catalog_api: IcebergCatalogAPI, ns_str: str
    ) -> None:
        print(f"Table: {'.'.join(self.namespace)}.{self.table_name}")
        print("-" * 80)
        try:
            resp = catalog_api.load_table(
                prefix=self.catalog_name, namespace=ns_str, table=self.table_name
            )
            # Metadata
            metadata = resp.metadata
            print("Metadata")
            print(f"  {'Location:':<30} {metadata.location}")
            print(f"  {'Format Version:':<30} {metadata.format_version}")
            print(f"  {'Snapshots:':<30} {len(metadata.snapshots)}")
            print(f"  {'Current Snapshot ID:':<30} {metadata.current_snapshot_id}")
            print(
                f"  {'Last Updated:':<30} {format_timestamp(metadata.last_updated_ms)}"
            )

            # Statistics
            print("\nStatistics")
            current_snapshot = next(
                (
                    snapshot
                    for snapshot in metadata.snapshots
                    if snapshot.snapshot_id == metadata.current_snapshot_id
                ),
                None,
            )
            if current_snapshot and current_snapshot.summary:
                stats = current_snapshot.summary.model_dump().get(
                    "additional_properties", {}
                )
                print(f"  {'Total Records:':<30} {stats.get('total-records', '0')}")
                print(
                    f"  {'Total Data Files:':<30} {stats.get('total-data-files', '0')}"
                )
                print(
                    f"  {'Total Files Size:':<30} {stats.get('total-files-size', '0')}"
                )
            else:
                print("  Table is empty (no snapshots found)")

            # Schema
            print("\nSchema")
            current_schema = next(
                (
                    schema
                    for schema in metadata.schemas
                    if schema.schema_id == metadata.current_schema_id
                ),
                None,
            )
            if current_schema and current_schema.fields:
                table = PrettyTable(
                    field_names=["ID", "Field Name", "Type", "Required", "Comment"],
                    align="l",
                )
                for field in current_schema.fields:
                    required = "*" if field.required else ""
                    type_str = format_iceberg_type(field.type)
                    column_comment = field.doc if getattr(field, "doc", None) else ""
                    table.add_row(
                        [field.id, field.name, type_str, required, column_comment]
                    )
                indented_table = "\n".join(
                    " " * 2 + line for line in table.get_string().splitlines()
                )
                print(indented_table)
            else:
                print("  No schema information available")

            # Partitioning
            print("\nPartitioning")
            current_spec = next(
                (
                    spec
                    for spec in metadata.partition_specs
                    if spec.spec_id == metadata.default_spec_id
                ),
                None,
            )
            if current_spec and current_spec.fields:
                table = PrettyTable(
                    field_names=["Source ID", "Field Name", "Transform"], align="l"
                )
                for field in current_spec.fields:
                    table.add_row([field.source_id, field.name, field.transform])
                indented_table = "\n".join(
                    " " * 2 + line for line in table.get_string().splitlines()
                )
                print(indented_table)
            else:
                print("  Table is un-partitioned.")

            # Sort Order
            print("\nSort order")
            current_sort_order = next(
                (
                    sort_order
                    for sort_order in metadata.sort_orders
                    if sort_order.order_id == metadata.default_sort_order_id
                ),
                None,
            )
            if current_sort_order and current_sort_order.fields:
                table = PrettyTable(
                    field_names=["Source ID", "Transform", "Null Order", "Direction"],
                    align="l",
                )
                for field in current_sort_order.fields:
                    table.add_row(
                        [
                            field.source_id,
                            field.transform,
                            field.null_order.value,
                            field.direction.value,
                        ]
                    )
                indented_table = "\n".join(
                    " " * 2 + line for line in table.get_string().splitlines()
                )
                print(indented_table)
            else:
                print("  Table is un-sorted.")
        except Exception as e:
            handle_api_exception("Table Metadata", e)

        # Effective policies
        print("\nEffective policies")
        try:
            policy_api = PolicyAPI(catalog_api.api_client)
            resp = policy_api.get_applicable_policies(
                prefix=self.catalog_name, namespace=ns_str, target_name=self.table_name
            )
            applicable_policies = resp.applicable_policies or []
            if applicable_policies:
                for policy in sorted(applicable_policies, key=lambda x: x.name):
                    source = "Direct"
                    if policy.inherited:
                        target = (
                            ".".join(policy.namespace)
                            if policy.namespace
                            else "Catalog"
                        )
                        source = f"Inherited from {target}"
                    print(f"  - {policy.name} ({source})")
            else:
                print("  No policies apply to this table")
        except Exception as e:
            handle_api_exception("Table Policies", e)
        print("-" * 80)
