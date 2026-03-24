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
from typing import List

from apache_polaris.cli.command import Command
from apache_polaris.cli.command.utils import get_catalog_api_client
from apache_polaris.cli.constants import Subcommands, Arguments, UNIT_SEPARATOR
from apache_polaris.cli.options.option_tree import Argument
from apache_polaris.sdk.catalog import IcebergCatalogAPI
from apache_polaris.sdk.management import PolarisDefaultApi
from apache_polaris.sdk.catalog.api.policy_api import PolicyAPI
from apache_polaris.cli.command.utils import handle_api_exception, format_timestamp


@dataclass
class TableCommand(Command):
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

    def execute(self, api: PolarisDefaultApi) -> None:
        catalog_api = IcebergCatalogAPI(get_catalog_api_client(api))
        if self.table_subcommand == Subcommands.LIST:
            ns_str = UNIT_SEPARATOR.join(self.namespace)
            result = catalog_api.list_tables(prefix=self.catalog_name, namespace=ns_str)
            for table_identifier in result.identifiers:
                print(
                    json.dumps(
                        {
                            "namespace": ".".join(table_identifier.namespace),
                            "name": table_identifier.name,
                        }
                    )
                )
        elif self.table_subcommand == Subcommands.GET:
            ns_str = UNIT_SEPARATOR.join(self.namespace)
            print(
                catalog_api.load_table(
                    prefix=self.catalog_name, namespace=ns_str, table=self.table_name
                ).to_json()
            )
        elif self.table_subcommand == Subcommands.DELETE:
            self._delete_table(catalog_api)
        elif self.table_subcommand == Subcommands.SUMMARIZE:
            self._generate_summary(api, catalog_api)

    def _delete_table(self, catalog_api: IcebergCatalogAPI) -> None:
        ns_str = UNIT_SEPARATOR.join(self.namespace)
        print(f"De-registering table {'.'.join(self.namespace)}.{self.table_name}...")
        try:
            catalog_api.drop_table(
                prefix=self.catalog_name,
                namespace=ns_str,
                table=self.table_name,
                purge_requested=False,
            )
        except Exception as e:
            raise Exception(f"Failed to de-register table: {e}")

    def _generate_summary(
        self, api: PolarisDefaultApi, catalog_api: IcebergCatalogAPI
    ) -> None:
        ns_str = UNIT_SEPARATOR.join(self.namespace)
        print(f"Table {'.'.join(self.namespace)}.{self.table_name}")
        print("-" * 80)
        # Metadata:
        try:
            resp = catalog_api.load_table(
                prefix=self.catalog_name, namespace=ns_str, table=self.table_name
            )
            metadata = resp.metadata
            print("Metadata")
            print(f" {'Location:':<30} {metadata.location}")
            print(f" {'Format Version:':<30} {metadata.format_version}")
            print(f" {'Current Schema ID:':<30} {metadata.current_schema_id}")
            print(f" {'Current Snapshot ID:':<30} {metadata.current_snapshot_id}")
            print(f" {'Snapshots:':<30} {len(metadata.snapshots)}")
            print(
                f" {'Last updated:':<30} {format_timestamp(metadata.last_updated_ms)}"
            )
        except Exception as e:
            handle_api_exception("Table Metadata", e)
        # Effective policies
        print("Effective policies")
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
                            if policy.self.namespace
                            else "Catalog"
                        )
                        source = f"Inherited from {target}"
                    print(f"  - {policy.name} ({source})")
            else:
                print(". no policies apply to this table")
        except Exception as e:
            handle_api_exception("Table Policies", e)
        print("-" * 80)
