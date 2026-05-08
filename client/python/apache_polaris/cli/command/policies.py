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
from typing import Optional, Dict, List, cast

from apache_polaris.cli.command import Command
from apache_polaris.cli.command.utils import get_catalog_api_client
from apache_polaris.cli.constants import Subcommands, Arguments, UNIT_SEPARATOR
from apache_polaris.cli.options.option_tree import Argument
from apache_polaris.sdk.catalog.api.policy_api import PolicyAPI
from apache_polaris.sdk.catalog.models.attach_policy_request import AttachPolicyRequest
from apache_polaris.sdk.catalog.models.create_policy_request import CreatePolicyRequest
from apache_polaris.sdk.catalog.models.detach_policy_request import DetachPolicyRequest
from apache_polaris.sdk.catalog.models.policy_attachment_target import (
    PolicyAttachmentTarget,
)
from apache_polaris.sdk.catalog.models.update_policy_request import UpdatePolicyRequest
from apache_polaris.sdk.management import PolarisDefaultApi


@dataclass
class PoliciesCommand(Command):
    """
    A Command implementation to represent `polaris policies`. The instance attributes correspond to parameters
    that can be provided to various subcommands, except `policies_subcommand` which represents the subcommand
    itself.

    Example commands:
        * ./polaris policies create --catalog some_catalog --namespace some.schema --policy-file some_policy.json --policy-type system.data-compaction some_policy
        * ./polaris policies list --catalog some_catalog --applicable
        * ./polaris policies attach --catalog some_catalog --namespace some.schema --attachment-type table-like --attachment-path some.schema.t some_table_policy
    """

    policies_subcommand: str
    catalog_name: Optional[str] = None
    namespace: Optional[str] = None
    policy_name: Optional[str] = None
    policy_file: Optional[str] = None
    policy_type: Optional[str] = None
    policy_description: Optional[str] = None
    target_name: Optional[str] = None
    parameters: Optional[Dict[str, str]] = field(default_factory=dict)
    detach_all: Optional[bool] = None
    applicable: Optional[bool] = None
    attachment_type: Optional[str] = None
    attachment_path: Optional[str] = None

    def __post_init__(self) -> None:
        if self.parameters is None:
            self.parameters = {}

    def validate(self) -> None:
        if not self.catalog_name:
            raise Exception(
                f"Missing required argument: {Argument.to_flag_name(Arguments.CATALOG)}"
            )

        if self.policies_subcommand in {
            Subcommands.CREATE,
            Subcommands.DELETE,
            Subcommands.GET,
            Subcommands.UPDATE,
            Subcommands.ATTACH,
            Subcommands.DETACH,
        }:
            if not self.policy_name:
                raise Exception(
                    f"Missing required argument: {Argument.to_flag_name(Arguments.POLICY)}"
                )

        if self.policies_subcommand in [Subcommands.CREATE, Subcommands.UPDATE]:
            if not self.policy_file:
                raise Exception(
                    f"Missing required argument: {Argument.to_flag_name(Arguments.POLICY_FILE)}"
                )
        if self.policies_subcommand in [Subcommands.ATTACH, Subcommands.DETACH]:
            if not self.attachment_type:
                raise Exception(
                    f"Missing required argument: {Argument.to_flag_name(Arguments.ATTACHMENT_TYPE)}"
                )
            if self.attachment_type != "catalog" and not self.attachment_path:
                raise Exception(
                    f"'{Argument.to_flag_name(Arguments.ATTACHMENT_PATH)}' is required when attachment type is not 'catalog'"
                )
        if (
            self.policies_subcommand == Subcommands.LIST
            and self.applicable
            and self.target_name
        ):
            if not self.namespace:
                raise Exception(
                    f"Missing required argument: {Argument.to_flag_name(Arguments.NAMESPACE)}"
                    f" when {Argument.to_flag_name(Arguments.TARGET_NAME)} is set."
                )
        if self.policies_subcommand == Subcommands.LIST and not self.applicable:
            if not self.namespace:
                raise Exception(
                    f"Missing required argument: {Argument.to_flag_name(Arguments.NAMESPACE)}"
                    f" when listing policies without {Argument.to_flag_name(Arguments.APPLICABLE)} flag."
                )

    def execute(self, api: PolarisDefaultApi) -> None:
        catalog_api_client = get_catalog_api_client(api)
        policy_api = PolicyAPI(catalog_api_client)

        catalog_name = cast(str, self.catalog_name)
        policy_name = cast(str, self.policy_name)

        namespace_str = (
            self.namespace.replace(".", UNIT_SEPARATOR) if self.namespace else ""
        )
        if self.policies_subcommand == Subcommands.CREATE:
            policy_file = cast(str, self.policy_file)
            with open(policy_file, "r") as f:
                policy = json.load(f)
            policy_api.create_policy(
                prefix=catalog_name,
                namespace=namespace_str,
                create_policy_request=CreatePolicyRequest(
                    name=policy_name,
                    type=self.policy_type,
                    description=self.policy_description,
                    content=json.dumps(policy),
                ),
            )
        elif self.policies_subcommand == Subcommands.DELETE:
            policy_api.drop_policy(
                prefix=catalog_name,
                namespace=namespace_str,
                policy_name=policy_name,
                detach_all=self.detach_all,
            )
        elif self.policies_subcommand == Subcommands.GET:
            print(
                policy_api.load_policy(
                    prefix=catalog_name,
                    namespace=namespace_str,
                    policy_name=policy_name,
                ).to_json()
            )
        elif self.policies_subcommand == Subcommands.LIST:
            if self.applicable:
                applicable_policies_list = []

                if self.target_name:
                    # Table-like level policies
                    applicable_policies_list = policy_api.get_applicable_policies(
                        prefix=catalog_name,
                        namespace=namespace_str,
                        target_name=self.target_name,
                        policy_type=self.policy_type,
                    ).applicable_policies
                elif self.namespace:
                    # Namespace level policies
                    applicable_policies_list = policy_api.get_applicable_policies(
                        prefix=catalog_name,
                        namespace=namespace_str,
                        policy_type=self.policy_type,
                    ).applicable_policies
                else:
                    # Catalog level policies
                    applicable_policies_list = policy_api.get_applicable_policies(
                        prefix=catalog_name, policy_type=self.policy_type
                    ).applicable_policies
                for policy in applicable_policies_list:
                    print(policy.to_json())
            else:
                # List all policy identifiers in the namespace
                policies_response = policy_api.list_policies(
                    prefix=catalog_name,
                    namespace=namespace_str,
                    policy_type=self.policy_type,
                ).to_json()
                print(policies_response)
        elif self.policies_subcommand == Subcommands.UPDATE:
            policy_file = cast(str, self.policy_file)
            with open(policy_file, "r") as f:
                policy_document = json.load(f)
            # Fetch the current policy to get its version
            loaded_policy_response = policy_api.load_policy(
                prefix=catalog_name,
                namespace=namespace_str,
                policy_name=policy_name,
            )
            if loaded_policy_response and loaded_policy_response.policy:
                current_policy_version = loaded_policy_response.policy.version
            else:
                raise Exception(
                    f"Could not retrieve current policy version for {policy_name}"
                )

            policy_api.update_policy(
                prefix=catalog_name,
                namespace=namespace_str,
                policy_name=policy_name,
                update_policy_request=UpdatePolicyRequest(
                    description=self.policy_description,
                    content=json.dumps(policy_document),
                    current_policy_version=current_policy_version,
                ),
            )
        elif self.policies_subcommand in [Subcommands.ATTACH, Subcommands.DETACH]:
            attachment_path_list: List[str] = []
            if self.attachment_type != "catalog" and self.attachment_path:
                attachment_path_list = self.attachment_path.split(".")

            if self.policies_subcommand == Subcommands.ATTACH:
                policy_api.attach_policy(
                    prefix=catalog_name,
                    namespace=namespace_str,
                    policy_name=policy_name,
                    attach_policy_request=AttachPolicyRequest(
                        target=PolicyAttachmentTarget(
                            type=self.attachment_type, path=attachment_path_list
                        ),
                        parameters=self.parameters
                        if isinstance(self.parameters, dict)
                        else None,
                    ),
                )
            else:
                policy_api.detach_policy(
                    prefix=catalog_name,
                    namespace=namespace_str,
                    policy_name=policy_name,
                    detach_policy_request=DetachPolicyRequest(
                        target=PolicyAttachmentTarget(
                            type=self.attachment_type, path=attachment_path_list
                        ),
                        parameters=self.parameters
                        if isinstance(self.parameters, dict)
                        else None,
                    ),
                )
        else:
            raise Exception(f"{self.policies_subcommand} is not supported in the CLI")
