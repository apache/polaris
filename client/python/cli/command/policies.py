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
from typing import List, Optional, Dict

from cli.command import Command
from cli.constants import Subcommands, Arguments
from polaris.management import PolarisDefaultApi
from polaris.catalog.api_client import ApiClient as CatalogApiClient

from polaris.catalog.configuration import Configuration as CatalogConfiguration
from polaris.catalog.api.policy_api import PolicyAPI
from polaris.catalog.models.create_policy_request import CreatePolicyRequest
from polaris.catalog.models.update_policy_request import UpdatePolicyRequest
from polaris.catalog.models.policy_attachment_target import PolicyAttachmentTarget
from polaris.catalog.models.attach_policy_request import AttachPolicyRequest
from polaris.catalog.models.detach_policy_request import DetachPolicyRequest



@dataclass
class PoliciesCommand(Command):
    """
    A Command implementation to represent `polaris policies`.
    """

    policies_subcommand: str
    catalog_name: str
    namespace: str
    policy_name: str
    policy_file: str
    policy_type: str
    policy_description: Optional[str]
    target_name: Optional[str]
    parameters: Optional[Dict[str, str]]
    detach_all: Optional[bool]
    applicable: Optional[bool]
    attach_target: str



    def validate(self):
        if not self.catalog_name:
            raise Exception(f"Missing required argument: {Arguments.CATALOG}")
        if self.policies_subcommand in [Subcommands.CREATE, Subcommands.UPDATE]:
            if not self.policy_file:
                raise Exception(f"Missing required argument: {Arguments.POLICY_FILE}")
        if self.policies_subcommand in [Subcommands.ATTACH, Subcommands.DETACH]:
            if not self.attach_target:
                raise Exception(f"Missing required argument: {Arguments.ATTACH_TARGET}")

    def execute(self, api: PolarisDefaultApi) -> None:
        mgmt_config = api.api_client.configuration
        catalog_host = mgmt_config.host.replace("/management/v1", "/catalog")

        catalog_config = CatalogConfiguration(
            host=catalog_host,
            access_token=mgmt_config.access_token,
        )
        catalog_config.proxy = mgmt_config.proxy
        catalog_config.proxy_headers = mgmt_config.proxy_headers

        catalog_api_client = CatalogApiClient(catalog_config)
        policy_api = PolicyAPI(catalog_api_client)

        namespace_str = None
        if self.namespace:
            namespace_list = self.namespace.split('.')
            namespace_str = "\x1F".join(namespace_list)
        if self.policies_subcommand == Subcommands.CREATE:
            with open(self.policy_file, "r") as f:
                policy = json.load(f)
            policy_api.create_policy(
                prefix=self.catalog_name,
                namespace=namespace_str,
                create_policy_request=CreatePolicyRequest(
                    name=self.policy_name,
                    type=self.policy_type,
                    description=self.policy_description,
                    content=json.dumps(policy)
                )
            )
        elif self.policies_subcommand == Subcommands.DELETE:
            policy_api.drop_policy(
                prefix=self.catalog_name,
                namespace=namespace_str,
                policy_name=self.policy_name,
                detach_all=self.detach_all
            )
        elif self.policies_subcommand == Subcommands.GET:
            print(policy_api.load_policy(
                prefix=self.catalog_name,
                namespace=namespace_str,
                policy_name=self.policy_name
            ).to_json())
        elif self.policies_subcommand == Subcommands.LIST:
            if self.applicable:
                api_namespace = None
                api_target_name = None
                applicable_policies_list = []

                if self.target_name:
                    if not self.namespace:
                        raise Exception("Missing required argument: --namespace when --target-name is set.")
                    api_namespace = namespace_str
                    api_target_name = self.target_name
                    applicable_policies_list = policy_api.get_applicable_policies(
                        prefix=self.catalog_name,
                        namespace=api_namespace,
                        target_name=api_target_name
                    ).applicable_policies
                elif self.namespace:
                    api_namespace = namespace_str
                    api_target_name = None

                    # Get policies applicable to the namespace (inherited)
                    applicable_policies_list = policy_api.get_applicable_policies(
                        prefix=self.catalog_name,
                        namespace=api_namespace,
                        target_name=api_target_name
                    ).applicable_policies

                    # Get policies defined directly in the namespace
                    defined_policies_response = policy_api.list_policies(
                        prefix=self.catalog_name,
                        namespace=namespace_str
                    ).identifiers

                    combined_policies = []
                    # Add applicable policies (which are full Policy objects)
                    for policy in applicable_policies_list:
                        combined_policies.append(policy.to_json())

                    # Add defined policies (which are PolicyIdentifier objects)
                    # Need to load each defined policy to get its full details
                    for policy_identifier in defined_policies_response:
                        # Check if this policy is already in applicable_policies by name
                        # This is a simplification; a more robust check might compare full policy content
                        is_duplicate = False
                        for existing_policy_json in combined_policies:
                            existing_policy = json.loads(existing_policy_json)
                            if existing_policy.get("name") == policy_identifier.name:
                                is_duplicate = True
                                break
                        if not is_duplicate:
                            loaded_policy = policy_api.load_policy(
                                prefix=self.catalog_name,
                                namespace=namespace_str,
                                policy_name=policy_identifier.name
                            ).policy
                            if loaded_policy:
                                combined_policies.append(loaded_policy.to_json())

                    for policy_json in combined_policies:
                        print(policy_json)
                    return # Exit after printing combined policies
                else:
                    # Catalog-level policies
                    api_namespace = None
                    api_target_name = None
                    applicable_policies_list = policy_api.get_applicable_policies(
                        prefix=self.catalog_name,
                        namespace=api_namespace,
                        target_name=api_target_name
                    ).applicable_policies

                for policy in applicable_policies_list:
                    print(policy.to_json())
        elif self.policies_subcommand == Subcommands.UPDATE:
            with open(self.policy_file, "r") as f:
                policy_document = json.load(f)

            # Fetch the current policy to get its version
            loaded_policy_response = policy_api.load_policy(
                prefix=self.catalog_name,
                namespace=namespace_str,
                policy_name=self.policy_name
            )
            if loaded_policy_response and loaded_policy_response.policy:
                current_policy_version = loaded_policy_response.policy.version
            else:
                raise Exception(f"Could not retrieve current policy version for {self.policy_name}")

            policy_api.update_policy(
                prefix=self.catalog_name,
                namespace=namespace_str,
                policy_name=self.policy_name,
                update_policy_request=UpdatePolicyRequest(
                    description=self.policy_description,
                    content=json.dumps(policy_document),
                    current_policy_version=current_policy_version
                )
            )
        elif self.policies_subcommand == Subcommands.ATTACH:
            target_parts = self.attach_target.split(":")
            if len(target_parts) != 2:
                raise Exception("Invalid attach target format. Expected 'type:name'")
            target_type, target_name = target_parts

            attachment_type = target_type
            if target_type in ["table", "view"]:
                attachment_type = "table-like"

            attachment_path = []
            if target_type == "catalog":
                attachment_path = []
            else:
                attachment_path = target_name.split(".")

            policy_api.attach_policy(
                prefix=self.catalog_name,
                namespace=namespace_str,
                policy_name=self.policy_name,
                attach_policy_request=AttachPolicyRequest(
                    target=PolicyAttachmentTarget(
                        type=attachment_type,
                        path=attachment_path
                    ),
                    parameters=self.parameters
                )
            )
        elif self.policies_subcommand == Subcommands.DETACH:
            target_parts = self.attach_target.split(":")
            if len(target_parts) != 2:
                raise Exception("Invalid attach target format. Expected 'type:name'")
            target_type, target_name = target_parts

            attachment_type = target_type
            if target_type in ["table", "view"]:
                attachment_type = "table-like"

            attachment_path = []
            if target_type == "catalog":
                attachment_path = []
            else:
                attachment_path = target_name.split(".")

            policy_api.detach_policy(
                prefix=self.catalog_name,
                namespace=namespace_str,
                policy_name=self.policy_name,
                detach_policy_request=DetachPolicyRequest(
                    target=PolicyAttachmentTarget(
                        type=attachment_type,
                        path=attachment_path
                    ),
                    parameters=self.parameters
                )
            )
        else:
            raise Exception(f"{self.policies_subcommand} is not supported in the CLI")
