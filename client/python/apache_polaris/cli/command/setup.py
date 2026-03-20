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
import os
import logging
import yaml
import json
from dataclasses import dataclass
from typing import Dict, Optional, List, Any, Set, cast

from apache_polaris.cli.command import Command
from apache_polaris.cli.constants import (
    PrincipalType,
    Subcommands,
    StorageType,
    UNIT_SEPARATOR,
    Actions,
    CatalogType,
    CatalogConnectionType,
    AuthenticationType,
    ServiceIdentityType,
)

from apache_polaris.sdk.management import PolarisDefaultApi
from apache_polaris.sdk.catalog import IcebergCatalogAPI
from apache_polaris.sdk.catalog.api.policy_api import PolicyAPI
from apache_polaris.sdk.catalog.exceptions import NotFoundException
from apache_polaris.sdk.catalog.models.create_policy_request import CreatePolicyRequest
from apache_polaris.cli.command.principals import PrincipalsCommand
from apache_polaris.cli.command.principal_roles import PrincipalRolesCommand
from apache_polaris.cli.command.catalogs import CatalogsCommand
from apache_polaris.cli.command.catalog_roles import CatalogRolesCommand
from apache_polaris.cli.command.namespaces import NamespacesCommand
from apache_polaris.cli.command.privileges import PrivilegesCommand
from apache_polaris.cli.command.policies import PoliciesCommand
from apache_polaris.cli.command.utils import get_catalog_api_client

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class SetupCommand(Command):
    setup_subcommand: str
    setup_config: Optional[str] = None
    dry_run: bool = False
    _config_cache: Optional[Dict[str, Any]] = None
    _existing_principal_roles: Optional[Set[str]] = None
    _existing_catalog_roles: Optional[Dict[str, Set[str]]] = None
    _existing_catalogs: Optional[Set[str]] = None
    _existing_principals: Optional[Set[str]] = None
    _catalog_api: Optional[Any] = None

    def _get_catalog_api(self, api: PolarisDefaultApi) -> Any:
        """Get or create and cache the IcebergCatalogAPI client."""
        if self._catalog_api is None:
            self._catalog_api = get_catalog_api_client(api)
        return self._catalog_api

    def _get_existing_principals(self, api: PolarisDefaultApi) -> Set[str]:
        """Fetch and cache the set of existing principal names."""
        if self._existing_principals is None:
            try:
                self._existing_principals = {
                    p.name for p in api.list_principals().principals
                }
            except Exception:
                logger.exception("Failed to fetch existing principals")
                self._existing_principals = set()
        return self._existing_principals

    def _get_existing_principal_roles(self, api: PolarisDefaultApi) -> Set[str]:
        """Fetch and cache the set of existing principal role names."""
        if self._existing_principal_roles is None:
            try:
                self._existing_principal_roles = {
                    role.name for role in api.list_principal_roles().roles
                }
            except Exception:
                logger.exception("Failed to fetch existing principal roles")
                self._existing_principal_roles = set()
        return self._existing_principal_roles

    def _get_existing_catalog_roles(
        self, api: PolarisDefaultApi, catalog_name: Optional[str] = None
    ) -> Set[str]:
        """Fetch and cache the set of existing catalog role names for a given catalog."""
        if self._existing_catalog_roles is None:
            self._existing_catalog_roles = {}

        if catalog_name:
            if catalog_name not in self._existing_catalog_roles:
                try:
                    roles = api.list_catalog_roles(catalog_name).roles
                    self._existing_catalog_roles[catalog_name] = {
                        role.name for role in roles
                    }
                except Exception as e:
                    # In dry-run, a 404 is expected if the catalog doesn't exist yet
                    is_404 = getattr(e, "status", None) == 404 or "(404)" in str(e)
                    if not (self.dry_run and is_404):
                        logger.warning(
                            f"Failed to fetch catalog roles for catalog '{catalog_name}': {e}"
                        )
                    self._existing_catalog_roles[catalog_name] = set()
            return self._existing_catalog_roles[catalog_name]
        return set()

    def _get_existing_catalogs(self, api: PolarisDefaultApi) -> Set[str]:
        """Fetch and cache the set of existing catalog names."""
        if self._existing_catalogs is None:
            try:
                self._existing_catalogs = {c.name for c in api.list_catalogs().catalogs}
            except Exception:
                logger.exception("Failed to fetch existing catalogs")
                self._existing_catalogs = set()
        return self._existing_catalogs

    def _export_config(self, api: PolarisDefaultApi) -> None:
        """Export the current Polaris configuration to YAML."""
        logger.info("--- Exporting Polaris Configuration ---")
        logger.warning(
            "Exporting policy attachments is not currently supported due to performance "
            "considerations. Attachments will be omitted from the exported configuration."
        )
        config = {
            "principals": self._export_principals(api),
            "principal_roles": self._export_principal_roles(api),
            "catalogs": self._export_catalogs(api),
        }
        print(yaml.safe_dump(config, sort_keys=False, indent=2).rstrip())
        logger.info("--- Finished Exporting Polaris Configuration ---")

    def _export_principals(self, api: PolarisDefaultApi) -> Dict[str, Any]:
        """Export all principals and their assigned roles."""
        principals_map: Dict[str, Any] = {}
        try:
            principals = sorted(api.list_principals().principals, key=lambda p: p.name)
            for p in principals:
                principal_info: Dict[str, Any] = {"type": PrincipalType.SERVICE.value}
                try:
                    assigned_roles = api.list_principal_roles_assigned(p.name).roles
                    if assigned_roles:
                        principal_info["roles"] = sorted(
                            [r.name for r in assigned_roles]
                        )
                except Exception:
                    logger.warning(f"Failed to export roles for principal '{p.name}'")
                principals_map[p.name] = principal_info
        except Exception:
            logger.exception("Failed to export principals")
        return principals_map

    def _export_principal_roles(self, api: PolarisDefaultApi) -> List[str]:
        """Export all principal role names."""
        try:
            return sorted([role.name for role in api.list_principal_roles().roles])
        except Exception:
            logger.exception("Failed to export principal roles")
            return []

    def _serialize_authentication_info(self, auth_params: Any) -> Dict[str, Any]:
        """Serializes authentication parameters into a dictionary for YAML export."""
        if not auth_params:
            return {}
        auth_type = auth_params.authentication_type.lower()
        auth_data = {"type": auth_type}
        if auth_type == AuthenticationType.OAUTH.value:
            auth_data.update(
                {
                    "token_uri": auth_params.token_uri,
                    "client_id": auth_params.client_id,
                    "client_secret": auth_params.client_secret.get_secret_value()
                    if auth_params.client_secret
                    else None,
                    "scopes": auth_params.scopes,
                }
            )
        elif auth_type == AuthenticationType.BEARER.value:
            auth_data["token"] = (
                auth_params.bearer_token.get_secret_value()
                if auth_params.bearer_token
                else None
            )
        elif auth_type == AuthenticationType.SIGV4.value:
            auth_data.update(
                {
                    "role_arn": auth_params.role_arn,
                    "role_session_name": auth_params.role_session_name,
                    "external_id": auth_params.external_id,
                    "signing_region": auth_params.signing_region,
                    "signing_name": auth_params.signing_name,
                }
            )
        # Filtered out None or empty values for cleaner YAML output
        filtered_auth_data = {
            k: v for k, v in auth_data.items() if v is not None and v != []
        }
        return {"authentication": filtered_auth_data} if filtered_auth_data else {}

    def _serialize_service_identity_info(self, identity_info: Any) -> Dict[str, Any]:
        """Serializes service identity info into a dictionary for YAML export."""
        if not identity_info:
            return {}
        identity_type = identity_info.identity_type.lower()
        identity_data = {"type": identity_type}
        if identity_type == ServiceIdentityType.AWS_IAM.value:
            identity_data["iam_arn"] = identity_info.iam_arn
        # Filtered out None or empty values for cleaner YAML output
        filtered_identity_data = {
            k: v for k, v in identity_data.items() if v is not None and v != []
        }
        return (
            {"service_identity": filtered_identity_data}
            if filtered_identity_data
            else {}
        )

    def _serialize_connection_info(self, conn_info: Any) -> Dict[str, Any]:
        """Serializes a connection_config_info object into a dictionary for YAML export."""
        if not conn_info:
            return {}
        conn_type_str = conn_info.connection_type.lower().replace("_", "-")
        conn_data = {
            "type": conn_type_str,
            "uri": conn_info.uri,
        }
        if conn_type_str == CatalogConnectionType.HADOOP.value:
            conn_data["warehouse"] = conn_info.warehouse
        elif conn_type_str == CatalogConnectionType.HIVE.value:
            conn_data["warehouse"] = conn_info.warehouse
        elif conn_type_str == CatalogConnectionType.ICEBERG.value:
            if (
                hasattr(conn_info, "remote_catalog_name")
                and conn_info.remote_catalog_name
            ):
                conn_data["remote_catalog_name"] = conn_info.remote_catalog_name
        if (
            hasattr(conn_info, "authentication_parameters")
            and conn_info.authentication_parameters
        ):
            conn_data.update(
                self._serialize_authentication_info(conn_info.authentication_parameters)
            )
        if hasattr(conn_info, "service_identity") and conn_info.service_identity:
            conn_data.update(
                self._serialize_service_identity_info(conn_info.service_identity)
            )
        # Filtered out None or empty values for cleaner YAML output
        filtered_conn_data = {
            k: v for k, v in conn_data.items() if v is not None and v != []
        }
        return {"connection": filtered_conn_data} if filtered_conn_data else {}

    def _export_catalogs(self, api: PolarisDefaultApi) -> List[Dict[str, Any]]:
        """Exports all catalogs and their nested entities into a list of dictionaries."""
        catalogs_list = []
        try:
            catalogs = sorted(api.list_catalogs().catalogs, key=lambda c: c.name)
            for c_summary in catalogs:
                c = api.get_catalog(c_summary.name)
                storage_type = c.storage_config_info.storage_type
                catalog_info = {
                    "name": c.name,
                    "type": c.type.lower() if c.type else "internal",
                    "storage_type": storage_type.lower(),
                    "default_base_location": c.properties.default_base_location,
                    "allowed_locations": sorted(
                        c.storage_config_info.allowed_locations
                    ),
                    "properties": c.properties.additional_properties
                    if c.properties.additional_properties
                    else {},
                }
                # Serialize storage config details
                if storage_type.lower() == StorageType.S3.value:
                    s3_info = {
                        "role_arn": c.storage_config_info.role_arn,
                        "external_id": c.storage_config_info.external_id,
                        "user_arn": c.storage_config_info.user_arn,
                        "region": c.storage_config_info.region,
                        "endpoint": c.storage_config_info.endpoint,
                        "sts_unavailable": c.storage_config_info.sts_unavailable,
                        "kms_unavailable": c.storage_config_info.kms_unavailable,
                        "path_style_access": c.storage_config_info.path_style_access,
                        "current_kms_key": c.storage_config_info.current_kms_key,
                        "allowed_kms_keys": sorted(
                            c.storage_config_info.allowed_kms_keys
                        )
                        if hasattr(c.storage_config_info, "allowed_kms_keys")
                        and c.storage_config_info.allowed_kms_keys
                        else [],
                    }
                    catalog_info.update(
                        {k: v for k, v in s3_info.items() if v is not None and v != []}
                    )
                elif storage_type.lower() == StorageType.AZURE.value:
                    azure_info = {
                        "tenant_id": c.storage_config_info.tenant_id,
                        "multi_tenant_app_name": c.storage_config_info.multi_tenant_app_name,
                        "consent_url": c.storage_config_info.consent_url,
                    }
                    catalog_info.update(
                        {
                            k: v
                            for k, v in azure_info.items()
                            if v is not None and v != []
                        }
                    )
                elif storage_type.lower() == StorageType.GCS.value:
                    gcs_info = {
                        "service_account": c.storage_config_info.gcs_service_account,
                    }
                    catalog_info.update(
                        {k: v for k, v in gcs_info.items() if v is not None and v != []}
                    )
                # Serialize connection info for external catalogs
                if c.type.lower() == CatalogType.EXTERNAL.value:
                    catalog_info.update(
                        self._serialize_connection_info(c.connection_config_info)
                    )
                # Add nested entities
                catalog_info["roles"] = self._export_catalog_roles_for_catalog(
                    api, c.name
                )
                catalog_info["namespaces"] = self._export_namespaces_for_catalog(
                    api, c.name
                )
                catalog_info["policies"] = self._export_policies_for_catalog(
                    api, c.name
                )
                # remove empty sections
                if not catalog_info.get("roles"):
                    del catalog_info["roles"]
                if not catalog_info.get("namespaces"):
                    del catalog_info["namespaces"]
                if not catalog_info.get("policies"):
                    del catalog_info["policies"]
                if not catalog_info.get("properties"):
                    del catalog_info["properties"]
                catalogs_list.append(catalog_info)
        except Exception:
            logger.exception("Failed to export catalogs")
        return catalogs_list

    def _export_catalog_roles_for_catalog(
        self, api: PolarisDefaultApi, catalog_name: str
    ) -> Dict[str, Any]:
        """Export all catalog roles for a given catalog, including assignments and privileges."""
        roles_map: Dict[str, Any] = {}
        try:
            roles = sorted(
                api.list_catalog_roles(catalog_name).roles, key=lambda r: r.name
            )
            for r in roles:
                role_info: Dict[str, Any] = {}
                # Assignments
                assigned_roles_resp = (
                    api.list_assignee_principal_roles_for_catalog_role(
                        catalog_name, r.name
                    )
                )
                if assigned_roles_resp and assigned_roles_resp.roles:
                    role_info["assign_to"] = sorted(
                        [role.name for role in assigned_roles_resp.roles]
                    )
                # Privileges
                privs = api.list_grants_for_catalog_role(catalog_name, r.name).grants
                if privs:
                    privileges: Dict[str, Any] = {}
                    catalog_privs = [
                        p.privilege.value for p in privs if p.type.lower() == "catalog"
                    ]
                    if catalog_privs:
                        privileges["catalog"] = sorted(catalog_privs)
                    ns_privs: Dict[str, List[str]] = {}
                    for p in privs:
                        if p.type.lower() == "namespace":
                            ns_name = ".".join(p.namespace)
                            if ns_name not in ns_privs:
                                ns_privs[ns_name] = []
                            ns_privs[ns_name].append(p.privilege.value)
                    for ns_name in ns_privs:
                        ns_privs[ns_name] = sorted(ns_privs[ns_name])
                    if ns_privs:
                        privileges["namespace"] = ns_privs
                    if privileges:
                        role_info["privileges"] = privileges
                roles_map[r.name] = role_info
        except Exception:
            logger.exception(
                f"Failed to export catalog roles for catalog '{catalog_name}'"
            )
        return roles_map

    def _export_namespaces_for_catalog(
        self, api: PolarisDefaultApi, catalog_name: str
    ) -> List[Any]:
        """Export all namespaces for a given catalog."""
        namespaces_list: List[Any] = []
        catalog_api_client = self._get_catalog_api(api)
        catalog_api = IcebergCatalogAPI(catalog_api_client)
        try:
            namespaces = sorted(
                catalog_api.list_namespaces(prefix=catalog_name).namespaces
            )
            for ns in namespaces:
                ns_name = ".".join(ns)
                ns_details = catalog_api.load_namespace_metadata(
                    prefix=catalog_name, namespace=UNIT_SEPARATOR.join(ns)
                )
                if hasattr(ns_details, "location") or hasattr(ns_details, "properties"):
                    ns_info = {"name": ns_name}
                    if hasattr(ns_details, "location") and ns_details.location:
                        ns_info["location"] = ns_details.location
                    if hasattr(ns_details, "properties") and ns_details.properties:
                        ns_info["properties"] = ns_details.properties
                    namespaces_list.append(ns_info)
                else:
                    namespaces_list.append(ns_name)
        except Exception:
            logger.exception(
                f"Failed to export namespaces for catalog '{catalog_name}'"
            )
        return namespaces_list

    def _export_policies_for_catalog(
        self, api: PolarisDefaultApi, catalog_name: str
    ) -> Dict[str, Any]:
        """Export all policies for a given catalog."""
        policies_map: Dict[str, Any] = {}
        catalog_api_client = self._get_catalog_api(api)
        catalog_api = IcebergCatalogAPI(catalog_api_client)
        try:
            policy_api = PolicyAPI(catalog_api_client)
            namespaces = sorted(
                catalog_api.list_namespaces(prefix=catalog_name).namespaces
            )
            for ns in namespaces:
                ns_name = ".".join(ns)
                namespace_str = ns_name.replace(".", UNIT_SEPARATOR)
                policies = policy_api.list_policies(
                    prefix=catalog_name, namespace=namespace_str
                ).identifiers
                for p in policies:
                    # Load the full policy to get its content
                    full_policy = policy_api.load_policy(
                        prefix=catalog_name, namespace=namespace_str, policy_name=p.name
                    )
                    policy_obj = full_policy.policy
                    # Compact the content JSON string into a single line for export
                    compact_content = None
                    if policy_obj.content:
                        try:
                            content_dict = json.loads(policy_obj.content)
                            compact_content = json.dumps(
                                content_dict, separators=(",", ":")
                            )
                        except json.JSONDecodeError:
                            compact_content = policy_obj.content
                    policy_info = {
                        "namespace": ns_name,
                        "type": policy_obj.policy_type,
                        "content": compact_content,
                    }
                    if policy_obj.description:
                        policy_info["description"] = policy_obj.description
                    policies_map[p.name] = policy_info
        except Exception:
            logger.exception(f"Failed to export policies for catalog '{catalog_name}'")
        return policies_map

    def _load_setup_config(self) -> Dict[str, Any]:
        """Load and cache the setup configuration from a YAML file."""
        if self.setup_config is None:
            raise ValueError(
                "Setup config file path is not provided for apply command."
            )
        if self._config_cache is None:
            if not os.path.isfile(self.setup_config):
                raise FileNotFoundError(f"Config file not found: {self.setup_config}")
            try:
                with open(self.setup_config, "r") as file:
                    self._config_cache = yaml.safe_load(file)
                    if not isinstance(self._config_cache, dict):
                        raise ValueError(
                            "Invalid configuration format: Expected a dictionary at the root level."
                        )
            except (yaml.YAMLError, ValueError) as e:
                raise ValueError(f"Failed to parse YAML configuration: {e}")
        return self._config_cache

    def validate(self) -> None:
        """Validate the command arguments."""
        if self.setup_subcommand == Subcommands.APPLY and self.setup_config is None:
            raise Exception("`setup_config` is required for the `apply` subcommand")

    def execute(self, api: PolarisDefaultApi) -> None:
        """Execute the setup command based on the subcommand (apply or export)."""
        if self.setup_subcommand == Subcommands.APPLY:
            if self.dry_run:
                logger.info("=== Starting Setup Dry-Run ===")
            else:
                logger.info("=== Starting Setup Apply Process ===")
            config = self._load_setup_config()
            # Process global entities
            self._create_principal_roles(
                api, config.get("principal_roles", []), dry_run=self.dry_run
            )
            self._create_principals(
                api, config.get("principals", {}), dry_run=self.dry_run
            )
            # Process entities per catalog
            for catalog_data in config.get("catalogs", []):
                catalog_name = catalog_data.get("name")
                if not catalog_name:
                    logger.warning("Skipping catalog with no name.")
                    continue
                logger.info(f"--- Processing catalog: {catalog_name} ---")
                # Create the catalog
                success = self._create_catalogs(
                    api, [catalog_data], dry_run=self.dry_run
                )
                if not success and not self.dry_run:
                    logger.warning(
                        f"Skipping further processing for catalog '{catalog_name}' due to creation failure."
                    )
                    continue
                # Create catalog specific entities
                self._create_namespaces(
                    api,
                    catalog_name,
                    catalog_data.get("namespaces", []),
                    dry_run=self.dry_run,
                )
                self._create_catalog_roles(
                    api,
                    catalog_name,
                    catalog_data.get("roles", {}),
                    dry_run=self.dry_run,
                )
                self._create_policies_and_attachments(
                    api,
                    catalog_name,
                    catalog_data.get("policies", {}),
                    dry_run=self.dry_run,
                )
                logger.info(f"--- Finished processing catalog: {catalog_name} ---")
            if self.dry_run:
                logger.info("=== Dry-Run Finished ===")
            else:
                logger.info("=== Setup Apply Process Completed Successfully ===")
        elif self.setup_subcommand == Subcommands.EXPORT:
            self._export_config(api)
        else:
            raise Exception(f"Unsupported subcommand: {self.setup_subcommand}")

    def _log_dry_run(
        self,
        action: str,
        entity_type: str,
        entity_name: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Helper for logging dry-run actions."""
        message = f"DRY-RUN: Would {action} {entity_type} {entity_name}"
        if details:
            details_copy = details.copy()
            # Clean up empty fields for cleaner output
            for key in [k for k, v in details_copy.items() if not v and v is not False]:
                if key in details_copy:
                    del details_copy[key]
            if details_copy:
                details_str = yaml.dump(details_copy, indent=2, sort_keys=False).strip()
                message += f" with details:\n{details_str}"
        logger.info(message)

    def _create_principals(
        self,
        api: PolarisDefaultApi,
        principals_config: Dict[str, Any],
        dry_run: bool = False,
    ) -> None:
        """Create principals and assign them to principal roles."""
        logger.info("--- Processing principals ---")
        existing_principals = self._get_existing_principals(api)
        for principal_name, principal_data in principals_config.items():
            if principal_name in existing_principals:
                logger.info(
                    f"Skipping creation for already existing principal: '{principal_name}'"
                )
            else:
                # Create principal
                if dry_run:
                    self._log_dry_run(
                        "create", "principal", principal_name, principal_data
                    )
                    existing_principals.add(principal_name)
                else:
                    try:
                        logger.info(f"Creating principal: {principal_name}")
                        principal_cmd = PrincipalsCommand(
                            principals_subcommand=Subcommands.CREATE,
                            principal_name=principal_name,
                            type=principal_data.get(
                                "type", PrincipalType.SERVICE.value
                            ).lower(),
                            properties=principal_data.get("properties", {}),
                            client_id="",
                            principal_role="",
                            set_properties={},
                            remove_properties=[],
                        )
                        principal_cmd.validate()
                        principal_cmd.execute(api)
                        logger.info(
                            f"Principal '{principal_name}' created successfully."
                        )
                        existing_principals.add(principal_name)
                    except Exception:
                        logger.exception(
                            f"Failed to create principal '{principal_name}'"
                        )
            # Assign roles
            roles_to_assign = principal_data.get("roles", [])
            if not roles_to_assign:
                continue

            self._get_existing_principal_roles(api)
            for role_name in roles_to_assign:
                if (
                    self._existing_principal_roles is None
                    or role_name not in self._existing_principal_roles
                ):
                    logger.warning(
                        f"Skipping assignment for non-existing principal role '{role_name}' for principal '{principal_name}'"
                    )
                    continue

                if dry_run:
                    self._log_dry_run(
                        "assign",
                        "principal",
                        f"'{principal_name}' to role '{role_name}'",
                    )
                else:
                    try:
                        logger.info(
                            f"Assigning principal '{principal_name}' to role '{role_name}'"
                        )
                        role_cmd = PrincipalRolesCommand(
                            principal_roles_subcommand=Subcommands.GRANT,
                            principal_name=principal_name,
                            principal_role_name=role_name,
                            catalog_name="",
                            catalog_role_name="",
                            properties={},
                            set_properties={},
                            remove_properties=[],
                        )
                        role_cmd.validate()
                        role_cmd.execute(api)
                        logger.info(
                            f"Assigned principal '{principal_name}' to role '{role_name}' successfully."
                        )
                    except Exception:
                        logger.exception(
                            f"Failed to assign principal '{principal_name}' to role '{role_name}'"
                        )
        logger.info("--- Finished processing principals ---")

    def _create_principal_roles(
        self,
        api: PolarisDefaultApi,
        principal_roles_config: List[str],
        dry_run: bool = False,
    ) -> None:
        """Create principal roles."""
        logger.info("--- Processing principal roles ---")
        self._get_existing_principal_roles(api)

        for role_name in principal_roles_config:
            if (
                self._existing_principal_roles is not None
                and role_name in self._existing_principal_roles
            ):
                logger.info(
                    f"Skipping creation for already existing principal role: '{role_name}'"
                )
                continue
            if dry_run:
                self._log_dry_run("create", "principal role", role_name)
                if self._existing_principal_roles is not None:
                    self._existing_principal_roles.add(role_name)
            else:
                try:
                    logger.info(f"Creating principal role: {role_name}")
                    cmd = PrincipalRolesCommand(
                        principal_roles_subcommand=Subcommands.CREATE,
                        principal_role_name=role_name,
                        principal_name="",
                        catalog_name="",
                        catalog_role_name="",
                        properties={},
                        set_properties={},
                        remove_properties=[],
                    )
                    cmd.validate()
                    cmd.execute(api)
                    logger.info(f"Principal role '{role_name}' created successfully.")
                    if self._existing_principal_roles is not None:
                        self._existing_principal_roles.add(role_name)
                except Exception:
                    logger.exception(f"Failed to create principal role '{role_name}'")
        logger.info("--- Finished processing principal roles ---")

    def _map_storage_properties(self, catalog_data: Dict[str, Any]) -> Dict[str, Any]:
        """Maps storage-related properties from YAML data to command arguments."""
        storage_keys = [
            "storage_type",
            "default_base_location",
            "allowed_locations",
            "properties",
            "role_arn",
            "external_id",
            "user_arn",
            "region",
            "tenant_id",
            "multi_tenant_app_name",
            "consent_url",
            "service_account",
            "endpoint",
            "endpoint_internal",
            "sts_endpoint",
            "sts_unavailable",
            "kms_unavailable",
            "path_style_access",
            "current_kms_key",
            "allowed_kms_keys",
        ]
        return {
            key: catalog_data.get(key)
            for key in storage_keys
            if catalog_data.get(key) is not None
        }

    def _map_authentication_properties(
        self, auth_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Maps authentication properties from YAML data to command arguments."""
        auth_args = {}
        auth_type = auth_data.get("type")
        if not auth_type:
            return {}
        auth_args["catalog_authentication_type"] = auth_type
        if auth_type == AuthenticationType.OAUTH.value:
            auth_args.update(
                {
                    "catalog_token_uri": auth_data.get("token_uri"),
                    "catalog_client_id": auth_data.get("client_id"),
                    "catalog_client_secret": auth_data.get("client_secret"),
                    "catalog_client_scopes": auth_data.get("scopes"),
                }
            )
        elif auth_type == AuthenticationType.BEARER.value:
            auth_args["catalog_bearer_token"] = auth_data.get("token")
        elif auth_type == AuthenticationType.SIGV4.value:
            auth_args.update(
                {
                    "catalog_role_arn": auth_data.get("role_arn"),
                    "catalog_role_session_name": auth_data.get("role_session_name"),
                    "catalog_external_id": auth_data.get("external_id"),
                    "catalog_signing_region": auth_data.get("signing_region"),
                    "catalog_signing_name": auth_data.get("signing_name"),
                }
            )
        return auth_args

    def _map_service_identity_properties(
        self, identity_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Maps service identity properties from YAML data to command arguments."""
        identity_args = {}
        identity_type = identity_data.get("type")
        if not identity_type:
            return {}
        identity_args["catalog_service_identity_type"] = identity_type
        if identity_type == ServiceIdentityType.AWS_IAM.value:
            identity_args["catalog_service_identity_iam_arn"] = identity_data.get(
                "iam_arn"
            )
        return identity_args

    def _map_connection_properties(
        self, catalog_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Maps connection specific properties from YAML data to command arguments."""
        connection_args = {}
        connection_data = catalog_data.get("connection", {})
        if not connection_data:
            return {}
        conn_type = connection_data.get("type")
        if not conn_type:
            return {}
        connection_args["catalog_connection_type"] = conn_type
        connection_args["catalog_uri"] = connection_data.get("uri")
        # Connection specific properties
        if conn_type == CatalogConnectionType.HADOOP.value:
            connection_args["hadoop_warehouse"] = connection_data.get("warehouse")
        elif conn_type == CatalogConnectionType.HIVE.value:
            connection_args["hive_warehouse"] = connection_data.get("warehouse")
        elif conn_type == CatalogConnectionType.ICEBERG.value:
            connection_args["iceberg_remote_catalog_name"] = connection_data.get(
                "remote_catalog_name"
            )
        # Authentication and Service Identity
        connection_args.update(
            self._map_authentication_properties(
                connection_data.get("authentication", {})
            )
        )
        connection_args.update(
            self._map_service_identity_properties(
                connection_data.get("service_identity", {})
            )
        )
        return connection_args

    def _create_catalogs(
        self,
        api: PolarisDefaultApi,
        catalogs_config: List[Dict[str, Any]],
        dry_run: bool = False,
    ) -> bool:
        """Create catalogs by parsing YAML data and calling the CatalogsCommand.
        Returns True if all catalogs were processed successfully or already exist.
        """
        logger.info("--- Processing catalogs ---")
        overall_success = True
        existing_catalogs = self._get_existing_catalogs(api)
        for catalog_data in catalogs_config:
            catalog_name = catalog_data.get("name")
            if not catalog_name:
                logger.warning("Skipping catalog with no name.")
                continue
            if catalog_name in existing_catalogs:
                logger.info(
                    f"Skipping creation for already existing catalog: '{catalog_name}'"
                )
                continue
            if dry_run:
                details = catalog_data.copy()
                if "name" in details:
                    del details["name"]
                self._log_dry_run("create", "catalog", catalog_name, details)
                existing_catalogs.add(catalog_name)
            else:
                try:
                    logger.info(f"Creating catalog: {catalog_name}")
                    # Map YAML data to command arguments using helpers
                    command_args = {
                        "catalogs_subcommand": Subcommands.CREATE,
                        "catalog_name": catalog_name,
                        "catalog_type": catalog_data.get(
                            "type", CatalogType.INTERNAL.value
                        ).lower(),
                    }
                    command_args.update(self._map_storage_properties(catalog_data))
                    if command_args.get("catalog_type") == CatalogType.EXTERNAL.value:
                        conn_args = self._map_connection_properties(catalog_data)
                        if not conn_args:
                            logger.warning(
                                f"External catalog '{catalog_name}' is missing the required 'connection' info block."
                            )
                            overall_success = False
                            continue
                        command_args.update(conn_args)
                    # Explicitly call constructor to satisfy mypy and improve readability
                    cmd = CatalogsCommand(
                        catalogs_subcommand=command_args.get(
                            "catalogs_subcommand", Subcommands.CREATE
                        ),
                        catalog_name=command_args.get("catalog_name", ""),
                        catalog_type=command_args.get("catalog_type", "INTERNAL"),
                        default_base_location=command_args.get(
                            "default_base_location", ""
                        ),
                        storage_type=command_args.get("storage_type", ""),
                        allowed_locations=command_args.get("allowed_locations") or [],
                        properties=command_args.get("properties") or {},
                        set_properties=command_args.get("set_properties") or {},
                        remove_properties=command_args.get("remove_properties") or [],
                        role_arn=command_args.get("role_arn", ""),
                        external_id=command_args.get("external_id", ""),
                        user_arn=command_args.get("user_arn", ""),
                        region=command_args.get("region", ""),
                        tenant_id=command_args.get("tenant_id", ""),
                        multi_tenant_app_name=command_args.get(
                            "multi_tenant_app_name", ""
                        ),
                        consent_url=command_args.get("consent_url", ""),
                        service_account=command_args.get("service_account", ""),
                        hierarchical=command_args.get("hierarchical", False),
                        endpoint=command_args.get("endpoint", ""),
                        endpoint_internal=command_args.get("endpoint_internal", ""),
                        sts_endpoint=command_args.get("sts_endpoint", ""),
                        sts_unavailable=command_args.get("sts_unavailable", False),
                        kms_unavailable=command_args.get("kms_unavailable", False),
                        path_style_access=command_args.get("path_style_access", False),
                        current_kms_key=command_args.get("current_kms_key", ""),
                        allowed_kms_keys=command_args.get("allowed_kms_keys") or [],
                        catalog_connection_type=command_args.get(
                            "catalog_connection_type", ""
                        ),
                        catalog_uri=command_args.get("catalog_uri", ""),
                        hadoop_warehouse=command_args.get("hadoop_warehouse", ""),
                        hive_warehouse=command_args.get("hive_warehouse", ""),
                        iceberg_remote_catalog_name=command_args.get(
                            "iceberg_remote_catalog_name", ""
                        ),
                        catalog_authentication_type=command_args.get(
                            "catalog_authentication_type", ""
                        ),
                        catalog_token_uri=command_args.get("catalog_token_uri", ""),
                        catalog_client_id=command_args.get("catalog_client_id", ""),
                        catalog_client_secret=command_args.get(
                            "catalog_client_secret", ""
                        ),
                        catalog_client_scopes=command_args.get("catalog_client_scopes")
                        or [],
                        catalog_bearer_token=command_args.get(
                            "catalog_bearer_token", ""
                        ),
                        catalog_role_arn=command_args.get("catalog_role_arn", ""),
                        catalog_role_session_name=command_args.get(
                            "catalog_role_session_name", ""
                        ),
                        catalog_external_id=command_args.get("catalog_external_id", ""),
                        catalog_signing_region=command_args.get(
                            "catalog_signing_region", ""
                        ),
                        catalog_signing_name=command_args.get(
                            "catalog_signing_name", ""
                        ),
                        catalog_service_identity_type=command_args.get(
                            "catalog_service_identity_type", ""
                        ),
                        catalog_service_identity_iam_arn=command_args.get(
                            "catalog_service_identity_iam_arn", ""
                        ),
                    )
                    cmd.validate()
                    cmd.execute(api)
                    logger.info(f"Catalog '{catalog_name}' created successfully.")
                    existing_catalogs.add(catalog_name)
                except Exception:
                    logger.exception(f"Failed to create catalog '{catalog_name}'")
                    overall_success = False
        logger.info("--- Finished processing catalogs ---")
        return overall_success

    def _create_catalog_roles(
        self,
        api: PolarisDefaultApi,
        catalog_name: str,
        roles_config: Dict[str, Any],
        dry_run: bool = False,
    ) -> None:
        """Create catalog roles, assign them to principal roles, and grant privileges."""
        logger.info(f"--- Processing catalog roles for catalog: {catalog_name} ---")
        existing_roles_in_catalog = self._get_existing_catalog_roles(api, catalog_name)
        self._get_existing_principal_roles(api)
        for role_name, role_data in roles_config.items():
            if role_name in existing_roles_in_catalog:
                logger.info(
                    f"Skipping creation for already existing catalog role: '{role_name}' in catalog '{catalog_name}'"
                )
            else:
                if dry_run:
                    self._log_dry_run(
                        "create", "catalog role", role_name, {"catalog": catalog_name}
                    )
                    # Add to local cache for dry-run to prevent duplicate logs for assigns/grants
                    existing_roles_in_catalog.add(role_name)
                else:
                    try:
                        logger.info(
                            f"Creating catalog role: {role_name} in catalog: {catalog_name}"
                        )
                        cmd = CatalogRolesCommand(
                            catalog_roles_subcommand=Subcommands.CREATE,
                            catalog_role_name=role_name,
                            catalog_name=catalog_name,
                            properties=role_data.get("properties", {}),
                            principal_role_name="",
                            set_properties={},
                            remove_properties=[],
                        )
                        cmd.validate()
                        cmd.execute(api)
                        logger.info(
                            f"Catalog role '{role_name}' created successfully in catalog '{catalog_name}'."
                        )
                        # Update the cache with the newly created role
                        existing_roles_in_catalog.add(role_name)
                    except Exception:
                        logger.exception(
                            f"Failed to create catalog role '{role_name}' in catalog '{catalog_name}'"
                        )
                        continue
            # Assign to principal roles
            principal_roles_to_assign = role_data.get("assign_to", [])
            for principal_role_name in principal_roles_to_assign:
                if (
                    self._existing_principal_roles is None
                    or principal_role_name not in self._existing_principal_roles
                ):
                    logger.warning(
                        f"Skipping assignment of catalog role '{role_name}' to non-existing principal role '{principal_role_name}'"
                    )
                    continue
                if dry_run:
                    self._log_dry_run(
                        "assign",
                        "catalog role",
                        f"'{role_name}' to principal role '{principal_role_name}'",
                        {"catalog": catalog_name},
                    )
                else:
                    try:
                        logger.info(
                            f"Assigning catalog role '{role_name}' to principal role '{principal_role_name}' in catalog '{catalog_name}'"
                        )
                        cmd = CatalogRolesCommand(
                            catalog_roles_subcommand=Subcommands.GRANT,
                            catalog_role_name=role_name,
                            principal_role_name=principal_role_name,
                            catalog_name=catalog_name,
                            properties=None,
                            set_properties={},
                            remove_properties=[],
                        )
                        cmd.validate()
                        cmd.execute(api)
                        logger.info(
                            f"Assigned catalog role '{role_name}' to principal role '{principal_role_name}' successfully."
                        )
                    except Exception:
                        logger.exception(
                            f"Failed to assign catalog role '{role_name}' to principal role '{principal_role_name}'"
                        )
            # Grant privileges
            privileges_to_grant = role_data.get("privileges", {})
            if "table" in privileges_to_grant or "view" in privileges_to_grant:
                logger.warning(
                    f"Granting 'table' or 'view' level privileges is not currently supported by the 'setup apply' command. "
                    f"The defined table/view privileges for role '{role_name}' in catalog '{catalog_name}' will be ignored."
                )
            for priv in privileges_to_grant.get("catalog", []):
                self._grant_privilege(
                    api, catalog_name, role_name, "catalog", priv, dry_run=dry_run
                )
            for ns_name, privs in privileges_to_grant.get("namespace", {}).items():
                for priv in privs:
                    self._grant_privilege(
                        api,
                        catalog_name,
                        role_name,
                        "namespace",
                        priv,
                        namespace=ns_name,
                        dry_run=dry_run,
                    )
        logger.info(
            f"--- Finished processing catalog roles for catalog: {catalog_name} ---"
        )

    def _grant_privilege(
        self,
        api: PolarisDefaultApi,
        catalog_name: str,
        catalog_role_name: str,
        level: str,
        privilege: str,
        namespace: Optional[str] = None,
        dry_run: bool = False,
    ) -> None:
        """Helper to grant a single privilege."""
        if dry_run:
            self._log_dry_run(
                "grant",
                "privilege",
                f"'{privilege}' on {level} '{namespace or catalog_name}' to role '{catalog_role_name}'",
            )
            return
        try:
            log_message = f"privilege '{privilege}' on {level} '{namespace or catalog_name}' to role '{catalog_role_name}'"
            logger.info(f"Granting {log_message}")
            privilege_args = {
                "action": Actions.GRANT,
                "cascade": False,
                "privilege": privilege,
                "catalog_role_name": catalog_role_name,
                "catalog_name": catalog_name,
            }
            if level == "namespace":
                if namespace:
                    privilege_args["namespace"] = namespace.split(".")
            subcommand_map = {
                "catalog": Subcommands.CATALOG,
                "namespace": Subcommands.NAMESPACE,
            }
            privileges_subcommand = subcommand_map.get(level)
            if not privileges_subcommand:
                logger.warning(f"Unsupported privilege level '{level}'")
                return
            cmd = PrivilegesCommand(
                privileges_subcommand=privileges_subcommand,
                action=str(privilege_args.get("action")),
                cascade=bool(privilege_args.get("cascade")),
                privilege=str(privilege_args.get("privilege")),
                catalog_role_name=str(privilege_args.get("catalog_role_name")),
                catalog_name=str(privilege_args.get("catalog_name")),
                namespace=cast(List[str], privilege_args.get("namespace") or []),
                view="",
                table="",
            )
            cmd.validate()
            cmd.execute(api)
            logger.info(f"Successfully granted {log_message}")
        except Exception:
            logger.exception(f"Failed to grant {log_message}")

    def _create_namespaces(
        self,
        api: PolarisDefaultApi,
        catalog_name: str,
        namespaces_config: List[Any],
        dry_run: bool = False,
    ) -> None:
        """Create namespaces in a specific catalog, including any parent namespaces."""
        logger.info(f"--- Processing namespaces for catalog: {catalog_name} ---")
        catalog_api_client = self._get_catalog_api(api)
        catalog_api = IcebergCatalogAPI(catalog_api_client)
        try:
            existing_namespaces = {
                ".".join(ns)
                for ns in catalog_api.list_namespaces(prefix=catalog_name).namespaces
            }
        except NotFoundException:
            # This is expected if the catalog has no namespaces yet
            existing_namespaces = set()
        except Exception:
            logger.exception(f"Failed to fetch namespaces for catalog '{catalog_name}'")
            existing_namespaces = set()
        all_namespaces_to_create = set()
        namespace_data_map = {}
        for ns_item in namespaces_config:
            ns_name: Optional[str]
            ns_data: Dict[str, Any]
            if isinstance(ns_item, str):
                ns_name = ns_item
                ns_data = {}
            elif isinstance(ns_item, dict):
                ns_name = ns_item.get("name")
                ns_data = ns_item
            else:
                logger.warning(f"Skipping invalid namespace entry: {ns_item}")
                continue
            if not ns_name:
                logger.warning(
                    f"Skipping namespace with no name in catalog '{catalog_name}'"
                )
                continue
            # Store data for the full namespace path
            namespace_data_map[ns_name] = ns_data
            # Add the full namespace and all its parents to the set
            parts = ns_name.split(".")
            for i in range(len(parts)):
                parent_ns_name = ".".join(parts[: i + 1])
                all_namespaces_to_create.add(parent_ns_name)
        sorted_namespaces = sorted(list(all_namespaces_to_create))
        for ns_name in sorted_namespaces:
            if ns_name in existing_namespaces:
                logger.info(
                    f"Skipping creation for already existing namespace '{ns_name}' in catalog '{catalog_name}'"
                )
                continue
            # Get data if it exists for this specific namespace, otherwise empty
            ns_data = namespace_data_map.get(ns_name, {})
            if dry_run:
                self._log_dry_run(
                    "create", "namespace", ns_name, {"catalog": catalog_name, **ns_data}
                )
            else:
                try:
                    logger.info(
                        f"Creating namespace: '{ns_name}' in catalog: '{catalog_name}'"
                    )
                    cmd = NamespacesCommand(
                        namespaces_subcommand=Subcommands.CREATE,
                        catalog=catalog_name,
                        namespace=ns_name.split("."),
                        parent=[],
                        location=ns_data.get("location") or "",
                        properties=ns_data.get("properties", {}),
                    )
                    cmd.validate()
                    cmd.execute(api)
                    logger.info(
                        f"Namespace '{ns_name}' created successfully in catalog '{catalog_name}'."
                    )
                except Exception:
                    logger.exception(
                        f"Failed to create namespace '{ns_name}' in catalog '{catalog_name}'"
                    )
        logger.info(
            f"--- Finished processing namespaces for catalog: {catalog_name} ---"
        )

    def _create_policies_and_attachments(
        self,
        api: PolarisDefaultApi,
        catalog_name: str,
        policies_config: Dict[str, Any],
        dry_run: bool = False,
    ) -> None:
        """Create policies and attach them."""
        logger.info(f"--- Processing policies for catalog: {catalog_name} ---")
        catalog_api_client = self._get_catalog_api(api)
        policy_api = PolicyAPI(catalog_api_client)
        for policy_name, policy_data in policies_config.items():
            ns_name = policy_data.get("namespace")
            if not ns_name:
                logger.warning(
                    f"Skipping policy '{policy_name}' due to missing namespace."
                )
                continue
            # Check if policy exists
            policy_exists = False
            try:
                namespace_str = ns_name.replace(".", UNIT_SEPARATOR)
                policy_api.load_policy(
                    prefix=catalog_name,
                    namespace=namespace_str,
                    policy_name=policy_name,
                )
                policy_exists = True
            except NotFoundException:
                policy_exists = False
            except Exception:
                logger.warning(
                    f"Could not verify existence of policy '{policy_name}', attempting creation."
                )
                policy_exists = False
            if policy_exists:
                logger.info(
                    f"Skipping creation for already existing policy '{policy_name}' in catalog '{catalog_name}' and namespace '{ns_name}'"
                )
            else:
                if not self._validate_entity(
                    policy_data, ["type"], f"policy '{policy_name}'"
                ):
                    continue
                if "content" not in policy_data and "file" not in policy_data:
                    logger.warning(
                        f"Skipping policy '{policy_name}' due to missing 'content' or 'file' field."
                    )
                    continue
                if dry_run:
                    self._log_dry_run(
                        "create",
                        "policy",
                        policy_name,
                        {"catalog": catalog_name, **policy_data},
                    )
                else:
                    try:
                        logger.info(
                            f"Creating policy: '{policy_name}' in catalog: '{catalog_name}' and namespace: '{ns_name}'"
                        )
                        policy_content_str = ""
                        if (
                            "content" in policy_data
                            and policy_data["content"] is not None
                        ):
                            policy_content_str = json.dumps(policy_data["content"])
                        elif "file" in policy_data:
                            if self.setup_config is None:
                                logger.error(
                                    "Path to setup config file not available to load policy file"
                                )
                                continue
                            config_dir = os.path.dirname(self.setup_config)
                            policy_file_path = os.path.join(
                                config_dir, policy_data["file"]
                            )
                            with open(policy_file_path, "r") as f:
                                policy_content_str = json.dumps(json.load(f))
                        policy_api.create_policy(
                            prefix=catalog_name,
                            namespace=ns_name.replace(".", UNIT_SEPARATOR),
                            create_policy_request=CreatePolicyRequest(
                                name=policy_name,
                                type=policy_data["type"],
                                description=policy_data.get("description"),
                                content=policy_content_str,
                            ),
                        )
                        logger.info(f"Policy '{policy_name}' created successfully.")
                    except Exception:
                        logger.exception(f"Failed to create policy '{policy_name}'")
                        continue
            # Attachments
            attachments = policy_data.get("attach", [])
            for attachment in attachments:
                attachment_type = attachment.get("type")
                if not attachment_type:
                    logger.warning(
                        f"Skipping attachment for policy '{policy_name}' due to missing 'type' field."
                    )
                    continue
                attachment_path = attachment.get("path")
                if dry_run:
                    self._log_dry_run(
                        "attach",
                        "policy",
                        f"'{policy_name}' to '{attachment_type}'",
                        {"path": attachment_path},
                    )
                else:
                    try:
                        log_message = (
                            f"Attaching policy '{policy_name}' to '{attachment_type}'"
                        )
                        if attachment_path:
                            log_message += f" at path '{attachment_path}'"
                        logger.info(log_message)
                        cmd = PoliciesCommand(
                            policies_subcommand=Subcommands.ATTACH,
                            catalog_name=catalog_name,
                            namespace=ns_name,
                            policy_name=policy_name,
                            attachment_type=attachment_type,
                            attachment_path=attachment_path,
                            parameters=attachment.get("parameters"),
                            policy_file="",
                            policy_type="",
                            policy_description="",
                            target_name="",
                            detach_all=False,
                            applicable=False,
                        )
                        cmd.validate()
                        cmd.execute(api)
                        logger.info(f"Successfully attached policy '{policy_name}'")
                    except Exception:
                        logger.exception(f"Failed to attach policy '{policy_name}'")
        logger.info(f"--- Finished processing policies for catalog: {catalog_name} ---")

    def _validate_entity(
        self, entity: Dict[str, Any], required_fields: List[str], entity_name: str
    ) -> bool:
        """Validate that an entity has all required fields."""
        missing_fields = [field for field in required_fields if not entity.get(field)]
        if missing_fields:
            logger.warning(
                f"Skipping {entity_name} due to missing required fields: {', '.join(missing_fields)}. Entity: {entity}"
            )
            return False
        return True
