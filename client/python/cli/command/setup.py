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
from dataclasses import dataclass
from typing import Dict, Optional, List, Any, Set

from cli.command import Command
from cli.constants import Subcommands, StorageType, UNIT_SEPARATOR, Actions, PrincipalType, CatalogType

from polaris.management import PolarisDefaultApi
from cli.command.principals import PrincipalsCommand
from cli.command.principal_roles import PrincipalRolesCommand
from cli.command.catalogs import CatalogsCommand
from cli.command.catalog_roles import CatalogRolesCommand
from cli.command.namespaces import NamespacesCommand
from cli.command.privileges import PrivilegesCommand

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

@dataclass
class SetupCommand(Command):
    setup_subcommand: str
    setup_config: str
    _config_cache: Optional[Dict[str, Any]] = None

    def _load_setup_config(self) -> Dict[str, Any]:
        """
        Load and cache the setup configuration from a YAML file.
        """
        if self._config_cache is not None:
            return self._config_cache

        if not os.path.isfile(self.setup_config):
            raise FileNotFoundError(f"Config file not found: {self.setup_config}")

        try:
            with open(self.setup_config, 'r') as file:
                self._config_cache = yaml.safe_load(file)
                if not isinstance(self._config_cache, dict):
                    raise ValueError("Invalid configuration format: Expected a dictionary at the root level.")
                return self._config_cache
        except (yaml.YAMLError, ValueError) as e:
            raise ValueError(f"Failed to parse YAML configuration: {e}")

    def _extract_principals(self) -> List[Dict[str, Any]]:
        """
        Extract principal information from the setup configuration.
        """
        config = self._load_setup_config()
        principals = config.get('principals', [])
        extracted = []
        seen = set()

        # Get allowed principal types from PrincipalType
        allowed_principal_types = {pt.value for pt in PrincipalType}

        for principal in principals:
            name = principal.get('name')
            principal_type = principal.get('type', PrincipalType.SERVICE.value).lower()  # Default to 'service' and normalize to lowercase
            properties = principal.get('properties', {})

            # Validate required fields
            missing_fields = [field for field, value in {
                "name": name
            }.items() if not value]

            if missing_fields:
                logger.warning(f"Skipping principal due to missing required fields: {', '.join(missing_fields)}. Principal: {principal}")
                continue

            # Validate principal type
            if principal_type not in allowed_principal_types:
                logger.warning(f"Skipping principal '{name}' due to invalid type '{principal_type}'. Allowed types: {', '.join(allowed_principal_types)}")
                continue

            # Check for duplicate principals
            if name in seen:
                logger.warning(f"Skipping duplicate principal: '{name}'")
                continue

            seen.add(name)
            extracted.append({
                'name': name,
                'type': principal_type.upper(),
                'properties': properties,
            })

        if not extracted:
            logger.warning("No valid principals found in the configuration.")

        return extracted

    def _get_current_principals(self, api: PolarisDefaultApi) -> Set[str]:
        """
        Get the current principals from the API.
        Returns a set of principal names.
        """
        try:
            # Fetch principals from the API
            principals = api.list_principals().principals
            return {principal.name for principal in principals}
        except Exception as e:
            logger.error(f"Failed to fetch existing principals: {e}")
            return set()

    def _create_principals(self, api: PolarisDefaultApi):
        """
        Create principals based on the setup configuration.
        """
        try:
            # Fetch existing principals from the API
            existed_principals = self._get_current_principals(api)

            # Extract principals from the configuration
            extracted_principals = self._extract_principals()

            # Separate new and skipped principals
            new_principals = [
                principal for principal in extracted_principals
                if principal['name'] not in existed_principals
            ]
            skipped_principals = [
                principal['name'] for principal in extracted_principals
                if principal['name'] in existed_principals
            ]

            # Log skipped principals
            if skipped_principals:
                logger.warning(f"Skipping creation for already existing principals: {', '.join(skipped_principals)}")

            # Create new principals
            for principal in new_principals:
                try:
                    logger.info(f"Creating principal: {principal['name']}")
                    principal_command = PrincipalsCommand(
                        principals_subcommand=Subcommands.CREATE,
                        type=principal['type'].upper(),
                        principal_name=principal['name'],
                        client_id='',  # Default value
                        principal_role='',  # Default value
                        properties=principal['properties'],
                        set_properties={},  # Default value
                        remove_properties=[]  # Default value
                    )
                    principal_command.execute(api)
                    logger.info(f"Principal '{principal['name']}' created successfully.")
                except Exception as e:
                    logger.error(f"Failed to create principal '{principal['name']}': {e}")

        except Exception as e:
            logger.error(f"Failed to create principals: {e}")
    
    def _extract_principals_role(self) -> List[Dict[str, Any]]:
        """
        Extract principal role information from the setup configuration.
        """
        config = self._load_setup_config()
        principal_roles = config.get('principal_roles', [])
        extracted = []
        seen = set()

        for role in principal_roles:
            name = role.get('name')
            properties = role.get('properties', {})

            # Validate required fields
            missing_fields = [field for field, value in {
                "name": name
            }.items() if not value]

            if missing_fields:
                logger.warning(f"Skipping principal role due to missing required fields: {', '.join(missing_fields)}. Role: {role}")
                continue

            # Check for duplicate principal roles
            if name in seen:
                logger.warning(f"Skipping duplicate principal role: '{name}'")
                continue

            seen.add(name)
            extracted.append({
                'name': name,
                'properties': properties,
            })

        if not extracted:
            logger.warning("No valid principal roles found in the configuration.")

        return extracted

    def _get_current_principal_roles(self, api: PolarisDefaultApi) -> Set[str]:
        """
        Get the current principal roles from the API.
        Returns a set of principal role names.
        """
        try:
            # Fetch principal roles from the API
            principal_roles = api.list_principal_roles().roles
            return {role.name for role in principal_roles}
        except Exception as e:
            logger.error(f"Failed to fetch existing principal roles: {e}")
            return set()

    def _create_principal_roles(self, api: PolarisDefaultApi):
        """
        Create principal roles based on the setup configuration.
        """
        try:
            # Fetch existing principal roles from the API
            existed_principal_roles = self._get_current_principal_roles(api)

            # Extract principal roles from the configuration
            extracted_principal_roles = self._extract_principals_role()

            # Separate new and existing principal roles
            new_principal_roles = [
                role for role in extracted_principal_roles
                if role['name'] not in existed_principal_roles
            ]
            skipped_principal_roles = [
                role['name'] for role in extracted_principal_roles
                if role['name'] in existed_principal_roles
            ]

            # Log skipped principal roles
            if skipped_principal_roles:
                logger.warning(f"Skipping creation for already existing principal roles: {', '.join(skipped_principal_roles)}")

            # Create new principal roles
            for role in new_principal_roles:
                try:
                    logger.info(f"Creating principal role: {role['name']}")
                    principal_role_command = PrincipalRolesCommand(
                        principal_roles_subcommand=Subcommands.CREATE,
                        principal_role_name=role['name'],
                        principal_name='',  # Default value
                        catalog_name='',  # Default value
                        catalog_role_name='',  # Default value
                        properties=role['properties'],
                        set_properties={},  # Default value
                        remove_properties=[]  # Default value
                    )
                    principal_role_command.execute(api)
                    logger.info(f"Principal role '{role['name']}' created successfully.")
                except Exception as e:
                    logger.error(f"Failed to create principal role '{role['name']}': {e}")
        except Exception as e:
            logger.error(f"Failed to create principal roles: {e}")

    def _extract_principals_role_assignments(self) -> List[Dict[str, str]]:
        """
        Extract principal-to-principal-role assignments from the setup configuration.
        """
        config = self._load_setup_config()
        principals = config.get('principals', [])
        assignments = []
        seen = set()

        for principal in principals:
            name = principal.get('name')
            if not name:
                logger.warning(f"Skipping principal with missing 'name': {principal}")
                continue

            for assignment in principal.get('assignments', []):
                principal_role = assignment.get('principal_role')

                # Validate required fields
                missing_fields = [field for field, value in {
                    "principal": name,
                    "principal_role": principal_role
                }.items() if not value]

                if missing_fields:
                    logger.warning(f"Skipping assignment due to missing required fields: {', '.join(missing_fields)}. Assignment: {assignment}")
                    continue

                # Check for duplicate assignments
                assignment_key = (name, principal_role)
                if assignment_key in seen:
                    logger.warning(f"Skipping duplicate assignment of role '{principal_role}' to principal '{name}'")
                    continue

                seen.add(assignment_key)
                assignments.append({
                    'principal': name,
                    'principal_role': principal_role,
                })

        if not assignments:
            logger.warning("No valid principal-to-principal-role assignments found in the configuration.")

        return assignments

    def _create_principal_role_assignments(self, api: PolarisDefaultApi):
        """
        Create principal-to-principal-role assignments based on the setup configuration.
        """
        try:
            # Fetch existing principal roles from the API
            existed_principal_role_names = self._get_current_principal_roles(api)

            # Extract assignments from the configuration
            extracted_assignments = self._extract_principals_role_assignments()

            # Process each assignment
            for assignment in extracted_assignments:
                principal_name = assignment['principal']
                principal_role = assignment['principal_role']

                # Validate that the principal role exists
                if principal_role not in existed_principal_role_names:
                    logger.warning(f"Skipping assignment for non-existing principal role '{principal_role}' for principal '{principal_name}'")
                    continue

                # Assign the principal role
                try:
                    logger.info(f"Assigning principal '{principal_name}' to role '{principal_role}'")
                    principal_role_command = PrincipalRolesCommand(
                        principal_roles_subcommand=Subcommands.GRANT,
                        principal_name=principal_name,
                        principal_role_name=principal_role,
                        catalog_name='',  # Default value
                        catalog_role_name='',  # Default value
                        properties='',  # Default value
                        set_properties={},  # Default value
                        remove_properties=[]  # Default value
                    )
                    principal_role_command.execute(api)
                    logger.info(f"Assigned principal '{principal_name}' to role '{principal_role}' successfully.")
                except Exception as e:
                    logger.error(f"Failed to assign principal '{principal_name}' to role '{principal_role}': {e}")
        except Exception as e:
            logger.error(f"Failed to create principal role assignments: {e}")

    def _extract_catalogs(self) -> List[Dict[str, Any]]:
        """
        Extract catalog information from the setup configuration.
        """
        config = self._load_setup_config()
        catalogs = config.get('catalogs', [])
        extracted = []
        seen = set()

        # Get allowed catalog types from CatalogType
        allowed_catalog_types = {ct.value for ct in CatalogType}

        # Get allowed storage types from StorageType
        allowed_storage_types = {st.value for st in StorageType}

        for catalog in catalogs:
            name = catalog.get('name')
            storage_type = catalog.get('storage_type').lower() # Normalize to lowercase
            default_base_location = catalog.get('default_base_location')
            role_arn = catalog.get('role_arn')
            tenant_id = catalog.get('tenant_id')
            service_account = catalog.get('service_account')
            catalog_type = catalog.get('type', CatalogType.INTERNAL.value).lower()  # Default to 'internal' and normalize to lowercase
            remote_url = catalog.get('remote_url')

            # Validate required fields
            missing_fields = [field for field, value in {
                "name": name,
                "storage_type": storage_type,
                "default_base_location": default_base_location
            }.items() if not value]

            if missing_fields:
                logger.warning(f"Skipping catalog due to missing required fields: {', '.join(missing_fields)}. Catalog: {catalog}")
                continue

            # Validate catalog type
            if catalog_type not in allowed_catalog_types:
                logger.warning(f"Skipping catalog '{name}' due to invalid type '{catalog_type}'. Allowed types: {', '.join(allowed_catalog_types)}")
                continue

            # Validate storage type
            if storage_type not in allowed_storage_types:
                logger.warning(f"Skipping catalog '{name}' due to invalid storage type '{storage_type}'. Allowed types: {', '.join(allowed_storage_types)}")
                continue

            # Additional validation for specific catalog/storage types
            if catalog_type == CatalogType.EXTERNAL.value and not remote_url:
                logger.warning(f"Skipping external catalog with missing 'remote_url': {catalog}")
                continue
            if storage_type == StorageType.S3.value and not role_arn:
                logger.warning(f"Skipping S3 catalog with missing 'role_arn': {catalog}")
                continue
            if storage_type == StorageType.AZURE.value and not tenant_id:
                logger.warning(f"Skipping Azure catalog with missing 'tenant_id': {catalog}")
                continue
            if storage_type == StorageType.GCS.value and not service_account:
                logger.warning(f"Skipping GCS catalog with missing 'service_account': {catalog}")
                continue            

            # Check for duplicate catalog names
            if name in seen:
                logger.warning(f"Skipping duplicate catalog: '{name}'")
                continue

            seen.add(name)
            extracted.append({
                'name': name,
                'type': catalog_type.upper(),
                'storage_type': storage_type.lower(), # Normalize to lowercase for passing additioanl validation in the catalog creation
                'default_base_location': default_base_location,
                'allowed_locations': catalog.get('allowed_locations', []),
                'role_arn': role_arn,
                'region': catalog.get('region'),
                'external_id': catalog.get('external_id'),
                'user_arn': catalog.get('user_arn'),
                'tenant_id': tenant_id,
                'multi_tenant_app_name': catalog.get('multi_tenant_app_name'),
                'consent_url': catalog.get('consent_url'),
                'service_account': catalog.get('service_account'),
                'remote_url': catalog.get('remote_url'),
                'properties': catalog.get('properties', {}),
            })

        if not extracted:
            logger.warning("No valid catalogs found in the configuration.")

        return extracted

    def _get_current_catalogs(self, api: PolarisDefaultApi) -> Set[str]:
        """
        Get the current catalogs from the API.
        Returns a set of catalog names.
        """
        try:
            # Fetch catalogs from the API
            catalogs = api.list_catalogs().catalogs
            return {catalog.name for catalog in catalogs}
        except Exception as e:
            logger.error(f"Failed to fetch existing catalogs: {e}")
            return set()

    def _create_catalogs(self, api: PolarisDefaultApi):
        """
        Create catalogs based on the setup configuration.
        """
        try:
            # Fetch existing catalogs from the API
            existed_catalogs_name = self._get_current_catalogs(api)

            # Extract catalogs from the configuration
            extracted_catalogs = self._extract_catalogs()
            # Separate new and existing catalogs
            new_catalogs = [
                catalog for catalog in extracted_catalogs
                if catalog['name'] not in existed_catalogs_name
            ]
            skipped_catalogs = [
                catalog['name'] for catalog in extracted_catalogs
                if catalog['name'] in existed_catalogs_name
            ]

            # Log skipped catalogs
            if skipped_catalogs:
                logger.warning(f"Skipping creation for already existing catalogs: {', '.join(skipped_catalogs)}")

            # Create new catalogs
            for catalog in new_catalogs:
                try:
                    logger.info(f"Creating catalog: {catalog['name']}")

                    # Create the catalog command
                    catalog_command = CatalogsCommand(
                        catalogs_subcommand=Subcommands.CREATE,
                        catalog_name=catalog['name'],
                        catalog_type=catalog['type'],
                        storage_type=catalog['storage_type'],
                        default_base_location=catalog['default_base_location'],
                        allowed_locations=catalog['allowed_locations'],
                        role_arn=catalog.get('role_arn'),
                        region=catalog.get('region'),
                        external_id=catalog.get('external_id'),
                        user_arn=catalog.get('user_arn'),
                        tenant_id=catalog.get('tenant_id'),
                        multi_tenant_app_name=catalog.get('multi_tenant_app_name'),
                        consent_url=catalog.get('consent_url'),
                        service_account=catalog.get('service_account'),
                        remote_url=catalog.get('remote_url'),
                        properties=catalog.get('properties', {}),
                        set_properties={},  # Default value
                        remove_properties=[]  # Default value
                    )
                    catalog_command.execute(api)
                    logger.info(f"Catalog '{catalog['name']}' created successfully.")
                except Exception as e:
                    logger.error(f"Failed to create catalog '{catalog['name']}': {e}")
        except Exception as e:
            logger.error(f"Failed to create catalogs: {e}")

    def _extract_catalogs_role(self) -> List[Dict[str, Any]]:
        """
        Extract catalog role information from the setup configuration.
        """
        config = self._load_setup_config()
        catalog_roles = config.get('catalog_roles', [])
        extracted = []
        seen = set()

        for catalog_role in catalog_roles:
            name = catalog_role.get('name')
            catalog_name = catalog_role.get('assignments', {}).get('catalog') if isinstance(catalog_role.get('assignments'), dict) else None
            properties = catalog_role.get('properties', {})

            # Validate required fields
            missing_fields = [field for field, value in {
                "name": name,
                "catalog": catalog_name
            }.items() if not value]

            if missing_fields:
                logger.warning(f"Skipping catalog role due to missing required fields: {', '.join(missing_fields)}. Catalog Role: {catalog_role}")
                continue

            # Check for duplicate catalog roles
            if name in seen:
                logger.warning(f"Skipping duplicate catalog role: '{name}'")
                continue

            seen.add(name)
            extracted.append({
                'name': name,
                'catalog': catalog_name,
                'properties': properties,
            })

        if not extracted:
            logger.warning("No valid catalog roles found in the configuration.")

        return extracted

    def _get_current_catalog_roles(self, api: PolarisDefaultApi) -> Set[str]:
        """
        Get the current catalog roles from the API.
        Returns a set of catalog role names.
        """
        catalog_role_names = set()

        try:
            # Fetch existing catalogs from the API
            existed_catalogs_name = self._get_current_catalogs(api)

            # Fetch catalog roles for each catalog
            for catalog_name in existed_catalogs_name:
                try:
                    roles = api.list_catalog_roles(catalog_name).roles
                    catalog_role_names.update(role.name for role in roles)
                except Exception as e:
                    logger.error(f"Failed to fetch catalog roles for catalog '{catalog_name}': {e}")
                    continue

        except Exception as e:
            logger.error(f"Failed to fetch catalog roles: {e}")

        return catalog_role_names
    
    def _create_catalog_roles(self, api: PolarisDefaultApi):
        """
        Create catalog roles based on the setup configuration.
        """
        try:
            # Fetch existing catalog roles from the API
            existed_catalog_roles = self._get_current_catalog_roles(api)

            # Extract catalog roles from the configuration
            extracted_catalog_roles = self._extract_catalogs_role()

            # Separate new and existing catalog roles
            new_catalog_roles = [
                role for role in extracted_catalog_roles
                if role['name'] not in existed_catalog_roles
            ]
            skipped_catalog_roles = [
                role['name'] for role in extracted_catalog_roles
                if role['name'] in existed_catalog_roles
            ]

            # Log skipped catalog roles
            if skipped_catalog_roles:
                logger.warning(f"Skipping creation for already existing catalog roles: {', '.join(skipped_catalog_roles)}")

            # Create new catalog roles
            for catalog_role in new_catalog_roles:
                try:
                    logger.info(f"Creating catalog role: {catalog_role['name']}")
                    catalog_role_command = CatalogRolesCommand(
                        catalog_roles_subcommand=Subcommands.CREATE,
                        catalog_role_name=catalog_role['name'],
                        principal_role_name='',  # Default value
                        catalog_name=catalog_role.get('catalog'),
                        properties=catalog_role.get('properties', {}),
                        set_properties={},  # Default value
                        remove_properties=[]  # Default value
                    )
                    catalog_role_command.execute(api)
                    logger.info(f"Catalog role '{catalog_role['name']}' created successfully.")
                except Exception as e:
                    logger.error(f"Failed to create catalog role '{catalog_role['name']}': {e}")
        except Exception as e:
            logger.error(f"Failed to create catalog roles: {e}")

    def _extract_catalogs_role_assignments(self) -> List[Dict[str, Any]]:
        """
        Extract catalog-role-to-principal-role assignments from the setup configuration.
        """
        config = self._load_setup_config()
        catalog_roles = config.get('catalog_roles', [])
        assignments = []
        seen = set()

        for catalog_role in catalog_roles:
            catalog_role_name = catalog_role.get('name')
            catalog_assignments = catalog_role.get('assignments', {})
            catalog_name = catalog_assignments.get('catalog') if isinstance(catalog_assignments, dict) else None
            principal_roles = catalog_assignments.get('principal_roles', []) if isinstance(catalog_assignments, dict) else []
            properties = catalog_role.get('properties', {})

            # Validate required fields
            missing_fields = [field for field, value in {
                "catalog_role": catalog_role_name,
                "catalog": catalog_name,
                "principal_roles": principal_roles
            }.items() if not value]

            if missing_fields:
                logger.warning(f"Skipping catalog role assignment due to missing required fields: {', '.join(missing_fields)}. Catalog Role: {catalog_role}")
                continue

            # Check for duplicate assignments
            assignment_key = (catalog_role_name, catalog_name)
            if assignment_key in seen:
                logger.warning(f"Skipping duplicate assignment for catalog role '{catalog_role_name}' in catalog '{catalog_name}'")
                continue

            seen.add(assignment_key)
            assignments.append({
                'catalog_role': catalog_role_name,
                'catalog': catalog_name,
                'principal_roles': principal_roles,
                'properties': properties,
            })

        if not assignments:
            logger.warning("No valid catalog-role-to-principal-role assignments found in the configuration.")

        return assignments

    def _create_catalog_role_assignments(self, api: PolarisDefaultApi):
        """
        Create catalog-role-to-principal-role assignments based on the setup configuration.
        """
        try:
            # Extract assignments from the configuration
            assignments = self._extract_catalogs_role_assignments()

            # Fetch existing catalog roles and principal roles from the API
            existed_catalog_roles = self._get_current_catalog_roles(api)
            existed_principal_roles = self._get_current_principal_roles(api)

            # Process each assignment
            for assignment in assignments:
                catalog_role_name = assignment['catalog_role']
                catalog_name = assignment['catalog']
                principal_roles = assignment['principal_roles']

                # Validate that the catalog role exists
                if catalog_role_name not in existed_catalog_roles:
                    logger.warning(f"Skipping assignment for non-existing catalog role '{catalog_role_name}' in catalog '{catalog_name}'")
                    continue

                # Validate that all principal roles exist
                missing_principal_roles = [role for role in principal_roles if role not in existed_principal_roles]
                if missing_principal_roles:
                    logger.warning(f"Skipping assignment for catalog role '{catalog_role_name}' due to missing principal roles: {', '.join(missing_principal_roles)}")
                    continue

                # Assign principal roles to the catalog role
                for principal_role in principal_roles:
                    try:
                        logger.info(f"Assigning principal role '{principal_role}' to catalog role '{catalog_role_name}' in catalog '{catalog_name}'")
                        catalog_role_command = CatalogRolesCommand(
                            catalog_roles_subcommand=Subcommands.GRANT,
                            catalog_role_name=catalog_role_name,
                            principal_role_name=principal_role,
                            catalog_name=catalog_name,
                            properties=assignment.get('properties', {}),
                            set_properties={},  # Default value
                            remove_properties=[]  # Default value
                        )
                        catalog_role_command.execute(api)
                        logger.info(f"Assigned principal role '{principal_role}' to catalog role '{catalog_role_name}' in catalog '{catalog_name}' successfully.")
                    except Exception as e:
                        logger.error(f"Failed to assign principal role '{principal_role}' to catalog role '{catalog_role_name}' in catalog '{catalog_name}': {e}")
        except Exception as e:
            logger.error(f"Failed to create catalog role assignments: {e}")

    def _get_current_namespaces(self, api: PolarisDefaultApi) -> Dict[str, Set[str]]:
        """
        Get the current namespaces from the API, grouped by catalog.
        Returns a dictionary where the key is the catalog name and the value is a set of namespace names.
        """
        namespaces_by_catalog = {}

        try:
            # Convert PolarisDefaultApi to IcebergCatalogAPI
            catalog_api = NamespacesCommand._get_catalog_api(self, api)

            # Fetch all catalogs
            existed_catalogs_name = self._get_current_catalogs(api)

            # Fetch namespaces for each catalog
            for catalog_name in existed_catalogs_name:
                try:
                    # Fetch namespaces with or without a parent
                    if hasattr(self, 'parent') and self.parent:
                        result = catalog_api.list_namespaces(prefix=catalog_name, parent=UNIT_SEPARATOR.join(self.parent))
                    else:
                        result = catalog_api.list_namespaces(prefix=catalog_name)

                    # Process namespaces
                    namespaces = {'.'.join(namespace) for namespace in result.namespaces}
                    namespaces_by_catalog[catalog_name] = namespaces
                except Exception as e:
                    logger.error(f"Failed to fetch namespaces for catalog '{catalog_name}': {e}")
                    namespaces_by_catalog[catalog_name] = set()

        except Exception as e:
            logger.error(f"Failed to fetch namespaces: {e}")

        return namespaces_by_catalog

    def _extract_namespaces(self) -> List[Dict[str, Any]]:
        """
        Extract namespace information from the setup configuration.
        """
        config = self._load_setup_config()
        namespaces = config.get('namespaces', [])
        extracted = []
        seen = set()

        for namespace in namespaces:
            name = namespace.get('name')
            catalog_name = namespace.get('catalog')
            location = namespace.get('location')
            properties = namespace.get('properties', {})

            # Validate required fields
            missing_fields = [field for field, value in {
                "name": name,
                "catalog": catalog_name
            }.items() if not value]

            if missing_fields:
                logger.warning(f"Skipping namespace due to missing required fields: {', '.join(missing_fields)}. Namespace: {namespace}")
                continue

            # Check for duplicate namespaces
            namespace_key = (catalog_name, name)
            if namespace_key in seen:
                logger.warning(f"Skipping duplicate namespace: '{name}' in catalog '{catalog_name}'")
                continue

            seen.add(namespace_key)
            extracted.append({
                'name': name,
                'catalog': catalog_name,
                'location': location,
                'properties': properties,
            })

        if not extracted:
            logger.warning("No valid namespaces found in the configuration.")

        return extracted

    def _create_namespaces(self, api: PolarisDefaultApi):
        """
        Create namespaces based on the setup configuration.
        """
        try:
            # Fetch existing namespaces grouped by catalog
            existed_namespaces_by_catalog = self._get_current_namespaces(api)

            # Extract namespaces from the configuration
            extracted_namespaces = self._extract_namespaces()

            # Process each namespace
            for namespace in extracted_namespaces:
                catalog_name = namespace['catalog']
                namespace_name = namespace['name']

                # Check if the namespace already exists in the catalog
                if catalog_name in existed_namespaces_by_catalog and namespace_name in existed_namespaces_by_catalog[catalog_name]:
                    logger.info(f"Skipping creation for already existing namespace '{namespace_name}' in catalog '{catalog_name}'")
                    continue

                # Create the namespace
                try:
                    logger.info(f"Creating namespace: '{namespace_name}' in catalog: '{catalog_name}'")
                    namespace_command = NamespacesCommand(
                        namespaces_subcommand=Subcommands.CREATE,
                        catalog=catalog_name,
                        namespace=[namespace_name],
                        parent=[],  # Default value
                        location=namespace.get('location'),
                        properties=namespace.get('properties', {})
                    )
                    namespace_command.execute(api)
                    logger.info(f"Namespace '{namespace_name}' created successfully in catalog '{catalog_name}'.")
                except Exception as e:
                    logger.error(f"Failed to create namespace '{namespace_name}' in catalog '{catalog_name}': {e}")
        except Exception as e:
            logger.error(f"Failed to create namespaces: {e}")

    def _extract_privileges(self) -> List[Dict[str, Any]]:
        """
        Extract privilege information from the setup configuration.
        """
        config = self._load_setup_config()
        privileges = config.get('privileges', [])
        extracted = []
        seen = set()

        for privilege in privileges:
            catalog_role = privilege.get('catalog_role')
            grants = privilege.get('grants', [])

            # Validate required fields for catalog_role
            if not catalog_role:
                logger.warning(f"Skipping privilege due to missing 'catalog_role': {privilege}")
                continue

            for grant in grants:
                level = grant.get('level')
                catalog_name = grant.get('catalog')
                namespace_name = grant.get('namespace')
                privilege_name = grant.get('privilege')
                table_name = grant.get('table')
                view_name = grant.get('view')

                # Validate required fields for each grant
                missing_fields = [field for field, value in {
                    "level": level,
                    "catalog": catalog_name,
                    "privilege": privilege_name
                }.items() if not value]

                if missing_fields:
                    logger.warning(f"Skipping grant due to missing required fields: {', '.join(missing_fields)}. Grant: {grant}")
                    continue

                # Check for duplicate grants
                grant_key = (catalog_role, level, catalog_name, namespace_name, privilege_name, table_name, view_name)
                if grant_key in seen:
                    logger.warning(f"Skipping duplicate grant: {grant_key}")
                    continue

                seen.add(grant_key)

                # Add the grant to the extracted list
                extracted.append({
                    'catalog_role': catalog_role,
                    'level': level,
                    'catalog': catalog_name,
                    'namespace': namespace_name,
                    'privilege': privilege_name,
                    'table': table_name,
                    'view': view_name,
                })

        if not extracted:
            logger.warning("No valid privileges found in the configuration.")

        return extracted

    def _create_privileges(self, api: PolarisDefaultApi):
        """
        Create privileges based on the setup configuration.
        """
        # Extract privileges from the configuration
        extracted_privileges = self._extract_privileges()

        # Fetch existing catalog roles from the API
        existed_catalog_roles = self._get_current_catalog_roles(api)

        # Create privileges
        for privilege in extracted_privileges:
            catalog_role = privilege['catalog_role']
            level = privilege['level']
            catalog_name = privilege['catalog']
            privilege_name = privilege['privilege']

            # Validate that the catalog role exists
            if catalog_role not in existed_catalog_roles:
                logger.warning(f"Skipping privilege assignment for non-existing catalog role '{catalog_role}'")
                continue

            # Map level to privileges_subcommand and set relevant arguments
            if level == "catalog":
                privileges_subcommand = Subcommands.CATALOG
                privilege_args = {
                    "catalog_name": catalog_name,
                    "catalog_role_name": catalog_role,
                    "privilege": privilege_name,
                    "namespace": [],  # Default value
                    "view": "",  # Default value
                    "table": "",  # Default value
                }
            elif level == "namespace":
                privileges_subcommand = Subcommands.NAMESPACE
                privilege_args = {
                    "catalog_name": catalog_name,
                    "catalog_role_name": catalog_role,
                    "namespace": [privilege.get("namespace", "")],
                    "privilege": privilege_name,
                    "view": "",  # Default value
                    "table": "",  # Default value
                }
            elif level == "table":
                privileges_subcommand = Subcommands.TABLE
                privilege_args = {
                    "catalog_name": catalog_name,
                    "catalog_role_name": catalog_role,
                    "namespace": [privilege.get("namespace", "")],
                    "table": privilege.get("table", ""),
                    "privilege": privilege_name,
                    "view": "",  # Default value
                }
            elif level == "view":
                privileges_subcommand = Subcommands.VIEW
                privilege_args = {
                    "catalog_name": catalog_name,
                    "catalog_role_name": catalog_role,
                    "namespace": [privilege.get("namespace", "")],
                    "view": privilege.get("view", ""),
                    "privilege": privilege_name,
                    "table": "",  # Default value
                }
            else:
                logger.warning(f"Skipping privilege assignment due to unsupported level '{level}'")
                continue

            # Assign the privilege
            try:
                logger.info(f"Assigning privilege '{privilege_name}' at level '{level}' for catalog '{catalog_name}' to catalog role '{catalog_role}'")
                privilege_command = PrivilegesCommand(
                    privileges_subcommand=privileges_subcommand,
                    action=Actions.GRANT,
                    cascade=False,  # Default value for cascade
                    **privilege_args
                )
                privilege_command.execute(api)
                logger.info(f"Privilege '{privilege_name}' assigned successfully to catalog role '{catalog_role}'")
            except Exception as e:
                logger.error(f"Failed to assign privilege '{privilege_name}' to catalog role '{catalog_role}': {e}")

    def validate(self):
        pass

    def execute(self, api: PolarisDefaultApi) -> None:
        """
        Execute the setup command based on the subcommand.
        """
        if self.setup_subcommand == Subcommands.CREATE:
            logger.info("=== Starting Setup Process ===")

            # Step 1: Create principals
            logger.info("=== Step 1: Creating Principals ===")
            self._create_principals(api)
            logger.info("Step 1 completed: Principals creation process finished.")

            # Step 2: Create principal roles
            logger.info("=== Step 2: Creating Principal Roles ===")
            self._create_principal_roles(api)
            logger.info("Step 2 completed: Principal roles creation process finished.")

            # Step 3: Assign principal roles
            logger.info("=== Step 3: Assigning Principal Roles ===")
            self._create_principal_role_assignments(api)
            logger.info("Step 3 completed: Principal roles assignment process finished.")

            # Step 4: Create catalogs
            logger.info("=== Step 4: Creating Catalogs ===")
            self._create_catalogs(api)
            logger.info("Step 4 completed: Catalogs creation process finished.")

            # Step 5: Create catalog roles
            logger.info("=== Step 5: Creating Catalog Roles ===")
            self._create_catalog_roles(api)
            logger.info("Step 5 completed: Catalog roles creation process finished.")

            # Step 6: Assign catalog roles
            logger.info("=== Step 6: Assigning Catalog Roles ===")
            self._create_catalog_role_assignments(api)
            logger.info("Step 6 completed: Catalog roles assignment process finished.")

            # Step 7: Create namespaces
            logger.info("=== Step 7: Creating Namespaces ===")
            self._create_namespaces(api)
            logger.info("Step 7 completed: Namespaces creation process finished.")

            # Step 8: Create privileges
            logger.info("=== Step 8: Creating Privileges ===")
            self._create_privileges(api)
            logger.info("Step 8 completed: Privileges creation process finished.")

            logger.info("=== Setup Process Completed Successfully ===")
        else:
            raise Exception(f"Unsupported subcommand: {self.setup_subcommand}")