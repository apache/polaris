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
from typing import Dict, Optional, List

from pydantic import StrictStr

from cli.command import Command
from cli.constants import StorageType, CatalogType, Subcommands, Arguments
from cli.options.option_tree import Argument
from polaris.management import PolarisDefaultApi, Catalog, CreateCatalogRequest, UpdateCatalogRequest, \
    StorageConfigInfo, ExternalCatalog, AwsStorageConfigInfo, AzureStorageConfigInfo, GcpStorageConfigInfo, \
    PolarisCatalog, CatalogProperties


@dataclass
class CatalogsCommand(Command):
    """
    A Command implementation to represent `polaris catalogs`. The instance attributes correspond to parameters
    that can be provided to various subcommands, except `catalogs_subcommand` which represents the subcommand
    itself.

    Example commands:
        * ./polaris catalogs create cat_name --storage-type s3 --default-base-location s3://bucket/path --role-arn ...
        * ./polaris catalogs update cat_name --default-base-location s3://new-bucket/new-location
        * ./polaris catalogs list
    """

    catalogs_subcommand: str
    catalog_type: str
    remote_url: str
    default_base_location: str
    storage_type: str
    allowed_locations: List[str]
    role_arn: str
    external_id: str
    user_arn: str
    tenant_id: str
    multi_tenant_app_name: str
    consent_url: str
    service_account: str
    catalog_name: str
    properties: Dict[str, StrictStr]

    def validate(self):
        if self.catalogs_subcommand == Subcommands.CREATE:
            if not self.storage_type:
                raise Exception(f'Missing required argument:'
                                f' {Argument.to_flag_name(Arguments.STORAGE_TYPE)}')
            if not self.default_base_location:
                raise Exception(f'Missing required argument:'
                                f' {Argument.to_flag_name(Arguments.DEFAULT_BASE_LOCATION)}')
        if self.catalogs_subcommand == Subcommands.UPDATE:
            if self.allowed_locations:
                if not self.storage_type:
                    raise Exception(f'Missing required argument when updating allowed locations for a catalog:'
                                    f' {Argument.to_flag_name(Arguments.STORAGE_TYPE)}')

        if self.storage_type == StorageType.S3.value:
            if not self.role_arn:
                raise Exception(f"Missing required argument for storage type 's3':"
                                f" {Argument.to_flag_name(Arguments.ROLE_ARN)}")
            if self._has_azure_storage_info() or self._has_gcs_storage_info():
                raise Exception(f"Storage type 's3' supports the storage credentials"
                                f" {Argument.to_flag_name(Arguments.ROLE_ARN)},"
                                f" {Argument.to_flag_name(Arguments.EXTERNAL_ID)}, and"
                                f" {Argument.to_flag_name(Arguments.USER_ARN)}")
        elif self.storage_type == StorageType.AZURE.value:
            if not self.tenant_id:
                raise Exception("Missing required argument for storage type 'azure': "
                                f" {Argument.to_flag_name(Arguments.TENANT_ID)}")
            if self._has_aws_storage_info() or self._has_gcs_storage_info():
                raise Exception("Storage type 'azure' supports the storage credentials"
                                f" {Argument.to_flag_name(Arguments.TENANT_ID)},"
                                f" {Argument.to_flag_name(Arguments.MULTI_TENANT_APP_NAME)}, and"
                                f" {Argument.to_flag_name(Arguments.CONSENT_URL)}")
        elif self.storage_type == StorageType.GCS.value:
            if self._has_aws_storage_info() or self._has_azure_storage_info():
                raise Exception("Storage type 'gcs' supports the storage credential"
                                f" {Argument.to_flag_name(Arguments.SERVICE_ACCOUNT)}")
        elif self.storage_type == StorageType.FILE.value:
            if self._has_aws_storage_info() or self._has_azure_storage_info() or self._has_gcs_storage_info():
                raise Exception("Storage type 'file' does not support any storage credentials")

    def _has_aws_storage_info(self):
        return self.role_arn or self.external_id or self.user_arn

    def _has_azure_storage_info(self):
        return self.tenant_id or self.multi_tenant_app_name or self.consent_url

    def _has_gcs_storage_info(self):
        return self.service_account

    def _build_storage_config_info(self):
        config = None
        if self.storage_type == StorageType.S3.value:
            config = AwsStorageConfigInfo(
                storage_type=self.storage_type.upper(),
                allowed_locations=self.allowed_locations,
                role_arn=self.role_arn,
                external_id=self.external_id,
                user_arn=self.user_arn
            )
        elif self.storage_type == StorageType.AZURE.value:
            config = AzureStorageConfigInfo(
                storage_type=self.storage_type.upper(),
                allowed_locations=self.allowed_locations,
                tenant_id=self.tenant_id,
                multi_tenant_app_name=self.multi_tenant_app_name,
                consent_url=self.consent_url,
            )
        elif self.storage_type == StorageType.GCS.value:
            config = GcpStorageConfigInfo(
                storage_type=self.storage_type.upper(),
                allowed_locations=self.allowed_locations,
                gcs_service_account=self.service_account
            )
        elif self.storage_type == StorageType.FILE.value:
            config = StorageConfigInfo(
                storage_type=self.storage_type.upper(),
                allowed_locations=self.allowed_locations
            )
        return config

    def execute(self, api: PolarisDefaultApi) -> None:
        if self.catalogs_subcommand == Subcommands.CREATE:
            config = self._build_storage_config_info()
            if self.catalog_type == CatalogType.EXTERNAL.value:
                request = CreateCatalogRequest(
                    catalog=ExternalCatalog(
                        type=self.catalog_type.upper(),
                        name=self.catalog_name,
                        storage_config_info=config,
                        remote_url=self.remote_url,
                        properties=CatalogProperties(
                            default_base_location=self.default_base_location,
                            additional_properties=self.properties
                        )
                    )
                )
            else:
                request = CreateCatalogRequest(
                    catalog=PolarisCatalog(
                        type=self.catalog_type.upper(),
                        name=self.catalog_name,
                        storage_config_info=config,
                        properties=CatalogProperties(
                            default_base_location=self.default_base_location,
                            additional_properties=self.properties
                        )
                    )
                )
            api.create_catalog(request)
        elif self.catalogs_subcommand == Subcommands.DELETE:
            api.delete_catalog(self.catalog_name)
        elif self.catalogs_subcommand == Subcommands.GET:
            print(api.get_catalog(self.catalog_name).to_json())
        elif self.catalogs_subcommand == Subcommands.LIST:
            for catalog in api.list_catalogs().catalogs:
                print(catalog.to_json())
        elif self.catalogs_subcommand == Subcommands.UPDATE:
            catalog = api.get_catalog(self.catalog_name)
            if self.default_base_location or self.properties:
                catalog.properties = CatalogProperties(
                    default_base_location=self.default_base_location,
                    additional_properties=self.properties
                )
            request = UpdateCatalogRequest(
                current_entity_version=catalog.entity_version,
                catalog=catalog
            )
            if (self._has_aws_storage_info() or self._has_azure_storage_info() or self._has_gcs_storage_info() or
                    self.allowed_locations or self.default_base_location):
                request = UpdateCatalogRequest(
                    current_entity_version=catalog.entity_version,
                    catalog=catalog,
                    storage_config_info=self._build_storage_config_info()
                )

            api.update_catalog(self.catalog_name, request)
        else:
            raise Exception(f'{self.catalogs_subcommand} is not supported in the CLI')

