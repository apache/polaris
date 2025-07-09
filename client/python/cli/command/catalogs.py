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
from typing import Dict, List

from pydantic import StrictStr, SecretStr

from cli.command import Command
from cli.constants import StorageType, CatalogType, CatalogConnectionType, Subcommands, Arguments, AuthenticationType, \
    ServiceIdentityType
from cli.options.option_tree import Argument
from polaris.management import PolarisDefaultApi, CreateCatalogRequest, UpdateCatalogRequest, \
    StorageConfigInfo, ExternalCatalog, AwsStorageConfigInfo, AzureStorageConfigInfo, GcpStorageConfigInfo, \
    PolarisCatalog, CatalogProperties, BearerAuthenticationParameters, \
    OAuthClientCredentialsParameters, SigV4AuthenticationParameters, HadoopConnectionConfigInfo, \
    IcebergRestConnectionConfigInfo, AwsIamServiceIdentityInfo


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
    default_base_location: str
    storage_type: str
    allowed_locations: List[str]
    role_arn: str
    external_id: str
    user_arn: str
    region: str
    tenant_id: str
    multi_tenant_app_name: str
    consent_url: str
    service_account: str
    catalog_name: str
    properties: Dict[str, StrictStr]
    set_properties: Dict[str, StrictStr]
    remove_properties: List[str]
    hadoop_warehouse: str
    iceberg_remote_catalog_name: str
    catalog_connection_type: str
    catalog_authentication_type: str
    catalog_service_identity_type: str
    catalog_service_identity_iam_arn: str
    catalog_uri: str
    catalog_token_uri: str
    catalog_client_id: str
    catalog_client_secret: str
    catalog_client_scopes: List[str]
    catalog_bearer_token: str
    catalog_role_arn: str
    catalog_role_session_name: str
    catalog_external_id: str
    catalog_signing_region: str
    catalog_signing_name: str

    def validate(self):
        if self.catalogs_subcommand == Subcommands.CREATE:
            if self.catalog_type != CatalogType.EXTERNAL.value:
                if not self.storage_type:
                    raise Exception(f'Missing required argument:'
                                    f' {Argument.to_flag_name(Arguments.STORAGE_TYPE)}')
                if not self.default_base_location:
                    raise Exception(f'Missing required argument:'
                                    f' {Argument.to_flag_name(Arguments.DEFAULT_BASE_LOCATION)}')
            else:
                if self.catalog_authentication_type == AuthenticationType.OAUTH.value:
                    if not self.catalog_token_uri or not self.catalog_client_id \
                            or not self.catalog_client_secret or len(self.catalog_client_scopes) == 0:
                        raise Exception(f"Authentication type 'OAUTH' requires"
                                f" {Argument.to_flag_name(Arguments.CATALOG_TOKEN_URI)},"
                                f" {Argument.to_flag_name(Arguments.CATALOG_CLIENT_ID)},"
                                f" {Argument.to_flag_name(Arguments.CATALOG_CLIENT_SECRET)},"
                                f" and at least one {Argument.to_flag_name(Arguments.CATALOG_CLIENT_SCOPE)}.")
                elif self.catalog_authentication_type == AuthenticationType.BEARER.value:
                    if not self.catalog_bearer_token:
                        raise Exception(f"Missing required argument for authentication type 'BEARER':"
                                f" {Argument.to_flag_name(Arguments.CATALOG_BEARER_TOKEN)}")
                elif self.catalog_authentication_type == AuthenticationType.SIGV4.value:
                    if not self.catalog_role_arn or not self.catalog_signing_region:
                        raise Exception(f"Authentication type 'SIGV4 requires"
                                f" {Argument.to_flag_name(Arguments.CATALOG_ROLE_ARN)}"
                                f" and {Argument.to_flag_name(Arguments.CATALOG_SIGNING_REGION)}")

        if self.catalog_service_identity_type == ServiceIdentityType.AWS_IAM.value:
            if not self.catalog_service_identity_iam_arn:
                        raise Exception(f"Missing required argument for service identity type 'AWS_IAM':"
                                f" {Argument.to_flag_name(Arguments.CATALOG_SERVICE_IDENTITY_IAM_ARN)}")

        if self.storage_type == StorageType.S3.value:
            if not self.role_arn:
                raise Exception(
                    f"Missing required argument for storage type 's3':"
                    f" {Argument.to_flag_name(Arguments.ROLE_ARN)}"
                )
            if self._has_azure_storage_info() or self._has_gcs_storage_info():
                raise Exception(
                    f"Storage type 's3' supports the storage credentials"
                    f" {Argument.to_flag_name(Arguments.ROLE_ARN)},"
                    f" {Argument.to_flag_name(Arguments.REGION)},"
                    f" {Argument.to_flag_name(Arguments.EXTERNAL_ID)}, and"
                    f" {Argument.to_flag_name(Arguments.USER_ARN)}"
                )
        elif self.storage_type == StorageType.AZURE.value:
            if not self.tenant_id:
                raise Exception(
                    "Missing required argument for storage type 'azure': "
                    f" {Argument.to_flag_name(Arguments.TENANT_ID)}"
                )
            if self._has_aws_storage_info() or self._has_gcs_storage_info():
                raise Exception(
                    "Storage type 'azure' supports the storage credentials"
                    f" {Argument.to_flag_name(Arguments.TENANT_ID)},"
                    f" {Argument.to_flag_name(Arguments.MULTI_TENANT_APP_NAME)}, and"
                    f" {Argument.to_flag_name(Arguments.CONSENT_URL)}"
                )
        elif self.storage_type == StorageType.GCS.value:
            if self._has_aws_storage_info() or self._has_azure_storage_info():
                raise Exception(
                    "Storage type 'gcs' supports the storage credential"
                    f" {Argument.to_flag_name(Arguments.SERVICE_ACCOUNT)}"
                )
        elif self.storage_type == StorageType.FILE.value:
            if (
                self._has_aws_storage_info()
                or self._has_azure_storage_info()
                or self._has_gcs_storage_info()
            ):
                raise Exception(
                    "Storage type 'file' does not support any storage credentials"
                )

    def _has_aws_storage_info(self):
        return self.role_arn or self.external_id or self.user_arn or self.region

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
                user_arn=self.user_arn,
                region=self.region,
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
                gcs_service_account=self.service_account,
            )
        elif self.storage_type == StorageType.FILE.value:
            config = StorageConfigInfo(
                storage_type=self.storage_type.upper(),
                allowed_locations=self.allowed_locations,
            )
        return config

    def _build_connection_config_info(self):
        if self.catalog_type != CatalogType.EXTERNAL.value:
            return None

        auth_params = None
        if self.catalog_authentication_type == AuthenticationType.OAUTH.value:
            auth_params = OAuthClientCredentialsParameters(
                authentication_type=self.catalog_authentication_type.upper(),
                token_uri=self.catalog_token_uri,
                client_id=self.catalog_client_id,
                client_secret=SecretStr(self.catalog_client_secret),
                scopes=self.catalog_client_scopes
            )
        elif self.catalog_authentication_type == AuthenticationType.BEARER.value:
            auth_params = BearerAuthenticationParameters(
                authentication_type=self.catalog_authentication_type.upper(),
                bearer_token=SecretStr(self.catalog_bearer_token)
            )
        elif self.catalog_authentication_type == AuthenticationType.SIGV4.value:
            auth_params = SigV4AuthenticationParameters(
                authentication_type=self.catalog_authentication_type.upper(),
                role_arn=self.catalog_role_arn,
                role_session_name=self.catalog_role_session_name,
                external_id=self.catalog_external_id,
                signing_region=self.catalog_signing_region,
                signing_name=self.catalog_signing_name,
            )
        elif self.catalog_authentication_type is not None:
            raise Exception("Unknown authentication type:", self.catalog_authentication_type)

        service_identity = None
        if self.catalog_service_identity_type == ServiceIdentityType.AWS_IAM:
            service_identity = AwsIamServiceIdentityInfo(
                identity_type=self.catalog_service_identity_type.upper(),
                iam_arn=self.catalog_service_identity_iam_arn
            )
        elif self.catalog_service_identity_type is not None:
            raise Exception("Unknown service identity type:", self.catalog_service_identity_type)

        config = None
        if self.catalog_connection_type == CatalogConnectionType.HADOOP.value:
            config = HadoopConnectionConfigInfo(
                connection_type=self.catalog_connection_type.upper(),
                uri=self.catalog_uri,
                authentication_parameters=auth_params,
                service_identity=service_identity,
                warehouse=self.hadoop_warehouse
            )
        elif self.catalog_connection_type == CatalogConnectionType.ICEBERG.value:
            config = IcebergRestConnectionConfigInfo(
                connection_type=self.catalog_connection_type.upper().replace('-', '_'),
                uri=self.catalog_uri,
                authentication_parameters=auth_params,
                service_identity=service_identity,
                remote_catalog_name=self.iceberg_remote_catalog_name
            )
        elif self.catalog_connection_type is not None:
            raise Exception("Unknown catalog connection type:", self.catalog_connection_type)
        return config

    def execute(self, api: PolarisDefaultApi) -> None:
        if self.catalogs_subcommand == Subcommands.CREATE:
            storage_config = self._build_storage_config_info()
            connection_config = self._build_connection_config_info()
            if self.catalog_type == CatalogType.EXTERNAL.value:
                request = CreateCatalogRequest(
                    catalog=ExternalCatalog(
                        type=self.catalog_type.upper(),
                        name=self.catalog_name,
                        storage_config_info=storage_config,
                        properties=CatalogProperties(
                            default_base_location=self.default_base_location,
                            additional_properties=self.properties
                        ),
                        connection_config_info=connection_config
                    )
                )
            else:
                request = CreateCatalogRequest(
                    catalog=PolarisCatalog(
                        type=self.catalog_type.upper(),
                        name=self.catalog_name,
                        storage_config_info=storage_config,
                        properties=CatalogProperties(
                            default_base_location=self.default_base_location,
                            additional_properties=self.properties
                        ),
                        connection_config_info=connection_config
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

            if (
                self.default_base_location
                or self.set_properties
                or self.remove_properties
            ):
                new_default_base_location = (
                    self.default_base_location
                    or catalog.properties.default_base_location
                )
                new_additional_properties = (
                    catalog.properties.additional_properties or {}
                )

                # Add or update all entries specified in set_properties
                if self.set_properties:
                    new_additional_properties = {
                        **new_additional_properties,
                        **self.set_properties,
                    }

                # Remove all keys specified in remove_properties
                if self.remove_properties:
                    for to_remove in self.remove_properties:
                        new_additional_properties.pop(to_remove, None)

                catalog.properties = CatalogProperties(
                    default_base_location=new_default_base_location,
                    additional_properties=new_additional_properties,
                )

            if (
                self._has_aws_storage_info()
                or self._has_azure_storage_info()
                or self._has_gcs_storage_info()
                or self.allowed_locations
            ):
                # We must first reconstitute local storage-config related settings from the existing
                # catalog to properly construct the complete updated storage-config
                updated_storage_info = catalog.storage_config_info

                # In order to apply mutations client-side, we can't just use the base
                # _build_storage_config_info helper; instead, each allowed updatable field defined
                # in option_tree.py should be applied individually against the existing
                # storage_config_info here.
                if self.allowed_locations:
                    updated_storage_info.allowed_locations.extend(
                        self.allowed_locations
                    )

                if self.region:
                    # Note: We have to lowercase the returned value because the server enum
                    # is uppercase but we defined the StorageType enums as lowercase.
                    storage_type = updated_storage_info.storage_type
                    if storage_type.lower() != StorageType.S3.value:
                        raise Exception(
                            f"--region requires S3 storage_type, got: {storage_type}"
                        )
                    updated_storage_info.region = self.region

                request = UpdateCatalogRequest(
                    current_entity_version=catalog.entity_version,
                    properties=catalog.properties.to_dict(),
                    storage_config_info=updated_storage_info,
                )
            else:
                request = UpdateCatalogRequest(
                    current_entity_version=catalog.entity_version,
                    properties=catalog.properties.to_dict(),
                )

            api.update_catalog(self.catalog_name, request)
        else:
            raise Exception(f"{self.catalogs_subcommand} is not supported in the CLI")
