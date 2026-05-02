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
from typing import List, Optional

from apache_polaris.cli.constants import (
    StorageType,
    CatalogType,
    PrincipalType,
    EntityType,
    Hints,
    Commands,
    Arguments,
    Subcommands,
    Actions,
    CatalogConnectionType,
    AuthenticationType,
    ServiceIdentityType,
)


@dataclass
class Argument:
    """
    A data class for representing a single argument within the CLI, such as `--host`.
    """

    name: str
    type: type
    hint: str
    choices: Optional[List[str]] = None
    lower: bool = False
    allow_repeats: bool = False
    default: object = None
    metavar: Optional[str] = None
    group: Optional[str] = None

    def __post_init__(self) -> None:
        if self.name.startswith("--"):
            raise Exception(
                f"Argument name {self.name} starts with `--`: should this be a flag_name?"
            )

    @staticmethod
    def to_flag_name(argument_name: str) -> str:
        return "--" + argument_name.replace("_", "-")

    def get_flag_name(self) -> str:
        return Argument.to_flag_name(self.name)


@dataclass
class Option:
    """
    A data class that represents a subcommand within the CLI, such as `catalogs`. Each Option can have child Options,
    a collection of Arguments, or both.
    """

    name: str
    hint: Optional[str] = None
    input_name: Optional[str] = None
    input_metavar: Optional[str] = None
    args: List[Argument] = field(default_factory=list)
    children: List["Option"] = field(default_factory=list)


class OptionTree:
    """
    `OptionTree.get_tree()` returns the full set of Options supported by the CLI. This structure is used to simplify
    configuration of the CLI and to generate a custom `--help` message including nested commands.
    """

    _CATALOG_ROLE_AND_CATALOG = [
        Argument(Arguments.CATALOG, str, Hints.CATALOG),
        Argument(Arguments.CATALOG_ROLE, str, Hints.CATALOG_ROLE),
    ]

    _FEDERATION_ARGS = [
        Argument(
            Arguments.CATALOG_CONNECTION_TYPE,
            str,
            "External catalog type [ICEBERG-REST, HADOOP, HIVE]",
            lower=True,
            choices=[ct.value for ct in CatalogConnectionType],
            group="External Catalog Federation: General Options",
        ),
        Argument(
            Arguments.ICEBERG_REMOTE_CATALOG_NAME,
            str,
            Hints.ICEBERG_REMOTE_CATALOG_NAME,
            group="External Catalog Federation: General Options",
        ),
        Argument(
            Arguments.HADOOP_WAREHOUSE,
            str,
            Hints.HADOOP_WAREHOUSE,
            group="External Catalog Federation: General Options",
        ),
        Argument(
            Arguments.HIVE_WAREHOUSE,
            str,
            Hints.HIVE_WAREHOUSE,
            group="External Catalog Federation: General Options",
        ),
        Argument(
            Arguments.CATALOG_AUTHENTICATION_TYPE,
            str,
            "Authentication type [OAUTH, BEARER, SIGV4, IMPLICIT]",
            lower=True,
            choices=[at.value for at in AuthenticationType],
            group="External Catalog Federation: General Options",
        ),
        Argument(
            Arguments.CATALOG_SERVICE_IDENTITY_TYPE,
            str,
            "Service identity type [AWS_IAM]",
            lower=True,
            choices=[st.value for st in ServiceIdentityType],
            group="External Catalog Federation: General Options",
        ),
        Argument(
            Arguments.CATALOG_SERVICE_IDENTITY_IAM_ARN,
            str,
            Hints.AWS_IAM_ARN,
            group="External Catalog Federation: AWS IAM Identity Options",
        ),
        Argument(
            Arguments.CATALOG_URI,
            str,
            Hints.EXTERNAL_CATALOG_URI,
            group="External Catalog Federation: General Options",
        ),
        Argument(
            Arguments.CATALOG_TOKEN_URI,
            str,
            Hints.OAUTH_TOKEN_URI,
            group="External Catalog Federation: OAuth Options",
        ),
        Argument(
            Arguments.CATALOG_CLIENT_ID,
            str,
            Hints.OAUTH_CLIENT_ID,
            group="External Catalog Federation: OAuth Options",
        ),
        Argument(
            Arguments.CATALOG_CLIENT_SECRET,
            str,
            Hints.OAUTH_CLIENT_SECRET,
            group="External Catalog Federation: OAuth Options",
        ),
        Argument(
            Arguments.CATALOG_CLIENT_SCOPE,
            str,
            Hints.OAUTH_CLIENT_SCOPE,
            allow_repeats=True,
            group="External Catalog Federation: OAuth Options",
        ),
        Argument(
            Arguments.CATALOG_BEARER_TOKEN,
            str,
            Hints.BEARER_TOKEN,
            group="External Catalog Federation: Bearer Token Options",
        ),
        Argument(
            Arguments.CATALOG_ROLE_ARN,
            str,
            Hints.SIGV4_ROLE_ARN,
            group="External Catalog Federation: AWS SigV4 Options",
        ),
        Argument(
            Arguments.CATALOG_ROLE_SESSION_NAME,
            str,
            Hints.SIGV4_ROLE_SESSION_NAME,
            group="External Catalog Federation: AWS SigV4 Options",
        ),
        Argument(
            Arguments.CATALOG_EXTERNAL_ID,
            str,
            Hints.SIGV4_EXTERNAL_ID,
            group="External Catalog Federation: AWS SigV4 Options",
        ),
        Argument(
            Arguments.CATALOG_SIGNING_REGION,
            str,
            Hints.SIGV4_SIGNING_REGION,
            group="External Catalog Federation: AWS SigV4 Options",
        ),
        Argument(
            Arguments.CATALOG_SIGNING_NAME,
            str,
            Hints.SIGV4_SIGNING_NAME,
            lower=True,
            group="External Catalog Federation: AWS SigV4 Options",
        ),
    ]

    @staticmethod
    def _catalogs_option() -> Option:
        return Option(
            Commands.CATALOGS,
            hint="manage catalogs",
            children=[
                Option(
                    Subcommands.CREATE,
                    hint="Create a new catalog",
                    args=[
                        Argument(
                            Arguments.TYPE,
                            str,
                            "The type of catalog [INTERNAL, EXTERNAL]",
                            lower=True,
                            choices=[ct.value for ct in CatalogType],
                            default=CatalogType.INTERNAL.value,
                        ),
                        Argument(
                            Arguments.STORAGE_TYPE,
                            str,
                            "(Required) The storage type [S3, AZURE, GCS, FILE]",
                            lower=True,
                            choices=[st.value for st in StorageType],
                        ),
                        Argument(
                            Arguments.DEFAULT_BASE_LOCATION,
                            str,
                            "(Required) Default base location for the catalog",
                        ),
                        Argument(
                            Arguments.ENDPOINT,
                            str,
                            Hints.S3_ENDPOINT,
                            group="AWS S3 Storage Options",
                        ),
                        Argument(
                            Arguments.ENDPOINT_INTERNAL,
                            str,
                            Hints.S3_ENDPOINT_INTERNAL,
                            group="AWS S3 Storage Options",
                        ),
                        Argument(
                            Arguments.STS_ENDPOINT,
                            str,
                            Hints.S3_STS_ENDPOINT,
                            group="AWS S3 Storage Options",
                        ),
                        Argument(
                            Arguments.STS_UNAVAILABLE,
                            bool,
                            Hints.S3_STS_UNAVAILABLE,
                            group="AWS S3 Storage Options",
                        ),
                        Argument(
                            Arguments.KMS_UNAVAILABLE,
                            bool,
                            Hints.S3_KMS_UNAVAILABLE,
                            group="AWS S3 Storage Options",
                        ),
                        Argument(
                            Arguments.PATH_STYLE_ACCESS,
                            bool,
                            Hints.S3_PATH_STYLE_ACCESS,
                            group="AWS S3 Storage Options",
                        ),
                        Argument(
                            Arguments.KMS_KEY_CURRENT,
                            str,
                            Hints.S3_KMS_KEY_CURRENT,
                            group="AWS S3 Storage Options",
                        ),
                        Argument(
                            Arguments.KMS_KEY_ALLOWED,
                            str,
                            Hints.S3_KMS_KEY_ALLOWED,
                            allow_repeats=True,
                            group="AWS S3 Storage Options",
                        ),
                        Argument(
                            Arguments.ALLOWED_LOCATION,
                            str,
                            "An allowed location for files tracked by the catalog",
                            allow_repeats=True,
                        ),
                        Argument(
                            Arguments.ROLE_ARN,
                            str,
                            Hints.S3_ROLE_ARN,
                            group="AWS S3 Storage Options",
                        ),
                        Argument(
                            Arguments.REGION,
                            str,
                            Hints.S3_REGION,
                            group="AWS S3 Storage Options",
                        ),
                        Argument(
                            Arguments.EXTERNAL_ID,
                            str,
                            Hints.S3_EXTERNAL_ID,
                            group="AWS S3 Storage Options",
                        ),
                        Argument(
                            Arguments.TENANT_ID,
                            str,
                            Hints.AZURE_TENANT_ID,
                            group="Azure Storage Options",
                        ),
                        Argument(
                            Arguments.MULTI_TENANT_APP_NAME,
                            str,
                            Hints.AZURE_MULTI_TENANT_APP_NAME,
                            group="Azure Storage Options",
                        ),
                        Argument(
                            Arguments.HIERARCHICAL,
                            bool,
                            Hints.AZURE_HIERARCHICAL,
                            group="Azure Storage Options",
                        ),
                        Argument(
                            Arguments.CONSENT_URL,
                            str,
                            Hints.AZURE_CONSENT_URL,
                            group="Azure Storage Options",
                        ),
                        Argument(
                            Arguments.SERVICE_ACCOUNT,
                            str,
                            Hints.GCS_SERVICE_ACCOUNT,
                            group="GCP Storage Options",
                        ),
                        Argument(
                            Arguments.PROPERTY,
                            str,
                            Hints.CATALOG_PROPERTY,
                            allow_repeats=True,
                        ),
                    ]
                    + OptionTree._FEDERATION_ARGS,
                    input_name=Arguments.CATALOG,
                    input_metavar="CATALOG_NAME",
                ),
                Option(
                    Subcommands.DELETE,
                    hint="Delete a catalog",
                    input_name=Arguments.CATALOG,
                    input_metavar="CATALOG_NAME",
                ),
                Option(
                    Subcommands.GET,
                    hint="Retrieve metadata for a catalog",
                    input_name=Arguments.CATALOG,
                    input_metavar="CATALOG_NAME",
                ),
                Option(
                    Subcommands.LIST,
                    hint="List catalogs",
                    args=[
                        Argument(
                            Arguments.PRINCIPAL_ROLE,
                            str,
                            "List only catalogs reachable by this principal role",
                        )
                    ],
                ),
                Option(
                    Subcommands.UPDATE,
                    hint="Update properties of a catalog",
                    args=[
                        Argument(
                            Arguments.DEFAULT_BASE_LOCATION,
                            str,
                            "A new default base location for the catalog",
                        ),
                        Argument(
                            Arguments.ALLOWED_LOCATION,
                            str,
                            "An additional allowed location for files",
                            allow_repeats=True,
                        ),
                        Argument(
                            Arguments.REGION,
                            str,
                            Hints.S3_REGION,
                            group="AWS S3 Storage Options",
                        ),
                        Argument(
                            Arguments.SET_PROPERTY,
                            str,
                            Hints.CATALOG_SET_PROPERTY,
                            allow_repeats=True,
                        ),
                        Argument(
                            Arguments.REMOVE_PROPERTY,
                            str,
                            Hints.REMOVE_PROPERTY,
                            allow_repeats=True,
                        ),
                    ],
                    input_name=Arguments.CATALOG,
                    input_metavar="CATALOG_NAME",
                ),
                Option(
                    Subcommands.SUMMARIZE,
                    hint="Display a summary for a catalog",
                    input_name=Arguments.CATALOG,
                    input_metavar="CATALOG_NAME",
                ),
            ],
        )

    @staticmethod
    def _principals_option() -> Option:
        return Option(
            Commands.PRINCIPALS,
            hint="manage principals",
            children=[
                Option(
                    Subcommands.CREATE,
                    hint="Create a new principal",
                    args=[
                        Argument(
                            Arguments.TYPE,
                            str,
                            "The type of principal [SERVICE]",
                            lower=True,
                            choices=[pt.value for pt in PrincipalType],
                            default=PrincipalType.SERVICE.value,
                        ),
                        Argument(
                            Arguments.PROPERTY,
                            str,
                            Hints.PROPERTY,
                            allow_repeats=True,
                        ),
                    ],
                    input_name=Arguments.PRINCIPAL,
                    input_metavar="PRINCIPAL_NAME",
                ),
                Option(
                    Subcommands.DELETE,
                    hint="Delete a principal",
                    input_name=Arguments.PRINCIPAL,
                    input_metavar="PRINCIPAL_NAME",
                ),
                Option(
                    Subcommands.GET,
                    hint="Retrieve metadata for a principal",
                    input_name=Arguments.PRINCIPAL,
                    input_metavar="PRINCIPAL_NAME",
                ),
                Option(Subcommands.LIST, hint="List principals"),
                Option(
                    Subcommands.ROTATE_CREDENTIALS,
                    hint="Rotate credentials for a principal",
                    input_name=Arguments.PRINCIPAL,
                    input_metavar="PRINCIPAL_NAME",
                ),
                Option(
                    Subcommands.UPDATE,
                    hint="Update properties of a principal",
                    args=[
                        Argument(
                            Arguments.SET_PROPERTY,
                            str,
                            Hints.SET_PROPERTY,
                            allow_repeats=True,
                        ),
                        Argument(
                            Arguments.REMOVE_PROPERTY,
                            str,
                            Hints.REMOVE_PROPERTY,
                            allow_repeats=True,
                        ),
                    ],
                    input_name=Arguments.PRINCIPAL,
                    input_metavar="PRINCIPAL_NAME",
                ),
                Option(
                    Subcommands.ACCESS,
                    hint="Retrieve access details for a principal",
                    input_name=Arguments.PRINCIPAL,
                    input_metavar="PRINCIPAL_NAME",
                ),
                Option(
                    Subcommands.RESET,
                    hint="Reset credentials for a principal",
                    args=[
                        Argument(
                            Arguments.NEW_CLIENT_ID,
                            str,
                            "The new client ID for the principal",
                        ),
                        Argument(
                            Arguments.NEW_CLIENT_SECRET,
                            str,
                            "The new client secret for the principal",
                        ),
                    ],
                    input_name=Arguments.PRINCIPAL,
                    input_metavar="PRINCIPAL_NAME",
                ),
                Option(
                    Subcommands.SUMMARIZE,
                    hint="Display a summary for a principal",
                    input_name=Arguments.PRINCIPAL,
                    input_metavar="PRINCIPAL_NAME",
                ),
            ],
        )

    @staticmethod
    def _principal_roles_option() -> Option:
        return Option(
            Commands.PRINCIPAL_ROLES,
            hint="manage principal roles",
            children=[
                Option(
                    Subcommands.CREATE,
                    hint="Create a new principal role",
                    args=[
                        Argument(
                            Arguments.PROPERTY,
                            str,
                            Hints.PROPERTY,
                            allow_repeats=True,
                        )
                    ],
                    input_name=Arguments.PRINCIPAL_ROLE,
                    input_metavar="PRINCIPAL_ROLE_NAME",
                ),
                Option(
                    Subcommands.DELETE,
                    hint="Delete a principal role",
                    input_name=Arguments.PRINCIPAL_ROLE,
                    input_metavar="PRINCIPAL_ROLE_NAME",
                ),
                Option(
                    Subcommands.GET,
                    hint="Retrieve metadata for a principal role",
                    input_name=Arguments.PRINCIPAL_ROLE,
                    input_metavar="PRINCIPAL_ROLE_NAME",
                ),
                Option(
                    Subcommands.LIST,
                    hint="List principal roles",
                    args=[
                        Argument(
                            Arguments.CATALOG_ROLE,
                            str,
                            "Show only principal roles assigned to this catalog role",
                        ),
                        Argument(
                            Arguments.PRINCIPAL,
                            str,
                            "Show only principal roles assigned to this principal",
                        ),
                    ],
                ),
                Option(
                    Subcommands.UPDATE,
                    hint="Update properties of a principal role",
                    args=[
                        Argument(
                            Arguments.SET_PROPERTY,
                            str,
                            Hints.SET_PROPERTY,
                            allow_repeats=True,
                        ),
                        Argument(
                            Arguments.REMOVE_PROPERTY,
                            str,
                            Hints.REMOVE_PROPERTY,
                            allow_repeats=True,
                        ),
                    ],
                    input_name=Arguments.PRINCIPAL_ROLE,
                    input_metavar="PRINCIPAL_ROLE_NAME",
                ),
                Option(
                    Subcommands.GRANT,
                    hint="Grant a principal role to a principal",
                    args=[
                        Argument(
                            Arguments.PRINCIPAL,
                            str,
                            Hints.PRINCIPAL,
                        )
                    ],
                    input_name=Arguments.PRINCIPAL_ROLE,
                    input_metavar="PRINCIPAL_ROLE_NAME",
                ),
                Option(
                    Subcommands.REVOKE,
                    hint="Revoke a principal role from a principal",
                    args=[
                        Argument(
                            Arguments.PRINCIPAL,
                            str,
                            Hints.PRINCIPAL,
                        )
                    ],
                    input_name=Arguments.PRINCIPAL_ROLE,
                    input_metavar="PRINCIPAL_ROLE_NAME",
                ),
                Option(
                    Subcommands.SUMMARIZE,
                    hint="Display a summary for a principal role",
                    input_name=Arguments.PRINCIPAL_ROLE,
                    input_metavar="PRINCIPAL_ROLE_NAME",
                ),
            ],
        )

    @staticmethod
    def _catalog_roles_option() -> Option:
        return Option(
            Commands.CATALOG_ROLES,
            hint="manage catalog roles",
            children=[
                Option(
                    Subcommands.CREATE,
                    hint="Create a new catalog role",
                    args=[
                        Argument(
                            Arguments.CATALOG,
                            str,
                            Hints.CATALOG,
                        ),
                        Argument(
                            Arguments.PROPERTY,
                            str,
                            Hints.PROPERTY,
                            allow_repeats=True,
                        ),
                    ],
                    input_name=Arguments.CATALOG_ROLE,
                    input_metavar="CATALOG_ROLE_NAME",
                ),
                Option(
                    Subcommands.DELETE,
                    hint="Delete a catalog role",
                    args=[
                        Argument(
                            Arguments.CATALOG,
                            str,
                            Hints.CATALOG,
                        ),
                    ],
                    input_name=Arguments.CATALOG_ROLE,
                    input_metavar="CATALOG_ROLE_NAME",
                ),
                Option(
                    Subcommands.GET,
                    hint="Retrieve metadata for a catalog role",
                    args=[
                        Argument(
                            Arguments.CATALOG,
                            str,
                            Hints.CATALOG,
                        ),
                    ],
                    input_name=Arguments.CATALOG_ROLE,
                    input_metavar="CATALOG_ROLE_NAME",
                ),
                Option(
                    Subcommands.LIST,
                    hint="List catalog roles",
                    args=[
                        Argument(
                            Arguments.PRINCIPAL_ROLE,
                            str,
                            Hints.PRINCIPAL_ROLE,
                        )
                    ],
                    input_name=Arguments.CATALOG,
                    input_metavar="CATALOG_NAME",
                ),
                Option(
                    Subcommands.UPDATE,
                    hint="Update properties of a catalog role",
                    args=[
                        Argument(
                            Arguments.CATALOG,
                            str,
                            Hints.CATALOG,
                        ),
                        Argument(
                            Arguments.SET_PROPERTY,
                            str,
                            Hints.SET_PROPERTY,
                            allow_repeats=True,
                        ),
                        Argument(
                            Arguments.REMOVE_PROPERTY,
                            str,
                            Hints.REMOVE_PROPERTY,
                            allow_repeats=True,
                        ),
                    ],
                    input_name=Arguments.CATALOG_ROLE,
                    input_metavar="CATALOG_ROLE_NAME",
                ),
                Option(
                    Subcommands.GRANT,
                    hint="Grant a catalog role to a principal role",
                    args=[
                        Argument(Arguments.CATALOG, str, Hints.CATALOG),
                        Argument(
                            Arguments.PRINCIPAL_ROLE,
                            str,
                            Hints.PRINCIPAL_ROLE,
                        ),
                    ],
                    input_name=Arguments.CATALOG_ROLE,
                    input_metavar="CATALOG_ROLE_NAME",
                ),
                Option(
                    Subcommands.REVOKE,
                    hint="Revoke a catalog role from a principal role",
                    args=[
                        Argument(Arguments.CATALOG, str, Hints.CATALOG),
                        Argument(
                            Arguments.PRINCIPAL_ROLE,
                            str,
                            Hints.PRINCIPAL_ROLE,
                        ),
                    ],
                    input_name=Arguments.CATALOG_ROLE,
                    input_metavar="CATALOG_ROLE_NAME",
                ),
                Option(
                    Subcommands.SUMMARIZE,
                    hint="Display a summary for a catalog role",
                    args=[Argument(Arguments.CATALOG, str, Hints.CATALOG)],
                    input_name=Arguments.CATALOG_ROLE,
                    input_metavar="CATALOG_ROLE_NAME",
                ),
            ],
        )

    @staticmethod
    def _privileges_option() -> Option:
        return Option(
            Commands.PRIVILEGES,
            hint="manage privileges for a catalog role",
            children=[
                Option(
                    Subcommands.LIST,
                    hint="List privilege grants",
                    args=OptionTree._CATALOG_ROLE_AND_CATALOG,
                ),
                Option(
                    Subcommands.CATALOG,
                    hint="Manage catalog-level privileges",
                    children=[
                        Option(
                            Actions.GRANT,
                            hint="Grant a catalog-level privilege",
                            args=OptionTree._CATALOG_ROLE_AND_CATALOG,
                            input_name=Arguments.PRIVILEGE,
                            input_metavar="PRIVILEGE_NAME",
                        ),
                        Option(
                            Actions.REVOKE,
                            hint="Revoke a catalog-level privilege",
                            args=[
                                Argument(
                                    Arguments.CASCADE,
                                    bool,
                                    "Cascade the revocation to dependent privileges",
                                )
                            ]
                            + OptionTree._CATALOG_ROLE_AND_CATALOG,
                            input_name=Arguments.PRIVILEGE,
                            input_metavar="PRIVILEGE_NAME",
                        ),
                    ],
                ),
                Option(
                    Subcommands.NAMESPACE,
                    hint="Manage namespace-level privileges",
                    children=[
                        Option(
                            Actions.GRANT,
                            hint="Grant a namespace-level privilege",
                            args=[
                                Argument(
                                    Arguments.NAMESPACE,
                                    str,
                                    Hints.NAMESPACE,
                                )
                            ]
                            + OptionTree._CATALOG_ROLE_AND_CATALOG,
                            input_name=Arguments.PRIVILEGE,
                            input_metavar="PRIVILEGE_NAME",
                        ),
                        Option(
                            Actions.REVOKE,
                            hint="Revoke a namespace-level privilege",
                            args=[
                                Argument(
                                    Arguments.NAMESPACE,
                                    str,
                                    Hints.NAMESPACE,
                                ),
                                Argument(
                                    Arguments.CASCADE,
                                    bool,
                                    "Cascade the revocation to dependent privileges",
                                ),
                            ]
                            + OptionTree._CATALOG_ROLE_AND_CATALOG,
                            input_name=Arguments.PRIVILEGE,
                            input_metavar="PRIVILEGE_NAME",
                        ),
                    ],
                ),
                Option(
                    Subcommands.TABLE,
                    hint="Manage table-level privileges",
                    children=[
                        Option(
                            Actions.GRANT,
                            hint="Grant a table-level privilege",
                            args=[
                                Argument(
                                    Arguments.NAMESPACE,
                                    str,
                                    Hints.NAMESPACE,
                                ),
                                Argument(Arguments.TABLE, str, Hints.TABLE),
                            ]
                            + OptionTree._CATALOG_ROLE_AND_CATALOG,
                            input_name=Arguments.PRIVILEGE,
                            input_metavar="PRIVILEGE_NAME",
                        ),
                        Option(
                            Actions.REVOKE,
                            hint="Revoke a table-level privilege",
                            args=[
                                Argument(
                                    Arguments.NAMESPACE,
                                    str,
                                    Hints.NAMESPACE,
                                ),
                                Argument(Arguments.TABLE, str, Hints.TABLE),
                                Argument(
                                    Arguments.CASCADE,
                                    bool,
                                    "Cascade the revocation to dependent privileges",
                                ),
                            ]
                            + OptionTree._CATALOG_ROLE_AND_CATALOG,
                            input_name=Arguments.PRIVILEGE,
                            input_metavar="PRIVILEGE_NAME",
                        ),
                    ],
                ),
                Option(
                    Subcommands.VIEW,
                    hint="Manage view-level privileges",
                    children=[
                        Option(
                            Actions.GRANT,
                            hint="Grant a view-level privilege",
                            args=[
                                Argument(
                                    Arguments.NAMESPACE,
                                    str,
                                    Hints.NAMESPACE,
                                ),
                                Argument(Arguments.VIEW, str, Hints.VIEW),
                            ]
                            + OptionTree._CATALOG_ROLE_AND_CATALOG,
                            input_name=Arguments.PRIVILEGE,
                            input_metavar="PRIVILEGE_NAME",
                        ),
                        Option(
                            Actions.REVOKE,
                            hint="Revoke a view-level privilege",
                            args=[
                                Argument(
                                    Arguments.NAMESPACE,
                                    str,
                                    Hints.NAMESPACE,
                                ),
                                Argument(Arguments.VIEW, str, Hints.VIEW),
                                Argument(
                                    Arguments.CASCADE,
                                    bool,
                                    "Cascade the revocation to dependent privileges",
                                ),
                            ]
                            + OptionTree._CATALOG_ROLE_AND_CATALOG,
                            input_name=Arguments.PRIVILEGE,
                            input_metavar="PRIVILEGE_NAME",
                        ),
                    ],
                ),
            ],
        )

    @staticmethod
    def _namespaces_option() -> Option:
        return Option(
            Commands.NAMESPACES,
            hint="manage namespaces",
            children=[
                Option(
                    Subcommands.CREATE,
                    hint="Create a new namespace",
                    args=[
                        Argument(Arguments.CATALOG, str, Hints.CATALOG),
                        Argument(
                            Arguments.LOCATION,
                            str,
                            "The storage location for the namespace",
                        ),
                        Argument(
                            Arguments.PROPERTY,
                            str,
                            Hints.PROPERTY,
                            allow_repeats=True,
                        ),
                    ],
                    input_name=Arguments.NAMESPACE,
                    input_metavar="NAMESPACE",
                ),
                Option(
                    Subcommands.LIST,
                    hint="List namespaces",
                    args=[
                        Argument(Arguments.CATALOG, str, Hints.CATALOG),
                        Argument(
                            Arguments.PARENT,
                            str,
                            "The parent namespace to list sub-namespaces from",
                        ),
                    ],
                ),
                Option(
                    Subcommands.DELETE,
                    hint="Delete a namespace",
                    args=[Argument(Arguments.CATALOG, str, Hints.CATALOG)],
                    input_name=Arguments.NAMESPACE,
                    input_metavar="NAMESPACE",
                ),
                Option(
                    Subcommands.GET,
                    hint="Retrieve metadata for a namespace",
                    args=[Argument(Arguments.CATALOG, str, Hints.CATALOG)],
                    input_name=Arguments.NAMESPACE,
                    input_metavar="NAMESPACE",
                ),
                Option(
                    Subcommands.SUMMARIZE,
                    hint="Display a summary for a namespace",
                    args=[Argument(Arguments.CATALOG, str, Hints.CATALOG)],
                    input_name=Arguments.NAMESPACE,
                    input_metavar="NAMESPACE",
                ),
            ],
        )

    @staticmethod
    def _profiles_option() -> Option:
        return Option(
            Commands.PROFILES,
            hint="manage profiles",
            children=[
                Option(
                    Subcommands.CREATE,
                    hint="Create a new profile",
                    input_name=Arguments.PROFILE,
                    input_metavar="PROFILE_NAME",
                ),
                Option(
                    Subcommands.DELETE,
                    hint="Delete a profile",
                    input_name=Arguments.PROFILE,
                    input_metavar="PROFILE_NAME",
                ),
                Option(
                    Subcommands.UPDATE,
                    hint="Update properties of a profile",
                    input_name=Arguments.PROFILE,
                    input_metavar="PROFILE_NAME",
                ),
                Option(
                    Subcommands.GET,
                    hint="Retrieve metadata for a profile",
                    input_name=Arguments.PROFILE,
                    input_metavar="PROFILE_NAME",
                ),
                Option(Subcommands.LIST, hint="List profiles"),
            ],
        )

    @staticmethod
    def _policies_option() -> Option:
        return Option(
            Commands.POLICIES,
            hint="manage policies",
            children=[
                Option(
                    Subcommands.CREATE,
                    hint="Create a new policy",
                    args=[
                        Argument(Arguments.CATALOG, str, Hints.CATALOG),
                        Argument(Arguments.NAMESPACE, str, Hints.NAMESPACE),
                        Argument(
                            Arguments.POLICY_FILE,
                            str,
                            "Path to the JSON file containing the policy definition",
                        ),
                        Argument(
                            Arguments.POLICY_TYPE,
                            str,
                            "The type of the policy (e.g., 'system.data-compaction')",
                        ),
                        Argument(
                            Arguments.POLICY_DESCRIPTION,
                            str,
                            "An optional description for the policy",
                        ),
                    ],
                    input_name=Arguments.POLICY,
                    input_metavar="POLICY_NAME",
                ),
                Option(
                    Subcommands.DELETE,
                    hint="Delete a policy",
                    args=[
                        Argument(Arguments.CATALOG, str, Hints.CATALOG),
                        Argument(Arguments.NAMESPACE, str, Hints.NAMESPACE),
                        Argument(
                            Arguments.DETACH_ALL,
                            bool,
                            "Delete the policy and all its attached mappings",
                        ),
                    ],
                    input_name=Arguments.POLICY,
                    input_metavar="POLICY_NAME",
                ),
                Option(
                    Subcommands.GET,
                    hint="Retrieve metadata for a policy",
                    args=[
                        Argument(Arguments.CATALOG, str, Hints.CATALOG),
                        Argument(Arguments.NAMESPACE, str, Hints.NAMESPACE),
                    ],
                    input_name=Arguments.POLICY,
                    input_metavar="POLICY_NAME",
                ),
                Option(
                    Subcommands.LIST,
                    hint="List policies",
                    args=[
                        Argument(Arguments.CATALOG, str, Hints.CATALOG),
                        Argument(Arguments.NAMESPACE, str, Hints.NAMESPACE),
                        Argument(
                            Arguments.TARGET_NAME,
                            str,
                            "The name of the target entity",
                        ),
                        Argument(
                            Arguments.APPLICABLE,
                            bool,
                            "List policies applicable to the target (considering inheritance)",
                        ),
                        Argument(
                            Arguments.POLICY_TYPE,
                            str,
                            "The type of the policy (e.g., 'system.data-compaction')",
                        ),
                    ],
                ),
                Option(
                    Subcommands.UPDATE,
                    hint="Update properties of a policy",
                    args=[
                        Argument(Arguments.CATALOG, str, Hints.CATALOG),
                        Argument(Arguments.NAMESPACE, str, Hints.NAMESPACE),
                        Argument(
                            Arguments.POLICY_FILE,
                            str,
                            "Path to the JSON file containing the policy definition",
                        ),
                        Argument(
                            Arguments.POLICY_DESCRIPTION,
                            str,
                            "An optional description for the policy",
                        ),
                    ],
                    input_name=Arguments.POLICY,
                    input_metavar="POLICY_NAME",
                ),
                Option(
                    Subcommands.ATTACH,
                    hint="Attach a policy to an entity",
                    args=[
                        Argument(Arguments.CATALOG, str, Hints.CATALOG),
                        Argument(Arguments.NAMESPACE, str, Hints.NAMESPACE),
                        Argument(
                            Arguments.ATTACHMENT_TYPE,
                            str,
                            "The type of entity to attach to ('catalog', 'namespace', 'table-like')",
                        ),
                        Argument(
                            Arguments.ATTACHMENT_PATH,
                            str,
                            "The path of the target entity (e.g., 'ns1.tb1')",
                        ),
                        Argument(
                            Arguments.PARAMETERS,
                            str,
                            "Key-value pairs for the attachment (e.g., key=value)",
                            allow_repeats=True,
                        ),
                    ],
                    input_name=Arguments.POLICY,
                    input_metavar="POLICY_NAME",
                ),
                Option(
                    Subcommands.DETACH,
                    hint="Detach a policy from an entity",
                    args=[
                        Argument(Arguments.CATALOG, str, Hints.CATALOG),
                        Argument(Arguments.NAMESPACE, str, Hints.NAMESPACE),
                        Argument(
                            Arguments.ATTACHMENT_TYPE,
                            str,
                            "The type of entity to attach to ('catalog', 'namespace', 'table-like')",
                        ),
                        Argument(
                            Arguments.ATTACHMENT_PATH,
                            str,
                            "The path of the target entity (e.g., 'ns1.tb1')",
                        ),
                        Argument(
                            Arguments.PARAMETERS,
                            str,
                            "Key-value pairs for the attachment (e.g., key=value)",
                            allow_repeats=True,
                        ),
                    ],
                    input_name=Arguments.POLICY,
                    input_metavar="POLICY_NAME",
                ),
            ],
        )

    @staticmethod
    def _setup_option() -> Option:
        return Option(
            Commands.SETUP,
            hint="perform setup",
            children=[
                Option(
                    Subcommands.APPLY,
                    hint="Apply a setup configuration",
                    args=[Argument(Arguments.DRY_RUN, bool, "Run without executing")],
                    input_name=Arguments.SETUP_CONFIG,
                    input_metavar="SETUP_CONFIG_FILE",
                ),
                Option(Subcommands.EXPORT, hint="Export current configuration"),
            ],
        )

    @staticmethod
    def _tables_option() -> Option:
        return Option(
            Commands.TABLES,
            hint="manage tables",
            children=[
                Option(
                    Subcommands.LIST,
                    hint="List tables",
                    args=[
                        Argument(Arguments.CATALOG, str, Hints.CATALOG),
                        Argument(Arguments.NAMESPACE, str, Hints.NAMESPACE),
                    ],
                ),
                Option(
                    Subcommands.GET,
                    hint="Retrieve metadata for a table",
                    args=[
                        Argument(Arguments.CATALOG, str, Hints.CATALOG),
                        Argument(Arguments.NAMESPACE, str, Hints.NAMESPACE),
                    ],
                    input_name=Arguments.TABLE,
                    input_metavar="TABLE_NAME",
                ),
                Option(
                    Subcommands.SUMMARIZE,
                    hint="Display a summary for a table",
                    args=[
                        Argument(Arguments.CATALOG, str, Hints.CATALOG),
                        Argument(Arguments.NAMESPACE, str, Hints.NAMESPACE),
                    ],
                    input_name=Arguments.TABLE,
                    input_metavar="TABLE_NAME",
                ),
                Option(
                    Subcommands.DELETE,
                    hint="De-register a table from catalog",
                    args=[
                        Argument(Arguments.CATALOG, str, Hints.CATALOG),
                        Argument(Arguments.NAMESPACE, str, Hints.NAMESPACE),
                    ],
                    input_name=Arguments.TABLE,
                    input_metavar="TABLE_NAME",
                ),
            ],
        )

    @staticmethod
    def _find_option() -> Option:
        return Option(
            Commands.FIND,
            hint="find an identifier",
            args=[
                Argument(Arguments.CATALOG, str, Hints.CATALOG),
                Argument(
                    Arguments.TYPE,
                    str,
                    "Filter results by entity type",
                    choices=[e.value for e in EntityType],
                ),
            ],
            input_name=Arguments.IDENTIFIER,
            input_metavar="IDENTIFIER",
        )

    @staticmethod
    def get_tree() -> List[Option]:
        return [
            OptionTree._catalogs_option(),
            OptionTree._principals_option(),
            OptionTree._principal_roles_option(),
            OptionTree._catalog_roles_option(),
            OptionTree._privileges_option(),
            OptionTree._namespaces_option(),
            OptionTree._profiles_option(),
            OptionTree._policies_option(),
            OptionTree._setup_option(),
            OptionTree._tables_option(),
            OptionTree._find_option(),
        ]
