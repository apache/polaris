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
from typing import List

from cli.constants import StorageType, CatalogType, PrincipalType, Hints, Commands, Arguments, Subcommands, Actions, \
    CatalogConnectionType, AuthenticationType, ServiceIdentityType


@dataclass
class Argument:
    """
    A data class for representing a single argument within the CLI, such as `--host`.
    """

    name: str
    type: type
    hint: str
    choices: List[str] = None
    lower: bool = False
    allow_repeats: bool = False
    default: object = None

    def __post_init__(self):
        if self.name.startswith("--"):
            raise Exception(
                f"Argument name {self.name} starts with `--`: should this be a flag_name?"
            )

    @staticmethod
    def to_flag_name(argument_name):
        return "--" + argument_name.replace("_", "-")

    def get_flag_name(self):
        return Argument.to_flag_name(self.name)


@dataclass
class Option:
    """
    A data class that represents a subcommand within the CLI, such as `catalogs`. Each Option can have child Options,
    a collection of Arguments, or both.
    """

    name: str
    hint: str = None
    input_name: str = None
    args: List[Argument] = field(default_factory=list)
    children: List["Option"] = field(default_factory=list)


class OptionTree:
    """
    `OptionTree.get_tree()` returns the full set of Options supported by the CLI. This structure is used to simplify
    configuration of the CLI and to generate a custom `--help` message including nested commands.
    """

    _CATALOG_ROLE_AND_CATALOG = [
        Argument(Arguments.CATALOG, str, Hints.CatalogRoles.CATALOG_NAME),
        Argument(Arguments.CATALOG_ROLE, str, Hints.CatalogRoles.CATALOG_ROLE),
    ]

    _FEDERATION_ARGS = [
        Argument(Arguments.CATALOG_CONNECTION_TYPE, str,
                 Hints.Catalogs.External.CATALOG_CONNECTION_TYPE, lower=True,
                 choices=[ct.value for ct in CatalogConnectionType]),
        Argument(Arguments.ICEBERG_REMOTE_CATALOG_NAME, str,
                 Hints.Catalogs.External.ICEBERG_REMOTE_CATALOG_NAME),
        Argument(Arguments.HADOOP_WAREHOUSE, str,
                 Hints.Catalogs.External.HADOOP_WAREHOUSE),
        Argument(Arguments.CATALOG_AUTHENTICATION_TYPE, str,
                 Hints.Catalogs.External.CATALOG_AUTHENTICATION_TYPE, lower=True,
                 choices=[at.value for at in AuthenticationType]),
        Argument(Arguments.CATALOG_SERVICE_IDENTITY_TYPE, str,
                 Hints.Catalogs.External.CATALOG_SERVICE_IDENTITY_TYPE, lower=True,
                 choices=[st.value for st in ServiceIdentityType]),
        Argument(Arguments.CATALOG_SERVICE_IDENTITY_IAM_ARN, str,
                 Hints.Catalogs.External.CATALOG_SERVICE_IDENTITY_IAM_ARN),
        Argument(Arguments.CATALOG_URI, str, Hints.Catalogs.External.CATALOG_URI),
        Argument(Arguments.CATALOG_TOKEN_URI, str, Hints.Catalogs.External.CATALOG_TOKEN_URI),
        Argument(Arguments.CATALOG_CLIENT_ID, str, Hints.Catalogs.External.CATALOG_CLIENT_ID),
        Argument(Arguments.CATALOG_CLIENT_SECRET, str, Hints.Catalogs.External.CATALOG_CLIENT_SECRET),
        Argument(Arguments.CATALOG_CLIENT_SCOPE, str,
                 Hints.Catalogs.External.CATALOG_CLIENT_SCOPE, allow_repeats=True),
        Argument(Arguments.CATALOG_BEARER_TOKEN, str, Hints.Catalogs.External.CATALOG_BEARER_TOKEN),
        Argument(Arguments.CATALOG_ROLE_ARN, str, Hints.Catalogs.External.CATALOG_ROLE_ARN),
        Argument(Arguments.CATALOG_ROLE_SESSION_NAME, str, Hints.Catalogs.External.CATALOG_ROLE_SESSION_NAME),
        Argument(Arguments.CATALOG_EXTERNAL_ID, str, Hints.Catalogs.External.CATALOG_EXTERNAL_ID),
        Argument(Arguments.CATALOG_SIGNING_REGION, str, Hints.Catalogs.External.CATALOG_SIGNING_REGION),
        Argument(Arguments.CATALOG_SIGNING_NAME, str, Hints.Catalogs.External.CATALOG_SIGNING_NAME, lower=True)
    ]

    @staticmethod
    def get_tree() -> List[Option]:
        return [
            Option(Commands.CATALOGS, 'manage catalogs', children=[
                Option(Subcommands.CREATE, args=[
                    Argument(Arguments.TYPE, str, Hints.Catalogs.Create.TYPE, lower=True,
                             choices=[ct.value for ct in CatalogType], default=CatalogType.INTERNAL.value),
                    Argument(Arguments.STORAGE_TYPE, str, Hints.Catalogs.Create.STORAGE_TYPE, lower=True,
                             choices=[st.value for st in StorageType]),
                    Argument(Arguments.DEFAULT_BASE_LOCATION, str, Hints.Catalogs.Create.DEFAULT_BASE_LOCATION),
                    Argument(Arguments.ALLOWED_LOCATION, str, Hints.Catalogs.Create.ALLOWED_LOCATION,
                             allow_repeats=True),
                    Argument(Arguments.ROLE_ARN, str, Hints.Catalogs.Create.ROLE_ARN),
                    Argument(Arguments.REGION, str, Hints.Catalogs.Create.REGION),
                    Argument(Arguments.EXTERNAL_ID, str, Hints.Catalogs.Create.EXTERNAL_ID),
                    Argument(Arguments.TENANT_ID, str, Hints.Catalogs.Create.TENANT_ID),
                    Argument(Arguments.MULTI_TENANT_APP_NAME, str, Hints.Catalogs.Create.MULTI_TENANT_APP_NAME),
                    Argument(Arguments.CONSENT_URL, str, Hints.Catalogs.Create.CONSENT_URL),
                    Argument(Arguments.SERVICE_ACCOUNT, str, Hints.Catalogs.Create.SERVICE_ACCOUNT),
                    Argument(Arguments.PROPERTY, str, Hints.PROPERTY, allow_repeats=True),
                ] + OptionTree._FEDERATION_ARGS, input_name=Arguments.CATALOG),
                Option(Subcommands.DELETE, input_name=Arguments.CATALOG),
                Option(Subcommands.GET, input_name=Arguments.CATALOG),
                Option(Subcommands.LIST, args=[
                    Argument(Arguments.PRINCIPAL_ROLE, str, Hints.PrincipalRoles.PRINCIPAL_ROLE)
                ]),
                Option(Subcommands.UPDATE, args=[
                    Argument(Arguments.DEFAULT_BASE_LOCATION, str, Hints.Catalogs.Update.DEFAULT_BASE_LOCATION),
                    Argument(Arguments.ALLOWED_LOCATION, str, Hints.Catalogs.Create.ALLOWED_LOCATION,
                             allow_repeats=True),
                    Argument(Arguments.REGION, str, Hints.Catalogs.Create.REGION),
                    Argument(Arguments.SET_PROPERTY, str, Hints.SET_PROPERTY, allow_repeats=True),
                    Argument(Arguments.REMOVE_PROPERTY, str, Hints.REMOVE_PROPERTY, allow_repeats=True),
                ], input_name=Arguments.CATALOG)
            ]),
            Option(Commands.PRINCIPALS, 'manage principals', children=[
                Option(Subcommands.CREATE, args=[
                    Argument(Arguments.TYPE, str, Hints.Principals.Create.TYPE, lower=True,
                             choices=[pt.value for pt in PrincipalType], default=PrincipalType.SERVICE.value),
                    Argument(Arguments.PROPERTY, str, Hints.PROPERTY, allow_repeats=True)
                ], input_name=Arguments.PRINCIPAL),
                Option(Subcommands.DELETE, input_name=Arguments.PRINCIPAL),
                Option(Subcommands.GET, input_name=Arguments.PRINCIPAL),
                Option(Subcommands.LIST),
                Option(Subcommands.ROTATE_CREDENTIALS, input_name=Arguments.PRINCIPAL),
                Option(Subcommands.UPDATE, args=[
                    Argument(Arguments.SET_PROPERTY, str, Hints.SET_PROPERTY, allow_repeats=True),
                    Argument(Arguments.REMOVE_PROPERTY, str, Hints.REMOVE_PROPERTY, allow_repeats=True),
                ], input_name=Arguments.PRINCIPAL),
                Option(Subcommands.ACCESS, input_name=Arguments.PRINCIPAL),
            ]),
            Option(Commands.PRINCIPAL_ROLES, 'manage principal roles', children=[
                Option(Subcommands.CREATE, args=[
                    Argument(Arguments.PROPERTY, str, Hints.PROPERTY, allow_repeats=True)
                ], input_name=Arguments.PRINCIPAL_ROLE),
                Option(Subcommands.DELETE, input_name=Arguments.PRINCIPAL_ROLE),
                Option(Subcommands.GET, input_name=Arguments.PRINCIPAL_ROLE),
                Option(Subcommands.LIST, hint=Hints.PrincipalRoles.LIST, args=[
                    Argument(Arguments.CATALOG_ROLE, str, Hints.PrincipalRoles.List.CATALOG_ROLE),
                    Argument(Arguments.PRINCIPAL, str, Hints.PrincipalRoles.List.PRINCIPAL_NAME)
                ]),
                Option(Subcommands.UPDATE, args=[
                    Argument(Arguments.SET_PROPERTY, str, Hints.SET_PROPERTY, allow_repeats=True),
                    Argument(Arguments.REMOVE_PROPERTY, str, Hints.REMOVE_PROPERTY, allow_repeats=True),
                ], input_name=Arguments.PRINCIPAL_ROLE),
                Option(Subcommands.GRANT, hint=Hints.PrincipalRoles.GRANT, args=[
                    Argument(Arguments.PRINCIPAL, str, Hints.PrincipalRoles.Grant.PRINCIPAL)
                ], input_name=Arguments.PRINCIPAL_ROLE),
                Option(Subcommands.REVOKE, hint=Hints.PrincipalRoles.REVOKE, args=[
                    Argument(Arguments.PRINCIPAL, str, Hints.PrincipalRoles.Revoke.PRINCIPAL)
                ], input_name=Arguments.PRINCIPAL_ROLE)
            ]),
            Option(Commands.CATALOG_ROLES, 'manage catalog roles', children=[
                Option(Subcommands.CREATE, args=[
                    Argument(Arguments.CATALOG, str, Hints.CatalogRoles.CATALOG_NAME),
                    Argument(Arguments.PROPERTY, str, Hints.PROPERTY, allow_repeats=True)
                ], input_name=Arguments.CATALOG_ROLE),
                Option(Subcommands.DELETE, args=[
                    Argument(Arguments.CATALOG, str, Hints.CatalogRoles.CATALOG_NAME),
                ], input_name=Arguments.CATALOG_ROLE),
                Option(Subcommands.GET, args=[
                    Argument(Arguments.CATALOG, str, Hints.CatalogRoles.CATALOG_NAME),
                ], input_name=Arguments.CATALOG_ROLE),
                Option(Subcommands.LIST, hint=Hints.CatalogRoles.LIST, args=[
                    Argument(Arguments.PRINCIPAL_ROLE, str, Hints.PrincipalRoles.PRINCIPAL_ROLE)
                ], input_name=Arguments.CATALOG),
                Option(Subcommands.UPDATE, args=[
                    Argument(Arguments.CATALOG, str, Hints.CatalogRoles.CATALOG_NAME),
                    Argument(Arguments.SET_PROPERTY, str, Hints.SET_PROPERTY, allow_repeats=True),
                    Argument(Arguments.REMOVE_PROPERTY, str, Hints.REMOVE_PROPERTY, allow_repeats=True),
                ], input_name=Arguments.CATALOG_ROLE),
                Option(Subcommands.GRANT, hint=Hints.CatalogRoles.GRANT_CATALOG_ROLE, args=[
                    Argument(Arguments.CATALOG, str, Hints.CatalogRoles.CATALOG_NAME),
                    Argument(Arguments.PRINCIPAL_ROLE, str, Hints.CatalogRoles.CATALOG_ROLE)
                ], input_name=Arguments.CATALOG_ROLE),
                Option(Subcommands.REVOKE, hint=Hints.CatalogRoles.GRANT_CATALOG_ROLE, args=[
                    Argument(Arguments.CATALOG, str, Hints.CatalogRoles.CATALOG_NAME),
                    Argument(Arguments.PRINCIPAL_ROLE, str, Hints.CatalogRoles.CATALOG_ROLE)
                ], input_name=Arguments.CATALOG_ROLE)
            ]),
            Option(Commands.PRIVILEGES, 'manage privileges for a catalog role', children=[
                Option(Subcommands.LIST, args=OptionTree._CATALOG_ROLE_AND_CATALOG),
                Option(Subcommands.CATALOG, children=[
                    Option(Actions.GRANT, args=OptionTree._CATALOG_ROLE_AND_CATALOG, input_name=Arguments.PRIVILEGE),
                    Option(Actions.REVOKE, args=[
                        Argument(Arguments.CASCADE, bool, Hints.Grant.CASCADE)
                    ] + OptionTree._CATALOG_ROLE_AND_CATALOG, input_name=Arguments.PRIVILEGE),
                ]),
                Option(Subcommands.NAMESPACE, children=[
                    Option(Actions.GRANT, args=[
                        Argument(Arguments.NAMESPACE, str, Hints.Grant.NAMESPACE)
                    ] + OptionTree._CATALOG_ROLE_AND_CATALOG, input_name=Arguments.PRIVILEGE),
                    Option(Actions.REVOKE, args=[
                        Argument(Arguments.NAMESPACE, str, Hints.Grant.NAMESPACE),
                        Argument(Arguments.CASCADE, bool, Hints.Grant.CASCADE)
                    ] + OptionTree._CATALOG_ROLE_AND_CATALOG, input_name=Arguments.PRIVILEGE),
                ]),
                Option(Subcommands.TABLE, children=[
                    Option(Actions.GRANT, args=[
                        Argument(Arguments.NAMESPACE, str, Hints.Grant.NAMESPACE),
                        Argument(Arguments.TABLE, str, Hints.Grant.TABLE)
                    ] + OptionTree._CATALOG_ROLE_AND_CATALOG, input_name=Arguments.PRIVILEGE),
                    Option(Actions.REVOKE, args=[
                        Argument(Arguments.NAMESPACE, str, Hints.Grant.NAMESPACE),
                        Argument(Arguments.TABLE, str, Hints.Grant.TABLE),
                        Argument(Arguments.CASCADE, bool, Hints.Grant.CASCADE)
                    ] + OptionTree._CATALOG_ROLE_AND_CATALOG, input_name=Arguments.PRIVILEGE),
                ]),
                Option(Subcommands.VIEW, children=[
                    Option(Actions.GRANT, args=[
                        Argument(Arguments.NAMESPACE, str, Hints.Grant.NAMESPACE),
                        Argument(Arguments.VIEW, str, Hints.Grant.VIEW)
                    ] + OptionTree._CATALOG_ROLE_AND_CATALOG, input_name=Arguments.PRIVILEGE),
                    Option(Actions.REVOKE, args=[
                        Argument(Arguments.NAMESPACE, str, Hints.Grant.NAMESPACE),
                        Argument(Arguments.VIEW, str, Hints.Grant.VIEW),
                        Argument(Arguments.CASCADE, bool, Hints.Grant.CASCADE)
                    ] + OptionTree._CATALOG_ROLE_AND_CATALOG, input_name=Arguments.PRIVILEGE),
                ])
            ]),
            Option(Commands.NAMESPACES, 'manage namespaces', children=[
                Option(Subcommands.CREATE, args=[
                    Argument(Arguments.CATALOG, str, Hints.CatalogRoles.CATALOG_NAME),
                    Argument(Arguments.LOCATION, str, Hints.Namespaces.LOCATION),
                    Argument(Arguments.PROPERTY, str, Hints.PROPERTY, allow_repeats=True)
                ], input_name=Arguments.NAMESPACE),
                Option(Subcommands.LIST, args=[
                    Argument(Arguments.CATALOG, str, Hints.CatalogRoles.CATALOG_NAME),
                    Argument(Arguments.PARENT, str, Hints.Namespaces.PARENT)
                ]),
                Option(Subcommands.DELETE, args=[
                    Argument(Arguments.CATALOG, str, Hints.CatalogRoles.CATALOG_NAME)
                ], input_name=Arguments.NAMESPACE),
                Option(Subcommands.GET, args=[
                    Argument(Arguments.CATALOG, str, Hints.CatalogRoles.CATALOG_NAME)
                ], input_name=Arguments.NAMESPACE),
            ]),
            Option(Commands.PROFILES, 'manage profiles', children=[
                Option(Subcommands.CREATE, input_name=Arguments.PROFILE),
                Option(Subcommands.DELETE, input_name=Arguments.PROFILE),
                Option(Subcommands.UPDATE, input_name=Arguments.PROFILE),
                Option(Subcommands.GET, input_name=Arguments.PROFILE),
                Option(Subcommands.LIST),
            ])
        ]
