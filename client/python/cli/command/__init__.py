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
import argparse
from abc import ABC

from cli.constants import Commands, Arguments
from cli.options.parser import Parser
from polaris.management import PolarisDefaultApi


class Command(ABC):
    """
    An abstract base class for commands. Implementations are expected to override the class methods `validate` and
    `execute`. The static method `Command.from_options` can be used to parse a argparse Namespace into the appropriate
    Command implementation if one exists.
    """

    @staticmethod
    def from_options(options: argparse.Namespace) -> "Command":
        def options_get(key, f=lambda x: x):
            return f(getattr(options, key)) if hasattr(options, key) else None

        properties = Parser.parse_properties(options_get(Arguments.PROPERTY))
        set_properties = Parser.parse_properties(options_get(Arguments.SET_PROPERTY))
        remove_properties = options_get(Arguments.REMOVE_PROPERTY)
        catalog_client_scopes = options_get(Arguments.CATALOG_CLIENT_SCOPE)

        command = None
        if options.command == Commands.CATALOGS:
            from cli.command.catalogs import CatalogsCommand

            command = CatalogsCommand(
                options_get(f"{Commands.CATALOGS}_subcommand"),
                catalog_type=options_get(Arguments.TYPE),
                default_base_location=options_get(Arguments.DEFAULT_BASE_LOCATION),
                storage_type=options_get(Arguments.STORAGE_TYPE),
                allowed_locations=options_get(Arguments.ALLOWED_LOCATION),
                role_arn=options_get(Arguments.ROLE_ARN),
                external_id=options_get(Arguments.EXTERNAL_ID),
                user_arn=options_get(Arguments.USER_ARN),
                region=options_get(Arguments.REGION),
                tenant_id=options_get(Arguments.TENANT_ID),
                multi_tenant_app_name=options_get(Arguments.MULTI_TENANT_APP_NAME),
                consent_url=options_get(Arguments.CONSENT_URL),
                service_account=options_get(Arguments.SERVICE_ACCOUNT),
                catalog_name=options_get(Arguments.CATALOG),
                properties={} if properties is None else properties,
                set_properties={} if set_properties is None else set_properties,
                hadoop_warehouse=options_get(Arguments.HADOOP_WAREHOUSE),
                iceberg_remote_catalog_name=options_get(Arguments.ICEBERG_REMOTE_CATALOG_NAME),
                remove_properties=[] if remove_properties is None else remove_properties,
                catalog_connection_type=options_get(Arguments.CATALOG_CONNECTION_TYPE),
                catalog_authentication_type=options_get(Arguments.CATALOG_AUTHENTICATION_TYPE),
                catalog_service_identity_type=options_get(Arguments.CATALOG_SERVICE_IDENTITY_TYPE),
                catalog_service_identity_iam_arn=options_get(Arguments.CATALOG_SERVICE_IDENTITY_IAM_ARN),
                catalog_uri=options_get(Arguments.CATALOG_URI),
                catalog_token_uri=options_get(Arguments.CATALOG_TOKEN_URI),
                catalog_client_id=options_get(Arguments.CATALOG_CLIENT_ID),
                catalog_client_secret=options_get(Arguments.CATALOG_CLIENT_SECRET),
                catalog_client_scopes=[] if catalog_client_scopes is None else catalog_client_scopes,
                catalog_bearer_token=options_get(Arguments.CATALOG_BEARER_TOKEN),
                catalog_role_arn=options_get(Arguments.CATALOG_ROLE_ARN),
                catalog_role_session_name=options_get(Arguments.CATALOG_ROLE_SESSION_NAME),
                catalog_external_id=options_get(Arguments.CATALOG_EXTERNAL_ID),
                catalog_signing_region=options_get(Arguments.CATALOG_SIGNING_REGION),
                catalog_signing_name=options_get(Arguments.CATALOG_SIGNING_NAME)
            )
        elif options.command == Commands.PRINCIPALS:
            from cli.command.principals import PrincipalsCommand

            command = PrincipalsCommand(
                options_get(f"{Commands.PRINCIPALS}_subcommand"),
                type=options_get(Arguments.TYPE),
                principal_name=options_get(Arguments.PRINCIPAL),
                client_id=options_get(Arguments.CLIENT_ID),
                principal_role=options_get(Arguments.PRINCIPAL_ROLE),
                properties={} if properties is None else properties,
                set_properties={} if set_properties is None else set_properties,
                remove_properties=[]
                if remove_properties is None
                else remove_properties,
            )
        elif options.command == Commands.PRINCIPAL_ROLES:
            from cli.command.principal_roles import PrincipalRolesCommand

            command = PrincipalRolesCommand(
                options_get(f"{Commands.PRINCIPAL_ROLES}_subcommand"),
                principal_role_name=options_get(Arguments.PRINCIPAL_ROLE),
                principal_name=options_get(Arguments.PRINCIPAL),
                catalog_name=options_get(Arguments.CATALOG),
                catalog_role_name=options_get(Arguments.CATALOG_ROLE),
                properties={} if properties is None else properties,
                set_properties={} if set_properties is None else set_properties,
                remove_properties=[]
                if remove_properties is None
                else remove_properties,
            )
        elif options.command == Commands.CATALOG_ROLES:
            from cli.command.catalog_roles import CatalogRolesCommand

            command = CatalogRolesCommand(
                options_get(f"{Commands.CATALOG_ROLES}_subcommand"),
                catalog_name=options_get(Arguments.CATALOG),
                catalog_role_name=options_get(Arguments.CATALOG_ROLE),
                principal_role_name=options_get(Arguments.PRINCIPAL_ROLE),
                properties={} if properties is None else properties,
                set_properties={} if set_properties is None else set_properties,
                remove_properties=[]
                if remove_properties is None
                else remove_properties,
            )
        elif options.command == Commands.PRIVILEGES:
            from cli.command.privileges import PrivilegesCommand

            subcommand = options_get(f"{Commands.PRIVILEGES}_subcommand")
            command = PrivilegesCommand(
                subcommand,
                action=options_get(f"{subcommand}_subcommand"),
                catalog_name=options_get(Arguments.CATALOG),
                catalog_role_name=options_get(Arguments.CATALOG_ROLE),
                namespace=options_get(
                    Arguments.NAMESPACE, lambda s: s.split(".") if s else None
                ),
                view=options_get(Arguments.VIEW),
                table=options_get(Arguments.TABLE),
                privilege=options_get(Arguments.PRIVILEGE),
                cascade=options_get(Arguments.CASCADE),
            )
        elif options.command == Commands.NAMESPACES:
            from cli.command.namespaces import NamespacesCommand

            subcommand = options_get(f"{Commands.NAMESPACES}_subcommand")
            command = NamespacesCommand(
                subcommand,
                catalog=options_get(Arguments.CATALOG),
                namespace=options_get(Arguments.NAMESPACE, lambda s: s.split(".")),
                parent=options_get(
                    Arguments.PARENT, lambda s: s.split(".") if s else None
                ),
                location=options_get(Arguments.LOCATION),
                properties=properties,
            )
        elif options.command == Commands.PROFILES:
            from cli.command.profiles import ProfilesCommand

            subcommand = options_get(f"{Commands.PROFILES}_subcommand")
            command = ProfilesCommand(
                subcommand, profile_name=options_get(Arguments.PROFILE)
            )

        if command is not None:
            command.validate()
            return command
        else:
            raise Exception(
                "Please specify a command or run ./polaris --help to view the available commands"
            )

    def execute(self, api: PolarisDefaultApi) -> None:
        """
        Execute a given command and, where applicable, print the response as JSON.
        """
        raise Exception("`execute` called on abstract `Command`")

    def validate(self) -> None:
        """
        Used to validate a command. Should always be called before `execute`. The arg parser will catch many issues
        with options, but this is used to apply additional constraints that the arg parser can't currently handle.
        One example is that a catalog cannot be created with the `s3` storage type without a `--role-arn` option, but
        one can be created without this flag if it's using the `gcs` storage type.
        """
        raise Exception("`validate` called on abstract `Command`")
