from dataclasses import dataclass, field
from typing import List

from cli.constants import StorageType, CatalogType, PrincipalType, Hints, Commands, Arguments, Subcommands, Actions


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
    flag_name = None

    def __post_init__(self):
        if self.name.startswith('--'):
            raise Exception(f'Argument name {self.name} starts with `--`: should this be a flag_name?')

    def get_flag_name(self):
        return self.flag_name or ('--' + self.name.replace('_', '-'))


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
    children: List['Option'] = field(default_factory=list)


class OptionTree:
    """
    `OptionTree.get_tree()` returns the full set of Options supported by the CLI. This structure is used to simplify
    configuration of the CLI and to generate a custom `--help` message including nested commands.
    """

    _STORAGE_CONFIG_INFO = [
        Argument(Arguments.STORAGE_TYPE, str, Hints.Catalogs.Create.STORAGE_TYPE, lower=True,
                 choices=[st.value for st in StorageType]),
        Argument(Arguments.ALLOWED_LOCATION, str, Hints.Catalogs.Create.ALLOWED_LOCATION, allow_repeats=True),
        Argument(Arguments.ROLE_ARN, str, Hints.Catalogs.Create.ROLE_ARN),
        Argument(Arguments.EXTERNAL_ID, str, Hints.Catalogs.Create.EXTERNAL_ID),
        Argument(Arguments.USER_ARN, str, Hints.Catalogs.Create.USER_ARN),
        Argument(Arguments.TENANT_ID, str, Hints.Catalogs.Create.TENANT_ID),
        Argument(Arguments.MULTI_TENANT_APP_NAME, str, Hints.Catalogs.Create.MULTI_TENANT_APP_NAME),
        Argument(Arguments.CONSENT_URL, str, Hints.Catalogs.Create.CONSENT_URL),
        Argument(Arguments.SERVICE_ACCOUNT, str, Hints.Catalogs.Create.SERVICE_ACCOUNT),
    ]

    @staticmethod
    def get_tree() -> List[Option]:
        return [
            Option(Commands.CATALOGS, 'manage catalogs', children=[
                Option(Subcommands.CREATE, args=[
                    Argument(Arguments.TYPE, str, Hints.Catalogs.Create.TYPE, lower=True,
                             choices=[ct.value for ct in CatalogType], default=CatalogType.INTERNAL.value),
                    Argument(Arguments.REMOTE_URL, str, Hints.Catalogs.Create.REMOTE_URL),
                    Argument(Arguments.DEFAULT_BASE_LOCATION, str, Hints.Catalogs.Create.DEFAULT_BASE_LOCATION),
                    Argument(Arguments.PROPERTY, str, Hints.PROPERTY, allow_repeats=True),
                ] + OptionTree._STORAGE_CONFIG_INFO, input_name=Arguments.CATALOG),
                Option(Subcommands.DELETE, input_name=Arguments.CATALOG),
                Option(Subcommands.GET, input_name=Arguments.CATALOG),
                Option(Subcommands.LIST, args=[
                    Argument(Arguments.PRINCIPAL_ROLE, str, Hints.PrincipalRoles.PRINCIPAL_ROLE)
                ]),
                Option(Subcommands.UPDATE, args=[
                    Argument(Arguments.PROPERTY, str, Hints.PROPERTY, allow_repeats=True),
                    Argument(Arguments.DEFAULT_BASE_LOCATION, str, Hints.Catalogs.Create.DEFAULT_BASE_LOCATION),
                ] + OptionTree._STORAGE_CONFIG_INFO, input_name=Arguments.CATALOG)
            ]),
            Option(Commands.PRINCIPALS, 'manage principals', children=[
                Option(Subcommands.CREATE, args=[
                    Argument(Arguments.TYPE, str, Hints.Catalogs.Create.TYPE, lower=True,
                             choices=[pt.value for pt in PrincipalType], default=PrincipalType.SERVICE.value),
                    Argument(Arguments.CLIENT_ID, str, Hints.Principals.Create.CLIENT_ID),
                    Argument(Arguments.PROPERTY, str, Hints.PROPERTY, allow_repeats=True)
                ], input_name=Arguments.PRINCIPAL),
                Option(Subcommands.DELETE, input_name=Arguments.PRINCIPAL),
                Option(Subcommands.GET, input_name=Arguments.PRINCIPAL),
                Option(Subcommands.LIST),
                Option(Subcommands.ROTATE_CREDENTIALS, input_name=Arguments.PRINCIPAL),
                Option(Subcommands.UPDATE, args=[
                    Argument(Arguments.PROPERTY, str, Hints.PROPERTY, allow_repeats=True)
                ], input_name=Arguments.PRINCIPAL)
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
                    Argument(Arguments.PROPERTY, str, Hints.PROPERTY, allow_repeats=True)
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
                    Argument(Arguments.CATALOG, str, Hints.CatalogRoles.Create.CATALOG_NAME),
                    Argument(Arguments.PROPERTY, str, Hints.PROPERTY, allow_repeats=True)
                ], input_name=Arguments.CATALOG_ROLE),
                Option(Subcommands.DELETE, args=[
                    Argument(Arguments.CATALOG, str, Hints.CatalogRoles.Create.CATALOG_NAME),
                ], input_name=Arguments.CATALOG_ROLE),
                Option(Subcommands.GET, args=[
                    Argument(Arguments.CATALOG, str, Hints.CatalogRoles.Create.CATALOG_NAME),
                ], input_name=Arguments.CATALOG_ROLE),
                Option(Subcommands.LIST, hint=Hints.CatalogRoles.LIST, args=[
                    Argument(Arguments.PRINCIPAL_ROLE, str, Hints.PrincipalRoles.PRINCIPAL_ROLE)
                ], input_name=Arguments.CATALOG),
                Option(Subcommands.UPDATE, args=[
                    Argument(Arguments.CATALOG, str, Hints.CatalogRoles.Create.CATALOG_NAME),
                    Argument(Arguments.PROPERTY, str, Hints.PROPERTY, allow_repeats=True)
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
            Option(Commands.PRIVILEGES, 'manage privileges for a catalog role', args=[
                Argument(Arguments.CATALOG, str, Hints.CatalogRoles.Create.CATALOG_NAME),
                Argument(Arguments.CATALOG_ROLE, str, Hints.CatalogRoles.CATALOG_ROLE)
            ], children=[
                Option(Subcommands.LIST),
                Option(Subcommands.CATALOG, children=[
                    Option(Actions.GRANT, input_name=Arguments.PRIVILEGE),
                    Option(Actions.REVOKE, args=[
                        Argument(Arguments.CASCADE, bool, Hints.Grant.CASCADE)
                    ], input_name=Arguments.PRIVILEGE),
                ]),
                Option(Subcommands.NAMESPACE, children=[
                    Option(Actions.GRANT, args=[
                        Argument(Arguments.NAMESPACE, str, Hints.Grant.NAMESPACE)
                    ], input_name=Arguments.PRIVILEGE),
                    Option(Actions.REVOKE, args=[
                        Argument(Arguments.NAMESPACE, str, Hints.Grant.NAMESPACE),
                        Argument(Arguments.CASCADE, bool, Hints.Grant.CASCADE)
                    ], input_name=Arguments.PRIVILEGE),
                ]),
                Option(Subcommands.TABLE, children=[
                    Option(Actions.GRANT, args=[
                        Argument(Arguments.NAMESPACE, str, Hints.Grant.NAMESPACE),
                        Argument(Arguments.TABLE, str, Hints.Grant.TABLE)
                    ], input_name=Arguments.PRIVILEGE),
                    Option(Actions.REVOKE, args=[
                        Argument(Arguments.NAMESPACE, str, Hints.Grant.NAMESPACE),
                        Argument(Arguments.TABLE, str, Hints.Grant.TABLE),
                        Argument(Arguments.CASCADE, bool, Hints.Grant.CASCADE)
                    ], input_name=Arguments.PRIVILEGE),
                ]),
                Option(Subcommands.VIEW, children=[
                    Option(Actions.GRANT, args=[
                        Argument(Arguments.NAMESPACE, str, Hints.Grant.NAMESPACE),
                        Argument(Arguments.VIEW, str, Hints.Grant.VIEW)
                    ], input_name=Arguments.PRIVILEGE),
                    Option(Actions.REVOKE, args=[
                        Argument(Arguments.NAMESPACE, str, Hints.Grant.NAMESPACE),
                        Argument(Arguments.VIEW, str, Hints.Grant.VIEW),
                        Argument(Arguments.CASCADE, bool, Hints.Grant.CASCADE)
                    ], input_name=Arguments.PRIVILEGE),
                ])
            ])
        ]
