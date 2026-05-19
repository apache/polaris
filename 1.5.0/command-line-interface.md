---
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
title: Command Line Interface
type: docs
weight: 300
---

In order to help administrators quickly set up and manage their Polaris server, Polaris provides a simple command-line interface (CLI) for common tasks.

The basic syntax of the Polaris CLI is outlined below:

```
usage: polaris [-h] [options] COMMAND ...

options:
  -h, --help                     show this help message and exit

Global Options:
  --host HOST                    Polaris server hostname
  --port PORT                    Polaris server port
  --base-url BASE_URL            Complete base URL (overrides host/port)
  --client-id CLIENT_ID          OAuth client ID
  --client-secret CLIENT_SECRET  OAuth client secret
  --access-token ACCESS_TOKEN    OAuth access token
  --realm REALM                  Polaris realm (default: from server)
  --header HEADER                Context header name (default: Polaris-Realm)
  --profile PROFILE              Polaris profile name
  --proxy PROXY                  Proxy URL
  --debug                        Enable debug mode
```

`COMMAND` must be one of the following:
1. catalogs
2. principals
3. principal-roles
4. catalog-roles
5. namespaces
6. privileges
7. profiles
8. policies
9. repair
10. setup
11. find
12. tables

Each _command_ supports several _subcommands_, and some _subcommands_ have _actions_ that come after the subcommand in turn. Finally, _arguments_ follow to form a full invocation. Within a set of named arguments at the end of an invocation ordering is generally not important. Many invocations also have a required positional argument of the type that the _command_ refers to. Again, the ordering of this positional argument relative to named arguments is not important.

Some example full invocations:

```
polaris principals list
polaris catalogs delete some_catalog_name
polaris catalogs update --set-property foo=bar some_other_catalog
polaris catalogs update another_catalog --set-property k=v
polaris privileges namespace grant --namespace some.schema --catalog fourth_catalog --catalog-role some_catalog_role TABLE_READ_DATA
polaris profiles list
polaris policies list --catalog some_catalog --namespace some.schema
polaris repair
polaris setup apply setup-config.yaml
polaris find some_table
polaris tables list --catalog my_catalog --namespace ns1
```

### Authentication

As outlined above, the Polaris CLI may take credentials using the `--client-id` and `--client-secret` options. For example:

```
polaris --client-id 4b5ed1ca908c3cc2 --client-secret 07ea8e4edefb9a9e57c247e8d1a4f51c principals ...
```

If `--client-id` and `--client-secret` are not provided, the Polaris CLI will try to read the client ID and client secret from environment variables called `CLIENT_ID` and `CLIENT_SECRET` respectively. If these flags are not provided and the environment variables are not set, the CLI will fail.

Alternatively, the `--access-token` option can be used instead of `--client-id` and `--client-secret`, but both authentication methods cannot be used simultaneously.

Additionally, the `--profile` option can be used to specify a saved profile instead of providing authentication details directly. If `--profile` is not provided, the CLI will check the `CLIENT_PROFILE` environment variable. Profiles store authentication details and connection settings, simplifying repeated CLI usage.

If the `--host` and `--port` options are not provided, the CLI will default to communicating with `localhost:8181`.

Alternatively, the `--base-url` option can be used instead of `--host` and `--port`, but both options cannot be used simultaneously. This allows specifying arbitrary Polaris URLs, including HTTPS ones, that have additional base prefixes before the `/api/*/v1` subpaths.

If your Polaris server is configured to use a realm other than the default, you can use the `--realm` option to specify a realm. If `--realm` is not provided, the CLI will check the `REALM` environment variable. If neither is provided, the CLI will not send the realm context header.
Also, if your Polaris server uses a custom realm header name, you can use the `--header` option to specify it. If `--header` is not provided, the CLI will check the `HEADER` environment variable. If neither is provided, the CLI will use default header name `Polaris-Realm`.

Read [here]({{% ref "configuration/configuring-polaris.md" %}}) more about configuring polaris server to work with multiple realms.

### PATH

These examples assume the Polaris CLI is on the PATH and so can be invoked just by the command `polaris`. You can add the CLI to your PATH environment variable with a command like the following:

```
export PATH="$HOME/polaris:$PATH"
```

Alternatively, you can run the CLI by providing a path to it, such as with the following invocation:

```
~/polaris principals list
```

## Commands

Each of the commands `catalogs`, `principals`, `principal-roles`, `catalog-roles`, and `privileges` is used to manage a different type of entity within Polaris.

In addition to these, the `profiles` command is available for managing stored authentication profiles, allowing login credentials to be configured for reuse. This provides an alternative to passing authentication details with every command. By default, profiles are stored in a `.polaris.json` file within the `~/.polaris` directory. The location of this directory can be overridden by setting the `POLARIS_HOME` environment variable.

To find details on the options that can be provided to a particular command or subcommand ad-hoc, you may wish to use the `--help` flag. For example:

```
polaris catalogs --help
polaris principals create --help
polaris profiles --help
polaris setup --help
polaris find --help
polaris tables --help
```

### Catalogs

The `catalogs` command is used to create, discover, and otherwise manage catalogs within Polaris.

`catalogs` supports the following subcommands:

1. create
2. delete
3. get
4. list
5. update
6. summarize

{{< alert warning >}}
Catalog properties configured with `--property` or `--set-property` are client-visible defaults.
Polaris returns them to authenticated catalog clients through the Iceberg REST `/config` response.
Use catalog properties only for non-sensitive client configuration. Do not store passwords, tokens,
access keys, or other secrets in catalog properties.
{{< /alert >}}

#### create

The `create` subcommand is used to create a catalog.

```
usage: polaris catalogs create [-h] [options] CATALOG_NAME

positional arguments:
  CATALOG_NAME                                                         catalog

options:
  -h, --help                                                           show this help message and exit

Command Options:
  --type {internal,external}                                           The type of catalog [INTERNAL, EXTERNAL]
  --storage-type {s3,azure,gcs,file}                                   (Required) The storage type [S3, AZURE, GCS, FILE]
  --default-base-location DEFAULT_BASE_LOCATION                        (Required) Default base location for the catalog
  --allowed-location ALLOWED_LOCATION                                  An allowed location for files tracked by the catalog
  --property PROPERTY                                                  A key/value pair such as: tag=value. Multiple can be provided by specifying this option more than once. Do not put passwords, tokens, access keys, or other secrets into the client-visible catalog properties.

AWS S3 Storage Options:
  --endpoint ENDPOINT                                                  The S3 endpoint to use when connecting to S3
  --endpoint-internal ENDPOINT_INTERNAL                                The S3 endpoint used by Polaris to use when connecting to S3, if different from the one that clients use
  --sts-endpoint STS_ENDPOINT                                          The STS endpoint to use when connecting to STS
  --no-sts                                                             Indicates that Polaris should not use STS (e.g. if STS is not available)
  --no-kms                                                             Indicates that Polaris should not use KMS (e.g. if KMS is not available)
  --path-style-access                                                  Whether to use path-style-access for S3
  --current-kms-key CURRENT_KMS_KEY                                    The AWS KMS key ARN to be used for encrypting new S3 data
  --allowed-kms-key ALLOWED_KMS_KEY                                    AWS KMS key ARN(s) that this catalog and its clients are allowed to use for reading S3 data (zero or more)
  --role-arn ROLE_ARN                                                  A role ARN to use when connecting to S3
  --region REGION                                                      The region to use when connecting to S3
  --external-id EXTERNAL_ID                                            The external ID to use when connecting to S3

Azure Storage Options:
  --tenant-id TENANT_ID                                                (Required) A tenant ID to use when connecting to Azure Storage
  --multi-tenant-app-name MULTI_TENANT_APP_NAME                        The app name to use when connecting to Azure Storage
  --hierarchical                                                       Indicates whether the referenced Azure storage location(s) support hierarchical namespaces
  --consent-url CONSENT_URL                                            A consent URL granting permissions for the Azure Storage location

GCP Storage Options:
  --service-account SERVICE_ACCOUNT                                    The service account to use when connecting to GCS

External Catalog Federation: General Options:
  --catalog-connection-type {hadoop,iceberg-rest,hive}                 External catalog type [ICEBERG-REST, HADOOP, HIVE]
  --iceberg-remote-catalog-name ICEBERG_REMOTE_CATALOG_NAME            The remote catalog name when federating to an Iceberg REST catalog
  --hadoop-warehouse HADOOP_WAREHOUSE                                  The warehouse to use when federating to a HADOOP catalog
  --hive-warehouse HIVE_WAREHOUSE                                      The warehouse to use when federating to a HIVE catalog
  --catalog-authentication-type {oauth,bearer,sigv4,implicit}          Authentication type [OAUTH, BEARER, SIGV4, IMPLICIT]
  --catalog-service-identity-type {aws_iam}                            Service identity type [AWS_IAM]
  --catalog-uri CATALOG_URI                                            The URI of the external catalog

External Catalog Federation: AWS IAM Identity Options:
  --catalog-service-identity-iam-arn CATALOG_SERVICE_IDENTITY_IAM_ARN  The ARN of the IAM user or IAM role Polaris uses to assume roles and then access external resources.

External Catalog Federation: OAuth Options:
  --catalog-token-uri CATALOG_TOKEN_URI                                Token server URI
  --catalog-client-id CATALOG_CLIENT_ID                                OAuth client ID
  --catalog-client-secret CATALOG_CLIENT_SECRET                        OAuth client secret (input-only)
  --catalog-client-scope CATALOG_CLIENT_SCOPE                          OAuth scopes to specify when exchanging for a short-lived access token. Multiple can be provided by specifying this option more than once

External Catalog Federation: Bearer Token Options:
  --catalog-bearer-token CATALOG_BEARER_TOKEN                          Bearer token (input-only)

External Catalog Federation: AWS SigV4 Options:
  --catalog-role-arn CATALOG_ROLE_ARN                                  The AWS IAM role ARN assumed by Polaris when signing requests
  --catalog-role-session-name CATALOG_ROLE_SESSION_NAME                The role session name to be used by the SigV4 protocol for signing requests
  --catalog-external-id CATALOG_EXTERNAL_ID                            An optional external ID used to establish a AWS trust relationship
  --catalog-signing-region CATALOG_SIGNING_REGION                      Region to be used by the SigV4 protocol for signing requests
  --catalog-signing-name CATALOG_SIGNING_NAME                          The service name to be used by the SigV4 protocol for signing requests
```

##### Examples

```
polaris catalogs create \
  --storage-type s3 \
  --default-base-location s3://example-bucket/my_data \
  --role-arn ${ROLE_ARN} \
  my_catalog

polaris catalogs create \
  --storage-type s3 \
  --default-base-location s3://example-bucket/my_other_data \
  --allowed-location s3://example-bucket/second_location \
  --allowed-location s3://other-bucket/third_location \
  --role-arn ${ROLE_ARN} \
  my_other_catalog

polaris catalogs create \
  --storage-type file \
  --default-base-location file:///example/tmp \
  quickstart_catalog
```

#### delete

The `delete` subcommand is used to delete a catalog.

```
usage: polaris catalogs delete [-h] [options] CATALOG_NAME

positional arguments:
  CATALOG_NAME  catalog

options:
  -h, --help    show this help message and exit
```

##### Examples

```
polaris catalogs delete some_catalog
```

#### get

The `get` subcommand is used to retrieve details about a catalog.

```
usage: polaris catalogs get [-h] [options] CATALOG_NAME

positional arguments:
  CATALOG_NAME  catalog

options:
  -h, --help    show this help message and exit
```

##### Examples

```
polaris catalogs get some_catalog

polaris catalogs get another_catalog
```

#### list

The `list` subcommand is used to show details about all catalogs, or those that a certain principal role has access to. The principal used to perform this operation must have the `CATALOG_LIST` privilege.

```
usage: polaris catalogs list [-h] [options]

options:
  -h, --help                       show this help message and exit

Command Options:
  --principal-role PRINCIPAL_ROLE  List only catalogs reachable by this principal role
```

##### Examples

```
polaris catalogs list

polaris catalogs list --principal-role some_user
```

#### summarize

The `summarize` subcommand is used to display summary for a catalog.

```
usage: polaris catalogs summarize [-h] [options] CATALOG_NAME

positional arguments:
  CATALOG_NAME  catalog

options:
  -h, --help    show this help message and exit
```

##### Examples

```
polaris catalogs summarize some_catalog
```

#### update

The `update` subcommand is used to update a catalog. Currently, this command supports changing the properties of a catalog or updating its storage configuration.

```
usage: polaris catalogs update [-h] [options] CATALOG_NAME

positional arguments:
  CATALOG_NAME                                   catalog

options:
  -h, --help                                     show this help message and exit

Command Options:
  --default-base-location DEFAULT_BASE_LOCATION  A new default base location for the catalog
  --allowed-location ALLOWED_LOCATION            An additional allowed location for files
  --set-property SET_PROPERTY                    A key/value pair such as: tag=value. Merges the specified key/value into an existing properties map by updating the value if the key already exists or creating a new entry if not. Multiple can be provided by specifying this option more than once. Do not put passwords, tokens, access keys, or other secrets into the client-visible catalog properties.
  --remove-property REMOVE_PROPERTY              A key to remove from a properties map. If the key already does not exist then no action is taken for the specified key. Multiple can be provided by specifying this option more than once

AWS S3 Storage Options:
  --region REGION                                The region to use when connecting to S3
```

##### Examples

```
polaris catalogs update --set-property tag=new_value my_catalog

polaris catalogs update --default-base-location s3://new-bucket/my_data my_catalog
```

### Principals

The `principals` command is used to manage principals within Polaris.

`principals` supports the following subcommands:

1. create
2. delete
3. get
4. list
5. rotate-credentials
6. update
7. access
8. reset

#### create

The `create` subcommand is used to create a new principal.

```
usage: polaris principals create [-h] [options] PRINCIPAL_NAME

positional arguments:
  PRINCIPAL_NAME       principal

options:
  -h, --help           show this help message and exit

Command Options:
  --type {service}     The type of principal [SERVICE]
  --property PROPERTY  A key/value pair such as: tag=value. Multiple can be provided by specifying this option more than once
```

##### Examples

```
polaris principals create some_user

polaris principals create --client-id ${CLIENT_ID} --property admin=true some_admin_user
```

#### delete

The `delete` subcommand is used to delete a principal.

```
usage: polaris principals delete [-h] [options] PRINCIPAL_NAME

positional arguments:
  PRINCIPAL_NAME  principal

options:
  -h, --help      show this help message and exit
```

##### Examples

```
polaris principals delete some_user

polaris principals delete some_admin_user
```

#### get

The `get` subcommand retrieves details about a principal.

```
usage: polaris principals get [-h] [options] PRINCIPAL_NAME

positional arguments:
  PRINCIPAL_NAME  principal

options:
  -h, --help      show this help message and exit
```

##### Examples

```
polaris principals get some_user

polaris principals get some_admin_user
```

#### list

The `list` subcommand shows details about all principals.

##### Examples

```
polaris principals list
```

#### rotate-credentials

The `rotate-credentials` subcommand is used to update the credentials used by a principal. After this command runs successfully, the new credentials will be printed to stdout.

```
usage: polaris principals rotate-credentials [-h] [options] PRINCIPAL_NAME

positional arguments:
  PRINCIPAL_NAME  principal

options:
  -h, --help      show this help message and exit
```

##### Examples

```
polaris principals rotate-credentials some_user

polaris principals rotate-credentials some_admin_user
```

#### update

The `update` subcommand is used to update a principal. Currently, this supports rewriting the properties associated with a principal.

```
usage: polaris principals update [-h] [options] PRINCIPAL_NAME

positional arguments:
  PRINCIPAL_NAME                     principal

options:
  -h, --help                         show this help message and exit

Command Options:
  --set-property SET_PROPERTY        A key/value pair such as: tag=value. Merges the specified key/value into an existing properties map by updating the value if the key already exists or creating a new entry if not. Multiple can be provided by specifying this option more than once
  --remove-property REMOVE_PROPERTY  A key to remove from a properties map. If the key already does not exist then no action is taken for the specified key. Multiple can be provided by specifying this option more than once
```

##### Examples

```
polaris principals update --property key=value --property other_key=other_value some_user

polaris principals update --property are_other_keys_removed=yes some_user
```

#### access

The `access` subcommand retrieves entities relation about a principal.

```
usage: polaris principals access [-h] [options] PRINCIPAL_NAME

positional arguments:
  PRINCIPAL_NAME  principal

options:
  -h, --help      show this help message and exit
```

##### Examples

```
polaris principals access quickstart_user
```

#### reset

The `reset` subcommand is used to reset principal credentials.

```
usage: polaris principals reset [-h] [options] PRINCIPAL_NAME

positional arguments:
  PRINCIPAL_NAME                         principal

options:
  -h, --help                             show this help message and exit

Command Options:
  --new-client-id NEW_CLIENT_ID          The new client ID for the principal
  --new-client-secret NEW_CLIENT_SECRET  The new client secret for the principal
```

##### Examples

```
polaris principals create some_user

polaris principals reset some_user
polaris principals reset --new-client-id ${NEW_CLIENT_ID} some_user
polaris principals reset --new-client-secret ${NEW_CLIENT_SECRET} some_user
polaris principals reset --new-client-id ${NEW_CLIENT_ID} --new-client-secret ${NEW_CLIENT_SECRET} some_user
```

#### summarize

The `summarize` subcommand is used to display summary for a principal.

```
usage: polaris principals summarize [-h] [options] PRINCIPAL_NAME

positional arguments:
  PRINCIPAL_NAME  principal

options:
  -h, --help      show this help message and exit
```

##### Examples

```
polaris principals summarize some_user
```

### Principal Roles

The `principal-roles` command is used to create, discover, and manage principal roles within Polaris. Additionally, this command can identify principals or catalog roles associated with a principal role, and can be used to grant a principal role to a principal.

`principal-roles` supports the following subcommands:

1. create
2. delete
3. get
4. list
5. update
6. grant
7. revoke
8. summarize

#### create

The `create` subcommand is used to create a new principal role.

```
usage: polaris principal-roles create [-h] [options] PRINCIPAL_ROLE_NAME

positional arguments:
  PRINCIPAL_ROLE_NAME  principal role

options:
  -h, --help           show this help message and exit

Command Options:
  --property PROPERTY  A key/value pair such as: tag=value. Multiple can be provided by specifying this option more than once
```

##### Examples

```
polaris principal-roles create data_engineer

polaris principal-roles create --property key=value data_analyst
```

#### delete

The `delete` subcommand is used to delete a principal role.

```
usage: polaris principal-roles delete [-h] [options] PRINCIPAL_ROLE_NAME

positional arguments:
  PRINCIPAL_ROLE_NAME  principal role

options:
  -h, --help           show this help message and exit
```

##### Examples

```
polaris principal-roles delete data_engineer

polaris principal-roles delete data_analyst
```

#### get

The `get` subcommand retrieves details about a principal role.

```
usage: polaris principal-roles get [-h] [options] PRINCIPAL_ROLE_NAME

positional arguments:
  PRINCIPAL_ROLE_NAME  principal role

options:
  -h, --help           show this help message and exit
```

##### Examples

```
polaris principal-roles get data_engineer

polaris principal-roles get data_analyst
```

#### list

The list subcommand is used to print out all principal roles or, alternatively, to list all principal roles associated with a given principal or with a given catalog role.

```
usage: polaris principal-roles list [-h] [options]

options:
  -h, --help                   show this help message and exit

Command Options:
  --catalog-role CATALOG_ROLE  Show only principal roles assigned to this catalog role
  --principal PRINCIPAL        Show only principal roles assigned to this principal
```

##### Examples

```
polaris principal-roles list

polaris principal-roles --principal d.knuth

polaris principal-roles --catalog-role super_secret_data
```

#### update

The `update` subcommand is used to update a principal role. Currently, this supports updating the properties tied to a principal role.

```
usage: polaris principal-roles update [-h] [options] PRINCIPAL_ROLE_NAME

positional arguments:
  PRINCIPAL_ROLE_NAME                principal role

options:
  -h, --help                         show this help message and exit

Command Options:
  --set-property SET_PROPERTY        A key/value pair such as: tag=value. Merges the specified key/value into an existing properties map by updating the value if the key already exists or creating a new entry if not. Multiple can be provided by specifying this option more than once
  --remove-property REMOVE_PROPERTY  A key to remove from a properties map. If the key already does not exist then no action is taken for the specified key. Multiple can be provided by specifying this option more than once
```

##### Examples

```
polaris principal-roles update --property key=value2 data_engineer

polaris principal-roles update data_analyst --property key=value3
```

#### grant

The `grant` subcommand is used to grant a principal role to a principal.

```
usage: polaris principal-roles grant [-h] [options] PRINCIPAL_ROLE_NAME

positional arguments:
  PRINCIPAL_ROLE_NAME    principal role

options:
  -h, --help             show this help message and exit

Command Options:
  --principal PRINCIPAL  The name of a principal
```

##### Examples

```
polaris principal-roles grant --principal d.knuth data_engineer

polaris principal-roles grant data_scientist --principal a.ng
```

#### revoke

The `revoke` subcommand is used to revoke a principal role from a principal.

```
usage: polaris principal-roles revoke [-h] [options] PRINCIPAL_ROLE_NAME

positional arguments:
  PRINCIPAL_ROLE_NAME    principal role

options:
  -h, --help             show this help message and exit

Command Options:
  --principal PRINCIPAL  The name of a principal
```

##### Examples

```
polaris principal-roles revoke --principal former.employee data_engineer

polaris principal-roles revoke data_scientist --principal changed.role
```

#### summarize

The `summarize` subcommand is used to display summary for a principal role.

```
usage: polaris principal-roles summarize [-h] [options] PRINCIPAL_ROLE_NAME

positional arguments:
  PRINCIPAL_ROLE_NAME  principal role

options:
  -h, --help           show this help message and exit
```

##### Examples

```
polaris principal-roles summarize data_engineer
```

### Catalog Roles

The catalog-roles command is used to create, discover, and manage catalog roles within Polaris. Additionally, this command can be used to grant a catalog role to a principal role.

`catalog-roles` supports the following subcommands:

1. create
2. delete
3. get
4. list
5. update
6. grant
7. revoke
8. summarize

#### create

The `create` subcommand is used to create a new catalog role.

```
usage: polaris catalog-roles create [-h] [options] CATALOG_ROLE_NAME

positional arguments:
  CATALOG_ROLE_NAME    catalog role

options:
  -h, --help           show this help message and exit

Command Options:
  --catalog CATALOG    The name of a catalog
  --property PROPERTY  A key/value pair such as: tag=value. Multiple can be provided by specifying this option more than once
```

##### Examples

```
polaris catalog-roles create --property key=value --catalog some_catalog sales_data

polaris catalog-roles create --catalog other_catalog sales_data
```

#### delete

The `delete` subcommand is used to delete a catalog role.

```
usage: polaris catalog-roles delete [-h] [options] CATALOG_ROLE_NAME

positional arguments:
  CATALOG_ROLE_NAME  catalog role

options:
  -h, --help         show this help message and exit

Command Options:
  --catalog CATALOG  The name of a catalog
```

##### Examples

```
polaris catalog-roles delete --catalog some_catalog sales_data

polaris catalog-roles delete --catalog other_catalog sales_data
```

#### get

The `get` subcommand retrieves details about a catalog role.

```
usage: polaris catalog-roles get [-h] [options] CATALOG_ROLE_NAME

positional arguments:
  CATALOG_ROLE_NAME  catalog role

options:
  -h, --help         show this help message and exit

Command Options:
  --catalog CATALOG  The name of a catalog
```

##### Examples

```
polaris catalog-roles get --catalog some_catalog inventory_data

polaris catalog-roles get --catalog other_catalog inventory_data
```

#### list

The `list` subcommand is used to print all catalog roles. Alternatively, if a principal role is provided, only catalog roles associated with that principal are shown.

```
usage: polaris catalog-roles list [-h] [options] CATALOG_NAME

positional arguments:
  CATALOG_NAME                     catalog

options:
  -h, --help                       show this help message and exit

Command Options:
  --principal-role PRINCIPAL_ROLE  The name of a principal role
```

##### Examples

```
polaris catalog-roles list

polaris catalog-roles list --principal-role data_engineer
```

#### update

The `update` subcommand is used to update a catalog role. Currently, only updating properties associated with the catalog role is supported.

```
usage: polaris catalog-roles update [-h] [options] CATALOG_ROLE_NAME

positional arguments:
  CATALOG_ROLE_NAME                  catalog role

options:
  -h, --help                         show this help message and exit

Command Options:
  --catalog CATALOG                  The name of a catalog
  --set-property SET_PROPERTY        A key/value pair such as: tag=value. Merges the specified key/value into an existing properties map by updating the value if the key already exists or creating a new entry if not. Multiple can be provided by specifying this option more than once
  --remove-property REMOVE_PROPERTY  A key to remove from a properties map. If the key already does not exist then no action is taken for the specified key. Multiple can be provided by specifying this option more than once
```

##### Examples

```
polaris catalog-roles update --property contains_pii=true --catalog some_catalog sales_data

polaris catalog-roles update sales_data --catalog some_catalog --property key=value
```

#### grant

The `grant` subcommand is used to grant a catalog role to a principal role.

```
usage: polaris catalog-roles grant [-h] [options] CATALOG_ROLE_NAME

positional arguments:
  CATALOG_ROLE_NAME                catalog role

options:
  -h, --help                       show this help message and exit

Command Options:
  --catalog CATALOG                The name of a catalog
  --principal-role PRINCIPAL_ROLE  The name of a principal role
```

##### Examples

```
polaris catalog-roles grant sensitive_data --catalog some_catalog --principal-role power_user

polaris catalog-roles grant --catalog sales_data contains_cc_info_catalog_role --principal-role financial_analyst_role
```

#### revoke

The `revoke` subcommand is used to revoke a catalog role from a principal role.

```
usage: polaris catalog-roles revoke [-h] [options] CATALOG_ROLE_NAME

positional arguments:
  CATALOG_ROLE_NAME                catalog role

options:
  -h, --help                       show this help message and exit

Command Options:
  --catalog CATALOG                The name of a catalog
  --principal-role PRINCIPAL_ROLE  The name of a principal role
```

##### Examples

```
polaris catalog-roles revoke sensitive_data --catalog some_catalog --principal-role power_user

polaris catalog-roles revoke --catalog sales_data contains_cc_info_catalog_role --principal-role financial_analyst_role
```

#### summarize

The `summarize` subcommand is used to display summary for a catalog role.

```
usage: polaris catalog-roles summarize [-h] [options] CATALOG_ROLE_NAME

positional arguments:
  CATALOG_ROLE_NAME  catalog role

options:
  -h, --help         show this help message and exit

Command Options:
  --catalog CATALOG  The name of a catalog
```

##### Examples

```
polaris catalog-roles summarize --catalog some_catalog some_catalog_role
```

### Namespaces

The `namespaces` command is used to manage namespaces within Polaris.

`namespaces` supports the following subcommands:

1. create
2. delete
3. get
4. list
5. summarize

#### create

The `create` subcommand is used to create a new namespace.

When creating a namespace with an explicit location, that location must reside within the parent catalog or namespace.

```
usage: polaris namespaces create [-h] [options] NAMESPACE

positional arguments:
  NAMESPACE            namespace

options:
  -h, --help           show this help message and exit

Command Options:
  --catalog CATALOG    The name of a catalog
  --location LOCATION  The storage location for the namespace
  --property PROPERTY  A key/value pair such as: tag=value. Multiple can be provided by specifying this option more than once
```

##### Examples

```
polaris namespaces create --catalog my_catalog outer

polaris namespaces create --catalog my_catalog --location 's3://bucket/outer/inner_SUFFIX' outer.inner
```

#### delete

The `delete` subcommand is used to delete a namespace.

```
usage: polaris namespaces delete [-h] [options] NAMESPACE

positional arguments:
  NAMESPACE          namespace

options:
  -h, --help         show this help message and exit

Command Options:
  --catalog CATALOG  The name of a catalog
```

##### Examples

```
polaris namespaces delete  outer_namespace.inner_namespace --catalog my_catalog

polaris namespaces delete --catalog my_catalog outer_namespace
```

#### get

The `get` subcommand retrieves details about a namespace.

```
usage: polaris namespaces get [-h] [options] NAMESPACE

positional arguments:
  NAMESPACE          namespace

options:
  -h, --help         show this help message and exit

Command Options:
  --catalog CATALOG  The name of a catalog
```

##### Examples

```
polaris namespaces get --catalog some_catalog a.b

polaris namespaces get a.b.c --catalog some_catalog
```

#### list

The `list` subcommand shows details about all namespaces directly within a catalog or, optionally, within some parent prefix in that catalog.

```
usage: polaris namespaces list [-h] [options]

options:
  -h, --help         show this help message and exit

Command Options:
  --catalog CATALOG  The name of a catalog
  --parent PARENT    The parent namespace to list sub-namespaces from
```

##### Examples

```
polaris namespaces list --catalog my_catalog

polaris namespaces list --catalog my_catalog --parent a

polaris namespaces list --catalog my_catalog --parent a.b
```

#### summarize

The `summarize` subcommand is used to display summary for a namespace.

```
usage: polaris namespaces summarize [-h] [options] NAMESPACE

positional arguments:
  NAMESPACE          namespace

options:
  -h, --help         show this help message and exit

Command Options:
  --catalog CATALOG  The name of a catalog
```

##### Examples

```
polaris namespaces summarize --catalog my_catalog a.b
```

### Privileges

The `privileges` command is used to grant various privileges to a catalog role, or to revoke those privileges. Privileges can be on the level of a catalog, a namespace, a table, or a view. For more information on privileges, please refer to the [docs]({{% ref "entities#privilege" %}}).

Note that when using the `privileges` command, the user specifies the relevant catalog and catalog role before selecting a subcommand.

`privileges` supports the following subcommands:

1. list
2. catalog
3. namespace
4. table
5. view

Each of these subcommands, except `list`, supports the `grant` and `revoke` actions and requires an action to be specified.

Note that each subcommand's `revoke` action always accepts the same options that the corresponding `grant` action does, but with the addition of the `cascade` option. `cascade` is used to revoke all other privileges that depend on the specified privilege.

#### list

The `list` subcommand shows details about all privileges for a catalog role.

```
usage: polaris privileges list [-h] [options]

options:
  -h, --help                   show this help message and exit

Command Options:
  --catalog CATALOG            The name of a catalog
  --catalog-role CATALOG_ROLE  The name of a catalog role
```

##### Examples

```
polaris privileges  list --catalog my_catalog --catalog-role my_role

polaris privileges my_role list --catalog-role my_other_role --catalog my_catalog
```

#### catalog

The `catalog` subcommand manages privileges at the catalog level. `grant` is used to grant catalog privileges to the specified catalog role, and `revoke` is used to revoke them.

```
usage: polaris privileges catalog [-h] [options] SUBCOMMAND ...

options:
  -h, --help  show this help message and exit

Subcommands:
  SUBCOMMAND
    grant     Grant a catalog-level privilege
    revoke    Revoke a catalog-level privilege
```

##### Examples

```
polaris privileges \
  catalog \
  grant \
  --catalog my_catalog \
  --catalog-role catalog_role \
  TABLE_CREATE

polaris privileges \
  catalog \
  revoke \
  --catalog my_catalog \
  --catalog-role catalog_role \
  --cascade \
  TABLE_CREATE
```

#### namespace

The `namespace` subcommand manages privileges at the namespace level.

```
usage: polaris privileges namespace [-h] [options] SUBCOMMAND ...

options:
  -h, --help  show this help message and exit

Subcommands:
  SUBCOMMAND
    grant     Grant a namespace-level privilege
    revoke    Revoke a namespace-level privilege
```

##### Examples

```
polaris privileges \
  namespace \
  grant \
  --catalog my_catalog \
  --catalog-role catalog_role \
  --namespace a.b \
  TABLE_LIST

polaris privileges \
  namespace \
  revoke \
  --catalog my_catalog \
  --catalog-role catalog_role \
  --namespace a.b \
  TABLE_LIST
```

#### table

The `table` subcommand manages privileges at the table level.

```
usage: polaris privileges table [-h] [options] SUBCOMMAND ...

options:
  -h, --help  show this help message and exit

Subcommands:
  SUBCOMMAND
    grant     Grant a table-level privilege
    revoke    Revoke a table-level privilege
```

##### Examples

```
polaris privileges \
  table \
  grant \
  --catalog my_catalog \
  --catalog-role catalog_role \
  --namespace a.b \
  --table t \
  TABLE_DROP

polaris privileges \
  table \
  grant \
  --catalog my_catalog \
  --catalog-role catalog_role \
  --namespace a.b \
  --table t \
  --cascade \
  TABLE_DROP
```

#### view

The `view` subcommand manages privileges at the view level.

```
usage: polaris privileges view [-h] [options] SUBCOMMAND ...

options:
  -h, --help  show this help message and exit

Subcommands:
  SUBCOMMAND
    grant     Grant a view-level privilege
    revoke    Revoke a view-level privilege
```

##### Examples

```
polaris privileges \
  view \
  grant \
  --catalog my_catalog \
  --catalog-role catalog_role \
  --namespace a.b.c \
  --view v \
  VIEW_FULL_METADATA

polaris privileges \
  view \
  grant \
  --catalog my_catalog \
  --catalog-role catalog_role \
  --namespace a.b.c \
  --view v \
  --cascade \
  VIEW_FULL_METADATA
```

### Profiles

The `profiles` command is used to manage stored authentication profiles in Polaris. Profiles allow authentication credentials to be saved and reused, eliminating the need to pass credentials with every command.

`profiles` supports the following subcommands:

1. create
2. delete
3. get
4. list
5. update

#### create

The `create` subcommand is used to create a new authentication profile.

```
usage: polaris profiles create [-h] [options] PROFILE_NAME

positional arguments:
  PROFILE_NAME  profile

options:
  -h, --help    show this help message and exit
```

##### Examples

```
polaris profiles create dev
```

#### delete

The `delete` subcommand removes a stored profile.

```
usage: polaris profiles delete [-h] [options] PROFILE_NAME

positional arguments:
  PROFILE_NAME  profile

options:
  -h, --help    show this help message and exit
```

##### Examples

```
polaris profiles delete dev
```

#### get

The `get` subcommand removes a stored profile.

```
usage: polaris profiles get [-h] [options] PROFILE_NAME

positional arguments:
  PROFILE_NAME  profile

options:
  -h, --help    show this help message and exit
```

##### Examples

```
polaris profiles get dev
```

#### list

The `list` subcommand displays all stored profiles.

```
usage: polaris profiles list [-h] [options]

options:
  -h, --help  show this help message and exit
```

##### Examples

```
polaris profiles list
```

#### update

The `update` subcommand modifies an existing profile.

```
usage: polaris profiles update [-h] [options] PROFILE_NAME

positional arguments:
  PROFILE_NAME  profile

options:
  -h, --help    show this help message and exit
```

##### Examples

```
polaris profiles update dev
```

### Policies

The `policies` command is used to manage policies within Polaris.

`policies` supports the following subcommands:

1. attach
2. create
3. delete
4. detach
5. get
6. list
7. update

#### attach

The `attach` subcommand is used to create a mapping between a policy and a resource entity.

```
usage: polaris policies attach [-h] [options] POLICY_NAME

positional arguments:
  POLICY_NAME                        policy

options:
  -h, --help                         show this help message and exit

Command Options:
  --catalog CATALOG                  The name of a catalog
  --namespace NAMESPACE              A period-delimited namespace
  --attachment-type ATTACHMENT_TYPE  The type of entity to attach to ('catalog', 'namespace', 'table-like')
  --attachment-path ATTACHMENT_PATH  The path of the target entity (e.g., 'ns1.tb1')
  --parameters PARAMETERS            Key-value pairs for the attachment (e.g., key=value)
```

##### Examples

```
polaris policies attach --catalog some_catalog --namespace some.schema --attachment-type namespace --attachment-path some.schema some_policy

polaris policies attach --catalog some_catalog --namespace some.schema --attachment-type table-like --attachment-path some.schema.t some_table_policy
```

#### create

The `create` subcommand is used to create a policy.

```
usage: polaris policies create [-h] [options] POLICY_NAME

positional arguments:
  POLICY_NAME                              policy

options:
  -h, --help                               show this help message and exit

Command Options:
  --catalog CATALOG                        The name of a catalog
  --namespace NAMESPACE                    A period-delimited namespace
  --policy-file POLICY_FILE                Path to the JSON file containing the policy definition
  --policy-type POLICY_TYPE                The type of the policy (e.g., 'system.data-compaction')
  --policy-description POLICY_DESCRIPTION  An optional description for the policy
```

##### Examples

```
polaris policies create --catalog some_catalog --namespace some.schema --policy-file some_policy.json --policy-type system.data-compaction some_policy

polaris policies create --catalog some_catalog --namespace some.schema --policy-file some_snapshot_expiry_policy.json --policy-type system.snapshot-expiry some_snapshot_expiry_policy
```

#### delete

The `delete` subcommand is used to delete a policy.

```
usage: polaris policies delete [-h] [options] POLICY_NAME

positional arguments:
  POLICY_NAME            policy

options:
  -h, --help             show this help message and exit

Command Options:
  --catalog CATALOG      The name of a catalog
  --namespace NAMESPACE  A period-delimited namespace
  --detach-all           Delete the policy and all its attached mappings
```

##### Examples

```
polaris policies delete --catalog some_catalog --namespace some.schema some_policy

polaris policies delete --catalog some_catalog --namespace some.schema --detach-all some_policy
```

#### detach

The `detach` subcommand is used to remove a mapping between a policy and a target entity

```
usage: polaris policies detach [-h] [options] POLICY_NAME

positional arguments:
  POLICY_NAME                        policy

options:
  -h, --help                         show this help message and exit

Command Options:
  --catalog CATALOG                  The name of a catalog
  --namespace NAMESPACE              A period-delimited namespace
  --attachment-type ATTACHMENT_TYPE  The type of entity to attach to ('catalog', 'namespace', 'table-like')
  --attachment-path ATTACHMENT_PATH  The path of the target entity (e.g., 'ns1.tb1')
  --parameters PARAMETERS            Key-value pairs for the attachment (e.g., key=value)
```

##### Examples

```
polaris policies detach --catalog some_catalog --namespace some.schema --attachment-type namespace --attachment-path some.schema some_policy

polaris policies detach --catalog some_catalog --namespace some.schema --attachment-type catalog --attachment-path some_catalog some_policy
```

#### get

The `get` subcommand is used to load a policy from the catalog.

```
usage: polaris policies get [-h] [options] POLICY_NAME

positional arguments:
  POLICY_NAME            policy

options:
  -h, --help             show this help message and exit

Command Options:
  --catalog CATALOG      The name of a catalog
  --namespace NAMESPACE  A period-delimited namespace
```

##### Examples

```
polaris policies get --catalog some_catalog --namespace some.schema some_policy
```

#### list

The `list` subcommand is used to get all policy identifiers under this namespace and all applicable policies for a specified entity.

```
usage: polaris policies list [-h] [options]

options:
  -h, --help                 show this help message and exit

Command Options:
  --catalog CATALOG          The name of a catalog
  --namespace NAMESPACE      A period-delimited namespace
  --target-name TARGET_NAME  The name of the target entity
  --applicable               List policies applicable to the target (considering inheritance)
  --policy-type POLICY_TYPE  The type of the policy (e.g., 'system.data-compaction')
```

##### Examples

```
polaris policies list --catalog some_catalog

polaris policies list --catalog some_catalog --applicable
```

#### update

The `update` subcommand is used to update a policy.

```
usage: polaris policies update [-h] [options] POLICY_NAME

positional arguments:
  POLICY_NAME                              policy

options:
  -h, --help                               show this help message and exit

Command Options:
  --catalog CATALOG                        The name of a catalog
  --namespace NAMESPACE                    A period-delimited namespace
  --policy-file POLICY_FILE                Path to the JSON file containing the policy definition
  --policy-description POLICY_DESCRIPTION  An optional description for the policy
```

##### Examples

```
polaris policies update --catalog some_catalog --namespace some.schema --policy-file my_updated_policy.json my_policy

polaris policies update --catalog some_catalog --namespace some.schema --policy-file my_updated_policy.json --policy-description "Updated policy description" my_policy
```

### Repair

The `repair` command is a bash script wrapper used to regenerate Python client code and update necessary dependencies, ensuring the Polaris client remains up-to-date and functional. **Please note that this command does not support any options and its usage information is not available via a `--help` flag.**

### Setup

The `setup` command is used to automate the creation of various entities in Polaris, such as principals, roles, catalogs, namespaces, privileges, and policies, based on a configuration file. This simplifies the process of setting up a Polaris environment.

`setup` supports the following subcommands:

1. apply
2. export

#### apply

The `apply` subcommand reads a configuration file and creates the specified entities in Polaris. The configuration file must be in YAML format and define the entities to be created.

```
usage: polaris setup apply [-h] [options] SETUP_CONFIG_FILE

positional arguments:
  SETUP_CONFIG_FILE  setup config

options:
  -h, --help         show this help message and exit

Command Options:
  --dry-run          Run without executing
```

##### Examples

```
polaris setup apply setup-config.yaml
```

#### export

The `export` subcommand retrieves the current Polaris configuration and outputs it in a YAML format. This output is compatible with the apply subcommand, allowing you to easily back up, migrate, or recreate your Polaris environment.

```
usage: polaris setup export [-h] [options]

options:
  -h, --help  show this help message and exit
```

##### Examples

```
polaris setup export
```

### Find

The `find` command is used to searches for an identifier across global entities (principals, roles, catalogs)
and catalog entities (namespaces, tables, views) using fuzzy matching.

##### Examples

```
polaris find my_table
polaris find ns1.ns2
polaris find --catalog my_catalog my_table
polaris find my_table --type table
```

### Tables

The `tables` command is used to manage Iceberg tables within a Polaris Catalog.

`tables` supports the following subcommands:

1. list
2. get
3. summarize
4. delete

#### list

The `list` subcommand is used to list tables within a namesace from a given catalog.

```
usage: polaris tables list [-h] [options]

options:
  -h, --help             show this help message and exit

Command Options:
  --catalog CATALOG      The name of a catalog
  --namespace NAMESPACE  A period-delimited namespace
```

##### Examples

```
polaris tables list
```

#### get

The `get` subcommand retrieves the table metadata for a specific table.

```
usage: polaris tables get [-h] [options] TABLE_NAME

positional arguments:
  TABLE_NAME             table

options:
  -h, --help             show this help message and exit

Command Options:
  --catalog CATALOG      The name of a catalog
  --namespace NAMESPACE  A period-delimited namespace
```

##### Examples

```
polaris tables get my_table --catalog my_catalog --namespace ns1
```

#### summarize

The `summarize` subcommand provides a detail overview of a table.

```
usage: polaris tables summarize [-h] [options] TABLE_NAME

positional arguments:
  TABLE_NAME             table

options:
  -h, --help             show this help message and exit

Command Options:
  --catalog CATALOG      The name of a catalog
  --namespace NAMESPACE  A period-delimited namespace
```

##### Examples

```
polaris tables summarize my_table --catalog my_catalog --namespace ns1
```

#### delete

The `delete` subcommand de-registers a table from catalog (metadata-only deletion).

```
usage: polaris tables delete [-h] [options] TABLE_NAME

positional arguments:
  TABLE_NAME             table

options:
  -h, --help             show this help message and exit

Command Options:
  --catalog CATALOG      The name of a catalog
  --namespace NAMESPACE  A period-delimited namespace
```

##### Examples

```
polaris tables delete my_table --catalog my_catalog --namespace ns1
```

## Examples

This section outlines example code for a few common operations as well as for some more complex ones.

For especially complex operations, you may wish to instead directly use the Python API.

### Creating a principal and a catalog

```
polaris principals create my_user

polaris catalogs create \
  --type internal \
  --storage-type s3 \
  --default-base-location s3://iceberg-bucket/polaris-base \
  --role-arn arn:aws:iam::111122223333:role/ExampleCorpRole \
  --allowed-location s3://iceberg-bucket/polaris-alt-location-1 \
  --allowed-location s3://iceberg-bucket/polaris-alt-location-2 \
  my_catalog
```

### Granting a principal the ability to manage the content of a catalog

```
polaris principal-roles create power_user
polaris principal-roles grant --principal my_user power_user

polaris catalog-roles create --catalog my_catalog my_catalog_role
polaris catalog-roles grant \
  --catalog my_catalog \
  --principal-role power_user \
  my_catalog_role

polaris privileges \
  catalog \
  grant \
  --catalog my_catalog \
  --catalog-role my_catalog_role \
  CATALOG_MANAGE_CONTENT
```

### Identifying the tables a given principal has been granted explicit access to read

_Note that some other privileges, such as `CATALOG_MANAGE_CONTENT`, subsume `TABLE_READ_DATA` and would not be discovered here._

```
principal_roles=$(polaris principal-roles list --principal readonly_user | jq -r .name)
for principal_role in ${principal_roles}; do
  catalog_roles=$(polaris catalog-roles list quickstart_catalog --principal-role ${principal_role} | jq -r .name)
  for catalog_role in ${catalog_roles}; do
    grants=$(polaris privileges list  --catalog-role ${catalog_role} --catalog quickstart_catalog)
    for grant in $(echo ${grants} | jq -c 'select(.privilege == "TABLE_READ_DATA")'); do
      echo "${grant}"
    done
  done
done
```
