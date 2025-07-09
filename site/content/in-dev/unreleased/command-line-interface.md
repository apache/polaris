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
polaris [options] COMMAND ...

options:
--host
--port
--base-url
--client-id
--client-secret
--access-token
--profile
```

`COMMAND` must be one of the following:
1. catalogs
2. principals
3. principal-roles
4. catalog-roles
5. namespaces
6. privileges
7. profiles
8. repair

Each _command_ supports several _subcommands_, and some _subcommands_ have _actions_ that come after the subcommand in turn. Finally, _arguments_ follow to form a full invocation. Within a set of named arguments at the end of an invocation ordering is generally not important. Many invocations also have a required positional argument of the type that the _command_ refers to. Again, the ordering of this positional argument relative to named arguments is not important.

Some example full invocations:

```
polaris principals list
polaris catalogs delete some_catalog_name
polaris catalogs update --property foo=bar some_other_catalog
polaris catalogs update another_catalog --property k=v
polaris privileges namespace grant --namespace some.schema --catalog fourth_catalog --catalog-role some_catalog_role TABLE_READ_DATA
polaris profiles list
polaris repair
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

### PATH

These examples assume the Polaris CLI is on the PATH and so can be invoked just by the command `polaris`. You can add the CLI to your PATH environment variable with a command like the following:

```
export PATH="~/polaris:$PATH"
```

Alternatively, you can run the CLI by providing a path to it, such as with the following invocation:

```
~/polaris principals list
```

## Commands

Each of the commands `catalogs`, `principals`, `principal-roles`, `catalog-roles`, and `privileges` is used to manage a different type of entity within Polaris.

In addition to these, the `profiles` command is available for managing stored authentication profiles, allowing login credentials to be configured for reuse. This provides an alternative to passing authentication details with every command.

To find details on the options that can be provided to a particular command or subcommand ad-hoc, you may wish to use the `--help` flag. For example:

```
polaris catalogs --help
polaris principals create --help
polaris profiles --help
```

### catalogs

The `catalogs` command is used to create, discover, and otherwise manage catalogs within Polaris.

`catalogs` supports the following subcommands:

1. create
2. delete
3. get
4. list
5. update

#### create

The `create` subcommand is used to create a catalog.

```
input: polaris catalogs create --help
options:
  create
    Named arguments:
      --type  The type of catalog to create in [INTERNAL, EXTERNAL]. INTERNAL by default.
      --storage-type  (Required) The type of storage to use for the catalog
      --default-base-location  (Required) Default base location of the catalog
      --allowed-location  An allowed location for files tracked by the catalog. Multiple locations can be provided by specifying this option more than once.
      --role-arn  (Required for S3) A role ARN to use when connecting to S3
      --region  (Only for S3) The region to use when connecting to S3
      --external-id  (Only for S3) The external ID to use when connecting to S3
      --tenant-id  (Required for Azure) A tenant ID to use when connecting to Azure Storage
      --multi-tenant-app-name  (Only for Azure) The app name to use when connecting to Azure Storage
      --consent-url  (Only for Azure) A consent URL granting permissions for the Azure Storage location
      --service-account  (Only for GCS) The service account to use when connecting to GCS
      --property  A key/value pair such as: tag=value. Multiple can be provided by specifying this option more than once
      --catalog-connection-type  The type of external catalog in [ICEBERG, HADOOP].
      --iceberg-remote-catalog-name  The remote catalog name when federating to an Iceberg REST catalog
      --hadoop-warehouse  The warehouse to use when federating to a HADOOP catalog
      --catalog-authentication-type  The type of authentication in [OAUTH, BEARER, SIGV4]
      --catalog-service-identity-type  The type of service identity in [AWS_IAM]
      --catalog-service-identity-iam-arn  When using the AWS_IAM service identity type, this is the ARN of the IAM user or IAM role Polaris uses to assume roles and then access external resources.
      --catalog-uri  The URI of the external catalog
      --catalog-token-uri  (For authentication type OAUTH) Token server URI
      --catalog-client-id  (For authentication type OAUTH) oauth client id
      --catalog-client-secret  (For authentication type OAUTH) oauth client secret (input-only)
      --catalog-client-scope  (For authentication type OAUTH) oauth scopes to specify when exchanging for a short-lived access token. Multiple can be provided by specifying this option more than once
      --catalog-bearer-token  (For authentication type BEARER) Bearer token (input-only)
      --catalog-role-arn  (For authentication type SIGV4) The aws IAM role arn assumed by polaris userArn when signing requests
      --catalog-role-session-name  (For authentication type SIGV4) The role session name to be used by the SigV4 protocol for signing requests
      --catalog-external-id  (For authentication type SIGV4) An optional external id used to establish a trust relationship with AWS in the trust policy
      --catalog-signing-region  (For authentication type SIGV4) Region to be used by the SigV4 protocol for signing requests
      --catalog-signing-name  (For authentication type SIGV4) The service name to be used by the SigV4 protocol for signing requests, the default signing name is "execute-api" is if not provided
    Positional arguments:
      catalog
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
input: polaris catalogs delete --help
options:
  delete
    Positional arguments:
      catalog
```

##### Examples

```
polaris catalogs delete some_catalog
```

#### get

The `get` subcommand is used to retrieve details about a catalog.

```
input: polaris catalogs get --help
options:
  get
    Positional arguments:
      catalog
```

##### Examples

```
polaris catalogs get some_catalog

polaris catalogs get another_catalog
```

#### list

The `list` subcommand is used to show details about all catalogs, or those that a certain principal role has access to. The principal used to perform this operation must have the `CATALOG_LIST` privilege.

```
input: polaris catalogs list --help
options:
  list
    Named arguments:
      --principal-role  The name of a principal role
```

##### Examples

```
polaris catalogs list

polaris catalogs list --principal-role some_user
```

#### update

The `update` subcommand is used to update a catalog. Currently, this command supports changing the properties of a catalog or updating its storage configuration.

```
input: polaris catalogs update --help
options:
  update
    Named arguments:
      --default-base-location  A new default base location for the catalog
      --allowed-location  An allowed location for files tracked by the catalog. Multiple locations can be provided by specifying this option more than once.
      --region  (Only for S3) The region to use when connecting to S3
      --set-property  A key/value pair such as: tag=value. Merges the specified key/value into an existing properties map by updating the value if the key already exists or creating a new entry if not. Multiple can be provided by specifying this option more than once
      --remove-property  A key to remove from a properties map. If the key already does not exist then no action is takn for the specified key. If properties are also being set in the same update command then the list of removals is applied last. Multiple can be provided by specifying this option more than once
    Positional arguments:
      catalog
```

##### Examples

```
polaris catalogs update --property tag=new_value my_catalog

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

#### create

The `create` subcommand is used to create a new principal.

```
input: polaris principals create --help
options:
  create
    Named arguments:
      --type  The type of principal to create in [SERVICE]
      --property  A key/value pair such as: tag=value. Multiple can be provided by specifying this option more than once
    Positional arguments:
      principal
```

##### Examples

```
polaris principals create some_user

polaris principals create --client-id ${CLIENT_ID} --property admin=true some_admin_user
```

#### delete

The `delete` subcommand is used to delete a principal.

```
input: polaris principals delete --help
options:
  delete
    Positional arguments:
      principal
```

##### Examples

```
polaris principals delete some_user

polaris principals delete some_admin_user
```

#### get

The `get` subcommand retrieves details about a principal.

```
input: polaris principals get --help
options:
  get
    Positional arguments:
      principal
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
input: polaris principals rotate-credentials --help
options:
  rotate-credentials
    Positional arguments:
      principal
```

##### Examples

```
polaris principals rotate-credentials some_user

polaris principals rotate-credentials some_admin_user
```

#### update

The `update` subcommand is used to update a principal. Currently, this supports rewriting the properties associated with a principal.

```
input: polaris principals update --help
options:
  update
    Named arguments:
      --set-property  A key/value pair such as: tag=value. Merges the specified key/value into an existing properties map by updating the value if the key already exists or creating a new entry if not. Multiple can be provided by specifying this option more than once
      --remove-property  A key to remove from a properties map. If the key already does not exist then no action is takn for the specified key. If properties are also being set in the same update command then the list of removals is applied last. Multiple can be provided by specifying this option more than once
    Positional arguments:
      principal
```

##### Examples

```
polaris principals update --property key=value --property other_key=other_value some_user

polaris principals update --property are_other_keys_removed=yes some_user
```

#### access

The `access` subcommand retrieves entities relation about a principal.

```
input: polaris principals access --help
options:
  access
    Positional arguments:
      principal
```

##### Examples

```
polaris principals access quickstart_user
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

#### create

The `create` subcommand is used to create a new principal role.

```
input: polaris principal-roles create --help
options:
  create
    Named arguments:
      --property  A key/value pair such as: tag=value. Multiple can be provided by specifying this option more than once
    Positional arguments:
      principal_role
```

##### Examples

```
polaris principal-roles create data_engineer

polaris principal-roles create --property key=value data_analyst
```

#### delete

The `delete` subcommand is used to delete a principal role.

```
input: polaris principal-roles delete --help
options:
  delete
    Positional arguments:
      principal_role
```

##### Examples

```
polaris principal-roles delete data_engineer

polaris principal-roles delete data_analyst
```

#### get

The `get` subcommand retrieves details about a principal role.

```
input: polaris principal-roles get --help
options:
  get
    Positional arguments:
      principal_role
```

##### Examples

```
polaris principal-roles get data_engineer

polaris principal-roles get data_analyst
```

#### list

The list subcommand is used to print out all principal roles or, alternatively, to list all principal roles associated with a given principal or with a given catalog role.

```
input: polaris principal-roles list --help
options:
  list
    Named arguments:
      --catalog-role  The name of a catalog role. If provided, show only principal roles assigned to this catalog role.
      --principal  The name of a principal. If provided, show only principal roles assigned to this principal.
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
input: polaris principal-roles update --help
options:
  update
    Named arguments:
      --set-property  A key/value pair such as: tag=value. Merges the specified key/value into an existing properties map by updating the value if the key already exists or creating a new entry if not. Multiple can be provided by specifying this option more than once
      --remove-property  A key to remove from a properties map. If the key already does not exist then no action is takn for the specified key. If properties are also being set in the same update command then the list of removals is applied last. Multiple can be provided by specifying this option more than once
    Positional arguments:
      principal_role
```

##### Examples

```
polaris principal-roles update --property key=value2 data_engineer

polaris principal-roles update data_analyst --property key=value3
```

#### grant

The `grant` subcommand is used to grant a principal role to a principal.

```
input: polaris principal-roles grant --help
options:
  grant
    Named arguments:
      --principal  A principal to grant this principal role to
    Positional arguments:
      principal_role
```

##### Examples

```
polaris principal-roles grant --principal d.knuth data_engineer

polaris principal-roles grant data_scientist --principal a.ng
```

#### revoke

The `revoke` subcommand is used to revoke a principal role from a principal.

```
input: polaris principal-roles revoke --help
options:
  revoke
    Named arguments:
      --principal  A principal to revoke this principal role from
    Positional arguments:
      principal_role
```

##### Examples

```
polaris principal-roles revoke --principal former.employee data_engineer

polaris principal-roles revoke data_scientist --principal changed.role
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

#### create

The `create` subcommand is used to create a new catalog role.

```
input: polaris catalog-roles create --help
options:
  create
    Named arguments:
      --catalog  The name of an existing catalog
      --property  A key/value pair such as: tag=value. Multiple can be provided by specifying this option more than once
    Positional arguments:
      catalog_role
```

##### Examples

```
polaris catalog-roles create --property key=value --catalog some_catalog sales_data

polaris catalog-roles create --catalog other_catalog sales_data
```

#### delete

The `delete` subcommand is used to delete a catalog role.

```
input: polaris catalog-roles delete --help
options:
  delete
    Named arguments:
      --catalog  The name of an existing catalog
    Positional arguments:
      catalog_role
```

##### Examples

```
polaris catalog-roles delete --catalog some_catalog sales_data

polaris catalog-roles delete --catalog other_catalog sales_data
```

#### get

The `get` subcommand retrieves details about a catalog role.

```
input: polaris catalog-roles get --help
options:
  get
    Named arguments:
      --catalog  The name of an existing catalog
    Positional arguments:
      catalog_role
```

##### Examples

```
polaris catalog-roles get --catalog some_catalog inventory_data

polaris catalog-roles get --catalog other_catalog inventory_data
```

#### list

The `list` subcommand is used to print all catalog roles. Alternatively, if a principal role is provided, only catalog roles associated with that principal are shown.

```
input: polaris catalog-roles list --help
options:
  list
    Named arguments:
      --principal-role  The name of a principal role
    Positional arguments:
      catalog
```

##### Examples

```
polaris catalog-roles list

polaris catalog-roles list --principal-role data_engineer
```

#### update

The `update` subcommand is used to update a catalog role. Currently, only updating properties associated with the catalog role is supported.

```
input: polaris catalog-roles update --help
options:
  update
    Named arguments:
      --catalog  The name of an existing catalog
      --set-property  A key/value pair such as: tag=value. Merges the specified key/value into an existing properties map by updating the value if the key already exists or creating a new entry if not. Multiple can be provided by specifying this option more than once
      --remove-property  A key to remove from a properties map. If the key already does not exist then no action is takn for the specified key. If properties are also being set in the same update command then the list of removals is applied last. Multiple can be provided by specifying this option more than once
    Positional arguments:
      catalog_role
```

##### Examples

```
polaris catalog-roles update --property contains_pii=true --catalog some_catalog sales_data

polaris catalog-roles update sales_data --catalog some_catalog --property key=value
```

#### grant

The `grant` subcommand is used to grant a catalog role to a principal role.

```
input: polaris catalog-roles grant --help
options:
  grant
    Named arguments:
      --catalog  The name of an existing catalog
      --principal-role  The name of a catalog role
    Positional arguments:
      catalog_role
```

##### Examples

```
polaris catalog-roles grant sensitive_data --catalog some_catalog --principal-role power_user

polaris catalog-roles grant --catalog sales_data contains_cc_info_catalog_role --principal-role financial_analyst_role
```

#### revoke

The `revoke` subcommand is used to revoke a catalog role from a principal role.

```
input: polaris catalog-roles revoke --help
options:
  revoke
    Named arguments:
      --catalog  The name of an existing catalog
      --principal-role  The name of a catalog role
    Positional arguments:
      catalog_role
```

##### Examples

```
polaris catalog-roles revoke sensitive_data --catalog some_catalog --principal-role power_user

polaris catalog-roles revoke --catalog sales_data contains_cc_info_catalog_role --principal-role financial_analyst_role
```

### Namespaces

The `namespaces` command is used to manage namespaces within Polaris.

`namespaces` supports the following subcommands:

1. create
2. delete
3. get
4. list

#### create

The `create` subcommand is used to create a new namespace.

When creating a namespace with an explicit location, that location must reside within the parent catalog or namespace.

```
input: polaris namespaces create --help
options:
  create
    Named arguments:
      --catalog  The name of an existing catalog
      --location  If specified, the location at which to store the namespace and entities inside it
      --property  A key/value pair such as: tag=value. Multiple can be provided by specifying this option more than once
    Positional arguments:
      namespace
```

##### Examples

```
polaris namespaces create --catalog my_catalog outer

polaris namespaces create --catalog my_catalog --location 's3://bucket/outer/inner_SUFFIX' outer.inner
```

#### delete

The `delete` subcommand is used to delete a namespace.

```
input: polaris namespaces delete --help
options:
  delete
    Named arguments:
      --catalog  The name of an existing catalog
    Positional arguments:
      namespace
```

##### Examples

```
polaris namespaces delete  outer_namespace.inner_namespace --catalog my_catalog

polaris namespaces delete --catalog my_catalog outer_namespace
```

#### get

The `get` subcommand retrieves details about a namespace.

```
input: polaris namespaces get --help
options:
  get
    Named arguments:
      --catalog  The name of an existing catalog
    Positional arguments:
      namespace
```

##### Examples

```
polaris namespaces get --catalog some_catalog a.b

polaris namespaces get a.b.c --catalog some_catalog
```

#### list

The `list` subcommand shows details about all namespaces directly within a catalog or, optionally, within some parent prefix in that catalog.

```
input: polaris namespaces list --help
options:
  list
    Named arguments:
      --catalog  The name of an existing catalog
      --parent  If specified, list namespaces inside this parent namespace
```

##### Examples

```
polaris namespaces list --catalog my_catalog

polaris namespaces list --catalog my_catalog --parent a

polaris namespaces list --catalog my_catalog --parent a.b
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
input: polaris privileges list --help
options:
  list
    Named arguments:
      --catalog  The name of an existing catalog
      --catalog-role  The name of a catalog role
```

##### Examples

```
polaris privileges  list --catalog my_catalog --catalog-role my_role

polaris privileges my_role list --catalog-role my_other_role --catalog my_catalog
```

#### catalog

The `catalog` subcommand manages privileges at the catalog level. `grant` is used to grant catalog privileges to the specified catalog role, and `revoke` is used to revoke them.

```
input: polaris privileges catalog --help
options:
  catalog
    grant
      Named arguments:
        --catalog  The name of an existing catalog
        --catalog-role  The name of a catalog role
      Positional arguments:
        privilege
    revoke
      Named arguments:
        --cascade  When revoking privileges, additionally revoke privileges that depend on the specified privilege
        --catalog  The name of an existing catalog
        --catalog-role  The name of a catalog role
      Positional arguments:
        privilege
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
input: polaris privileges namespace --help
options:
  namespace
    grant
      Named arguments:
        --namespace  A period-delimited namespace
        --catalog  The name of an existing catalog
        --catalog-role  The name of a catalog role
      Positional arguments:
        privilege
    revoke
      Named arguments:
        --namespace  A period-delimited namespace
        --cascade  When revoking privileges, additionally revoke privileges that depend on the specified privilege
        --catalog  The name of an existing catalog
        --catalog-role  The name of a catalog role
      Positional arguments:
        privilege
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
input: polaris privileges table --help
options:
  table
    grant
      Named arguments:
        --namespace  A period-delimited namespace
        --table  The name of a table
        --catalog  The name of an existing catalog
        --catalog-role  The name of a catalog role
      Positional arguments:
        privilege
    revoke
      Named arguments:
        --namespace  A period-delimited namespace
        --table  The name of a table
        --cascade  When revoking privileges, additionally revoke privileges that depend on the specified privilege
        --catalog  The name of an existing catalog
        --catalog-role  The name of a catalog role
      Positional arguments:
        privilege
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
input: polaris privileges view --help
options:
  view
    grant
      Named arguments:
        --namespace  A period-delimited namespace
        --view  The name of a view
        --catalog  The name of an existing catalog
        --catalog-role  The name of a catalog role
      Positional arguments:
        privilege
    revoke
      Named arguments:
        --namespace  A period-delimited namespace
        --view  The name of a view
        --cascade  When revoking privileges, additionally revoke privileges that depend on the specified privilege
        --catalog  The name of an existing catalog
        --catalog-role  The name of a catalog role
      Positional arguments:
        privilege
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

### profiles

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
input: polaris profiles create --help
options:
  create
    Positional arguments:
      profile
```

##### Examples

```
polaris profiles create dev
```

#### delete

The `delete` subcommand removes a stored profile.

```
input: polaris profiles delete --help
options:
  delete
    Positional arguments:
      profile
```

##### Examples

```
polaris profiles delete dev
```

#### get

The `get` subcommand removes a stored profile.

```
input: polaris profiles get --help
options:
  get
    Positional arguments:
      profile
```

##### Examples

```
polaris profiles get dev
```

#### list

The `list` subcommand displays all stored profiles.

```
input: polaris profiles list --help
options:
  list
```

##### Examples

```
polaris profiles list
```

#### update

The `update` subcommand modifies an existing profile.

```
input: polaris profiles update --help
options:
  update
    Positional arguments:
      profile
```

##### Examples

```
polaris profiles update dev
```

### repair

The `repair` command is a bash script wrapper used to regenerate Python client code and update necessary dependencies, ensuring the Polaris client remains up-to-date and functional. **Please note that this command does not support any options and its usage information is not available via a `--help` flag.**

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
  --catalog my_catalog \
  --catalog-role my_catalog_role \
  grant \
  CATALOG_MANAGE_CONTENT
```

### Identifying the tables a given principal has been granted explicit access to read

_Note that some other privileges, such as `CATALOG_MANAGE_CONTENT`, subsume `TABLE_READ_DATA` and would not be discovered here._

```
principal_roles=$(polaris principal-roles list --principal my_principal)
for principal_role in ${principal_roles}; do
  catalog_roles=$(polaris catalog-roles --list --principal-role "${principal_role}")
  for catalog_role in ${catalog_roles}; do
    grants=$(polaris privileges list  --catalog-role "${catalog_role}" --catalog "${catalog}")
    for grant in $(echo "${grants}" | jq -c '.[] | select(.privilege == "TABLE_READ_DATA")'); do
      echo "${grant}"
    done
  done
done
```
