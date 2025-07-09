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
title: Admin Tool
type: docs
weight: 300
---

Polaris includes a tool for administrators to manage the metastore.

The tool must be built with the necessary JDBC drivers to access the metastore database. For
example, to build the tool with support for Postgres, run the following:

```shell
./gradlew \
  :polaris-admin:assemble \
  :polaris-admin:quarkusAppPartsBuild --rerun \
  -Dquarkus.container-image.build=true
```

The above command will generate:

- One Fast-JAR in `runtime/admin/build/quarkus-app/quarkus-run.jar`
- Two Docker images named `apache/polaris-admin-tool:latest` and `apache/polaris-admin-tool:<version>`

## Usage

Please make sure the admin tool and Polaris server are with the same version before using it.
To run the standalone JAR, use the following command:

```shell
java -jar runtime/admin/build/quarkus-app/quarkus-run.jar --help
```

To run the Docker image, use the following command:

```shell
docker run apache/polaris-admin-tool:latest --help
```

The basic usage of the Polaris Admin Tool is outlined below:

```
Usage: polaris-admin-runner.jar [-hV] [COMMAND]
Polaris Admin Tool
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
Commands:
  help       Display help information about the specified command.
  bootstrap  Bootstraps realms and principal credentials.
  purge      Purge principal credentials.
```

## Configuration

The Polaris Admin Tool must be executed with the same configuration as the Polaris server. The
configuration can be done via environment variables or system properties.

At a minimum, it is necessary to configure the Polaris Admin Tool to connect to the same database
used by the Polaris server.

See the [metastore documentation]({{% ref "metastores" %}}) for more information on configuring the
database connection.

Note: Polaris will always create schema 'polaris_schema' during bootstrap under the configured database.

## Bootstrapping Realms and Principal Credentials

The `bootstrap` command is used to bootstrap realms and create the necessary principal credentials
for the Polaris server. This command is idempotent and can be run multiple times without causing any
issues. If a realm is already bootstrapped, running the `bootstrap` command again will not have any
effect on that realm.

```shell
java -jar runtime/admin/build/quarkus-app/quarkus-run.jar bootstrap --help
```

The basic usage of the `bootstrap` command is outlined below:

```
Usage: polaris-admin-runner.jar bootstrap [-hV] [-c=<realm,clientId,
       clientSecret>]... -r=<realm> [-r=<realm>]...
Bootstraps realms and root principal credentials.
  -c, --credential=<realm,clientId,clientSecret>
                        Root principal credentials to bootstrap. Must be of the form
                          'realm,clientId,clientSecret'.
  -h, --help            Show this help message and exit.
  -r, --realm=<realm>   The name of a realm to bootstrap.
  -V, --version         Print version information and exit.
```

For example, to bootstrap the `realm1` realm and create its root principal credential with the
client ID `admin` and client secret `admin`, you can run the following command:

```shell
java -jar runtime/admin/build/quarkus-app/quarkus-run.jar bootstrap -r realm1 -c realm1,admin,admin
```

## Purging Realms and Principal Credentials

The `purge` command is used to remove realms and principal credentials from the Polaris server.

> [!WARNING]
> Running the `purge` command will remove all data associated with the specified realms!
  This includes all entities (catalogs, namespaces, tables, views, roles), all principal
  credentials, grants, and any other data associated with the realms.

```shell
java -jar runtime/admin/build/quarkus-app/quarkus-run.jar purge --help
```

The basic usage of the `purge` command is outlined below:

```
Usage: polaris-admin-runner.jar purge [-hV] -r=<realm> [-r=<realm>]...
Purge realms and all associated entities.
  -h, --help            Show this help message and exit.
  -r, --realm=<realm>   The name of a realm to purge.
  -V, --version         Print version information and exit.
```

For example, to purge the `realm1` realm, you can run the following command:

```shell
java -jar runtime/admin/build/quarkus-app/quarkus-run.jar purge -r realm1
```
