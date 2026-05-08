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

Polaris includes a tool for administrators to manage the [metastore]({{% ref "metastores" %}}).

The tool is available as a Docker image: `apache/polaris-admin-tool`. It can also be downloaded as part of the [binary distribution]({{% ref "getting-started/binary-distribution" %}}).

{{< alert note >}} 
The tool must be built with the necessary database drivers to access the metastore database. 
The default build includes drivers for the PostgreSQL and NoSQL (MongoDB) backends.
{{< /alert >}}

## Usage

Please make sure the admin tool and Polaris server are with the same version before using it.
To run the standalone JAR, use the following command:

If you downloaded the [binary distribution]({{% ref "getting-started/binary-distribution" %}}), you
can run the admin tool as follows:

```shell
java -jar polaris-bin-<version>/admin/quarkus-run.jar --help
```

Make sure to replace `<version>` with the actual version of Polaris you are using.

To run the Docker image instead, use the following command:

```shell
docker run apache/polaris-admin-tool:latest --help
```

The basic usage of the Polaris Admin Tool is outlined below:

```
Usage: polaris-admin-tool.jar [-hV] [COMMAND]
Polaris administration & maintenance tool
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
Commands:
  help       Display help information about the specified command.
  bootstrap  Bootstraps realms and root principal credentials.
  purge      Purge realms and all associated entities.
  nosql      Sub-commands specific to NoSQL persistence.
```

## Configuration

The Polaris Admin Tool must be executed with the same configuration as the Polaris server. The
configuration can be done via environment variables or system properties.

At a minimum, it is necessary to configure the Polaris Admin Tool to connect to the same database
used by the Polaris server.

See the [Metastores]({{% ref "metastores" %}}) section for more information on configuring the
database connection. See [Configuration Reference]({{% ref "configuration/configuration-reference"
%}}) for the full list of configuration options.

Note: Polaris will always create the schema `polaris_schema` during bootstrap under the configured database.

## Bootstrapping Realms and Principal Credentials

The `bootstrap` command is used to bootstrap realms and create the necessary principal credentials
for the Polaris server. This command is idempotent and can be run multiple times without causing any
issues. If a realm is already bootstrapped, running the `bootstrap` command again will not have any
effect on that realm.

If you have downloaded the [binary distribution]({{% ref "getting-started/binary-distribution" %}}),
you can run the `bootstrap` command as follows:

```shell
java -jar polaris-bin-<version>/admin/quarkus-run.jar bootstrap --help
```

You can also use the Docker image to run the `bootstrap` command:

```shell
docker run apache/polaris-admin-tool:latest bootstrap --help
```

The basic usage of the `bootstrap` command is outlined below:

```
Usage: polaris-admin-tool.jar bootstrap [-hV] [-v=<schema version>]
                                        ([-r=<realm> [-r=<realm>]... [-c=<realm,
                                        clientId,clientSecret>]... [-p]] |
                                        [[-f=<file>]])
Bootstraps realms and root principal credentials.
  -h, --help                Show this help message and exit.
  -v, --schema-version=<schema version>
                            The version of the schema to load in [1, 2, 3,
                              LATEST].
  -V, --version             Print version information and exit.
Standard Input Options:
  -c, --credential=<realm,clientId,clientSecret>
                            Root principal credentials to bootstrap. Must be of
                              the form 'realm,clientId,clientSecret'.
  -p, --print-credentials   Print root credentials to stdout
  -r, --realm=<realm>       The name of a realm to bootstrap.
File Input Options:
  -f, --credentials-file=<file>
                            A file containing root principal credentials to
                              bootstrap.
```

For example, to bootstrap the `realm1` realm and create its root principal credential with the
client ID `admin` and client secret `admin`, you can run the following commands:

Example for the PostgreSQL backend:

```bash
docker run --rm -it \
  --env="polaris.persistence.type=relational-jdbc" \
  --env="quarkus.datasource.username=<your-username>" \
  --env="quarkus.datasource.password=<your-password>" \
  --env="quarkus.datasource.jdbc.url=<jdbc-url-of-postgres>" \
  apache/polaris-admin-tool:latest bootstrap -r realm1 -c realm1,admin,admin
```

Example for the NoSQL (MongoDB) backend:

```bash
docker run --rm -it \
  --env="polaris.persistence.type=nosql" \
  --env="polaris.persistence.nosql.backend=MongoDb" \
  --env="quarkus.mongodb.database=polaris" \
  --env="quarkus.mongodb.connection-string=<mongodb-connection-string>" \
  apache/polaris-admin-tool:latest bootstrap -r realm1 -c realm1,admin,admin
```

As you can see, the Polaris Admin Tool must be executed with appropriate configuration to connect to the same database used by the Polaris server. The configuration can be done via environment variables (as above) or system properties.

To know which configuration options you should use, read the [Metastores]({{% ref "metastores" %}}) section and the documentation of the specific metastore backend you are using.

## Purging Realms and Principal Credentials

The `purge` command is used to remove realms and principal credentials from the Polaris server.

{{< alert warning >}}
Running the `purge` command will remove all data associated with the specified realms!
This includes all entities (catalogs, namespaces, tables, views, roles), all principal
credentials, grants, and any other data associated with the realms.
{{< /alert >}}

If you have downloaded the [binary distribution]({{% ref "getting-started/binary-distribution" %}}), you can run the `purge` command as follows:

```shell
java -jar polaris-bin-<version>/admin/quarkus-run.jar purge --help
```

You can also use the Docker image to run the `purge` command:

```shell
docker run apache/polaris-admin-tool:latest purge --help
```

The basic usage of the `purge` command is outlined below:

```
Usage: polaris-admin-tool.jar purge [-hV] -r=<realm> [-r=<realm>]...
Purge realms and all associated entities.
  -h, --help            Show this help message and exit.
  -r, --realm=<realm>   The name of a realm to purge.
  -V, --version         Print version information and exit.
```

For example, to purge the `realm1` realm, you can run the following commands:

Example for the PostgreSQL backend:

```bash
docker run --rm -it \
  --env="polaris.persistence.type=relational-jdbc" \
  --env="quarkus.datasource.username=<your-username>" \
  --env="quarkus.datasource.password=<your-password>" \
  --env="quarkus.datasource.jdbc.url=<jdbc-url-of-postgres>" \
  apache/polaris-admin-tool:latest purge -r realm1
```

Example for the NoSQL (MongoDB) backend:

```bash
docker run --rm -it \
  --env="polaris.persistence.type=nosql" \
  --env="polaris.persistence.nosql.backend=MongoDb" \
  --env="quarkus.mongodb.database=polaris" \
  --env="quarkus.mongodb.connection-string=<mongodb-connection-string>" \
  apache/polaris-admin-tool:latest purge -r realm1
```

Again, the Polaris Admin Tool must be executed with appropriate configuration to connect to the same database used by the Polaris server. The configuration can be done via environment variables (as above) or system properties.

## NoSQL Specific Operations

The `nosql` admin tool command is used to perform NoSQL metastore specific operations.
```
Usage: polaris-admin-tool.jar nosql [-hV] [COMMAND]
Sub-commands specific to NoSQL persistence.
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
Commands:
  maintenance-info  Polaris NoSQL persistence maintenance information.
  maintenance-log   Show Polaris persistence maintenance log.
  maintenance-run   Run Polaris persistence maintenance.
```

### Running NoSQL maintenance

NoSQL maintenance is run using the `nosql maintenance-run` command.
Maintenance covers the deletion of stale database entries.

{{< alert note >}}
It is recommended to run the `nosql maintenance-run` command regulary, for example, once per day.
{{< /alert >}}

The output shows a bunch of configuration options, most of which are automatically determined by the tool.

```
Maintenance configuration:
            created-at grace time: PT3H
                delete batch size: 10
                    retained runs: 50
            expected object count: 100000
         expected reference count: 100
              last-run multiplier: 1.100000
                  initialized FPP: 0.000010
                     expected FPP: 0.000050
  reference scan rate limit / sec: (unlimited)
  object    scan rate limit / sec: (unlimited)
```

The remaining output is a detailed report of the maintenance run.

The implementation of NoSQL maintenance is self-tuning and does usually not require additional configuration.

If you encounter that the maintenance run operation puts too much load on the database,
you can adjust scan rates to reduce the load.
The option for the reference and object scan rates are
`polaris.persistence.nosql.maintenance.object-scan-rate-limit-per-second` and
`polaris.persistence.nosql.maintenance.reference-scan-rate-limit-per-second`, both default to 'unlimited.'
It is safe to let the maintenance run for a long time, there is no runtime limit.
Alternatively, you can run maintenance at high or the default unlimited scan rate during off-peak hours.

{{< alert note >}}
The `nosql maintenance-run` command may emit a warning that not all purgable database entries have been deleted.
In this case, the tool has already determined the parameters that will be effective in the next maintenance run.
This warning is informative and can usually be ignored.

The number of expected objects and references is calculated from the actual number of objects and references
scanned during the last run, multiplied by the `count-from-last-run-multiplier` value.

Encountering this warning often, especially in multiple consecutive maintenance runs, is a sign that
more data is added to the database than expected. In such a case, update the
`polaris.persistence.nosql.maintenance.count-from-last-run-multiplier` configuration option to a higher value.
It defaults to `1.1`.
{{< /alert >}}

All NoSQL maintenance configuration options are under the `polaris.persistence.nosql.maintenance` namespace.
See the `Configuration Reference` pages for more information.

Avoid running multiple instances of the `nosql maintenance-run` command at the same time to avoid an unnecessary
database load, although this does not cause any other issues.

### Inspecting NoSQL maintenance run logs

The `nosql maintenance-log` command shows the logs of the recent NoSQL maintenance run.

This command is useful for auditing and debugging purposes.

### Inspecting NoSQL maintenance information

The `nosql maintenance-info` command shows the effective maintenance parameters.
This information is also shown in the output of the `nosql maintenance-run` command.
