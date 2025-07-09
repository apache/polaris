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
title: Configuring Polaris
type: docs
weight: 550
---

## Overview

This page provides information on how to configure Apache Polaris (Incubating). Unless stated
otherwise, this information is valid both for Polaris Docker images (and Kubernetes deployments) as
well as for Polaris binary distributions.

> [!NOTE]
> For Production tips and best practices, refer to [Configuring Polaris for Production]({{% ref "configuring-polaris-for-production.md" %}}).

First off, Polaris server runs on Quarkus, and uses its configuration mechanisms. Read Quarkus
[configuration guide](https://quarkus.io/guides/config) to get familiar with the basics.

Quarkus aggregates configuration properties from multiple sources, applying them in a specific order
of precedence. When a property is defined in multiple sources, the value from the source with the
higher priority overrides those from lower-priority sources.

The sources are listed below, from highest to lowest priority:

1. System properties: properties set via the Java command line using `-Dproperty.name=value`.
2. Environment variables (see below for important details).
3. Settings in `$PWD/config/application.properties` file.
4. The `application.properties` files packaged in Polaris.
5. Default values: hardcoded defaults within the application.

When using environment variables, there are two naming conventions:

1. If possible, just use the property name as the environment variable name. This works fine in most
   cases, e.g. in Kubernetes deployments. For example, `polaris.realm-context.realms` can be
   included as is in a container YAML definition:
   ```yaml
   env:
   - name: "polaris.realm-context.realms"
     value: "realm1,realm2"
   ```

2. If running from a script or shell prompt, however, stricter naming rules apply: variable names
   can consist solely of uppercase letters, digits, and the `_` (underscore) sign. In such
   situations, the environment variable name must be derived from the property name, by using
   uppercase letters, and replacing all dots, dashes and quotes by underscores. For example,
   `polaris.realm-context.realms` becomes `POLARIS_REALM_CONTEXT_REALMS`. See
   [here](https://smallrye.io/smallrye-config/Main/config/environment-variables/) for more details.

> [!IMPORTANT]
> While convenient, uppercase-only environment variables can be problematic for complex property
> names. In these situations, it's preferable to use system properties or a configuration file.

As stated above, a configuration file can also be provided at runtime; it should be available
(mounted) at `$PWD/config/application.properties` for Polaris server to recognize it. In Polaris
official Docker images, this location is `/deployment/config/application.properties`.

For Kubernetes deployments, the configuration file is typically defined as a `ConfigMap`, then
mounted in the container at `/deployment/config/application.properties`. It can be mounted in
read-only mode, as Polaris only reads the configuration file once, at startup.

## Polaris Configuration Options Reference

| Configuration Property                                                                 | Default Value         | Description                                                                                                                                                                                                                                                                                                                                                   |
|----------------------------------------------------------------------------------------|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `polaris.persistence.type`                                                             | `relational-jdbc`     | Define the persistence backend used by Polaris (`in-memory`, `relational-jdbc`, `eclipse-link` (deprecated)). See [Configuring Apache Polaris for Production)[{{% ref "configuring-polaris-for-production.md" %}})                                                                                                                                            |
| `polaris.persistence.relational.jdbc.max-retries`                                      | `1`                   | Total number of retries JDBC persistence will attempt on connection resets or serialization failures before giving up.                                                                                                                                                                                                                                        |
| `polaris.persistence.relational.jdbc.max_duaration_in_ms`                              | `5000 ms`             | Max time interval (ms) since the start of a transaction when retries can be attempted.                                                                                                                                                                                                                                                                        |
| `polaris.persistence.relational.jdbc.initial_delay_in_ms`                              | `100 ms`              | Initial delay before retrying. The delay is doubled after each retry.                                                                                                                                                                                                                                                                                         |
| `polaris.persistence.eclipselink.configurationFile`                                    |                       | Define the location of the `persistence.xml`. By default, it's the built-in `persistence.xml` in use.                                                                                                                                                                                                                                                         |
| `polaris.persistence.eclipselink.persistenceUnit`                                      | `polaris`             | Define the name of the persistence unit to use, as defined in the `persistence.xml`.                                                                                                                                                                                                                                                                          |
| `polaris.realm-context.type`                                                           | `default`             | Define the type of the Polaris realm to use.                                                                                                                                                                                                                                                                                                                  |
| `polaris.realm-context.realms`                                                         | `POLARIS`             | Define the list of realms to use.                                                                                                                                                                                                                                                                                                                             |
| `polaris.realm-context.header-name`                                                    | `Polaris-Realm`       | Define the header name defining the realm context.                                                                                                                                                                                                                                                                                                            |
| `polaris.features."ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING"`           | `false`               | Flag to enforce check if credential rotation.                                                                                                                                                                                                                                                                                                                 |
| `polaris.features."SUPPORTED_CATALOG_STORAGE_TYPES"`                                   | `FILE`                | Define the catalog supported storage. Supported values are `S3`, `GCS`, `AZURE`, `FILE`.                                                                                                                                                                                                                                                                      |
| `polaris.features.realm-overrides."my-realm"."SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION"` | `true`                | "Override" realm features, here the skip credential subscoping indirection flag.                                                                                                                                                                                                                                                                              |
| `polaris.authentication.authenticator.type`                                            | `default`             | Define the Polaris authenticator type.                                                                                                                                                                                                                                                                                                                        |
| `polaris.authentication.token-service.type`                                            | `default`             | Define the Polaris token service type.                                                                                                                                                                                                                                                                                                                        |
| `polaris.authentication.token-broker.type`                                             | `rsa-key-pair`        | Define the Polaris token broker type. Also configure the location of the key files. For RSA: if the locations of the key files are not configured, an ephemeral key-pair will be created on each Polaris server instance startup, which breaks existing tokens after server restarts and is also incompatible with running multiple Polaris server instances. |
| `polaris.authentication.token-broker.max-token-generation`                             | `PT1H`                | Define the max token generation policy on the token broker.                                                                                                                                                                                                                                                                                                   |
| `polaris.authentication.token-broker.rsa-key-pair.private-key-file`                    |                       | Define the location of the RSA-256 private key file, if present the `public-key` file must be specified, too.                                                                                                                                                                                                                                                 |
| `polaris.authentication.token-broker.rsa-key-pair.public-key-file`                     |                       | Define the location of the RSA-256 public key file, if present the `private-key` file must be specified, too.                                                                                                                                                                                                                                                 |
| `polaris.authentication.token-broker.symmetric-key.secret`                             | `secret`              | Define the secret of the symmetric key.                                                                                                                                                                                                                                                                                                                       |
| `polaris.authentication.token-broker.symmetric-key.file`                               | `/tmp/symmetric.key`  | Define the location of the symmetric key file.                                                                                                                                                                                                                                                                                                                |
| `polaris.storage.aws.access-key`                                                       | `accessKey`           | Define the AWS S3 access key. If unset, the default credential provider chain will be used.                                                                                                                                                                                                                                                                   |
| `polaris.storage.aws.secret-key`                                                       | `secretKey`           | Define the AWS S3 secret key. If unset, the default credential provider chain will be used.                                                                                                                                                                                                                                                                   |
| `polaris.storage.gcp.token`                                                            | `token`               | Define the Google Cloud Storage token. If unset, the default credential provider chain will be used.                                                                                                                                                                                                                                                          |
| `polaris.storage.gcp.lifespan`                                                         | `PT1H`                | Define the Google Cloud Storage lifespan type. If unset, the default credential provider chain will be used.                                                                                                                                                                                                                                                  |
| `polaris.log.request-id-header-name`                                                   | `Polaris-Request-Id`  | Define the header name to match request ID in the log.                                                                                                                                                                                                                                                                                                        |
| `polaris.log.mdc.aid`                                                                  | `polaris`             | Define the log context (e.g. MDC) AID.                                                                                                                                                                                                                                                                                                                        |
| `polaris.log.mdc.sid`                                                                  | `polaris-service`     | Define the log context (e.g. MDC) SID.                                                                                                                                                                                                                                                                                                                        |
| `polaris.rate-limiter.filter.type`                                                     | `no-op`               | Define the Polaris rate limiter. Supported values are `no-op`, `token-bucket`.                                                                                                                                                                                                                                                                                |
| `polaris.rate-limiter.token-bucket.type`                                               | `default`             | Define the token bucket rate limiter.                                                                                                                                                                                                                                                                                                                         |
| `polaris.rate-limiter.token-bucket.requests-per-second`                                | `9999`                | Define the number of requests per second for the token bucket rate limiter.                                                                                                                                                                                                                                                                                   |
| `polaris.rate-limiter.token-bucket.window`                                             | `PT10S`               | Define the window type for the token bucket rate limiter.                                                                                                                                                                                                                                                                                                     |
| `polaris.metrics.tags.<tag-name>=<tag-value>`                                          | `application=Polaris` | Define arbitrary metric tags to include in every request.                                                                                                                                                                                                                                                                                                     |
| `polaris.metrics.realm-id-tag.api-metrics-enabled`                                     | `false`               | Whether to enable the `realm_id` metric tag in API metrics.                                                                                                                                                                                                                                                                                                   |
| `polaris.metrics.realm-id-tag.http-metrics-enabled`                                    | `false`               | Whether to enable the `realm_id` metric tag in HTTP request metrics.                                                                                                                                                                                                                                                                                          |
| `polaris.metrics.realm-id-tag.http-metrics-max-cardinality`                            | `100`                 | The maximum cardinality for the `realm_id` tag in HTTP request metrics.                                                                                                                                                                                                                                                                                       |
| `polaris.tasks.max-concurrent-tasks`                                                   | `100`                 | Define the max number of concurrent tasks.                                                                                                                                                                                                                                                                                                                    |
| `polaris.tasks.max-queued-tasks`                                                       | `1000`                | Define the max number of tasks in queue.                                                                                                                                                                                                                                                                                                                      |
 | `polaris.config.rollback.compaction.on-conflicts.enabled`                              | `false`              | When set to true Polaris will apply the deconfliction by rollbacking those REPLACE operations snapshots which have the property of `polaris.internal.rollback.compaction.on-conflict` in their snapshot summary set to `rollback`, to resolve conflicts at the server end.                                                                                    |

There are non Polaris configuration properties that can be useful:

| Configuration Property                               | Default Value                   | Description                                                                 |
|------------------------------------------------------|---------------------------------|-----------------------------------------------------------------------------|
| `quarkus.log.level`                                  | `INFO`                          | Define the root log level.                                                  |
| `quarkus.log.category."org.apache.polaris".level`    |                                 | Define the log level for a specific category.                               |
| `quarkus.default-locale`                             | System locale                   | Force the use of a specific locale, for instance `en_US`.                   |
| `quarkus.http.port`                                  | `8181`                          | Define the HTTP port number.                                                |
| `quarkus.http.auth.basic`                            | `false`                         | Enable the HTTP basic authentication.                                       |
| `quarkus.http.limits.max-body-size`                  | `10240K`                        | Define the HTTP max body size limit.                                        |
| `quarkus.http.cors.origins`                          |                                 | Define the HTTP CORS origins.                                               |
| `quarkus.http.cors.methods`                          | `PATCH, POST, DELETE, GET, PUT` | Define the HTTP CORS covered methods.                                       |
| `quarkus.http.cors.headers`                          | `*`                             | Define the HTTP CORS covered headers.                                       |
| `quarkus.http.cors.exposed-headers`                  | `*`                             | Define the HTTP CORS covered exposed headers.                               |
| `quarkus.http.cors.access-control-max-age`           | `PT10M`                         | Define the HTTP CORS access control max age.                                |
| `quarkus.http.cors.access-control-allow-credentials` | `true`                          | Define the HTTP CORS access control allow credentials flag.                 |
| `quarkus.management.enabled`                         | `true`                          | Enable the management server.                                               |
| `quarkus.management.port`                            | `8182`                          | Define the port number of the Polaris management server.                    |
| `quarkus.management.root-path`                       |                                 | Define the root path where `/metrics` and `/health` endpoints are based on. |
| `quarkus.otel.sdk.disabled`                          | `true`                          | Enable the OpenTelemetry layer.                                             |

> [!NOTE]
> This section is only relevant for Polaris Docker images and Kubernetes deployments.

There are many other actionable environment variables available in the official Polaris Docker
image; they come from the base image used by Polaris, [ubi9/openjdk-21-runtime]. They should be used
to fine-tune the Java runtime directly, e.g. to enable debugging or to set the heap size. These
variables are not specific to Polaris, but are inherited from the base image. If in doubt, leave
everything at its default!

[ubi9/openjdk-21-runtime]: https://catalog.redhat.com/software/containers/ubi9/openjdk-21-runtime/6501ce769a0d86945c422d5f

| Environment variable             | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
|----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `JAVA_OPTS` or `JAVA_OPTIONS`    | **NOT RECOMMENDED**. JVM options passed to the `java` command (example: "-verbose:class"). Setting this variable will override all options set by any of the other variables in this table. To pass extra settings, use `JAVA_OPTS_APPEND` instead.                                                                                                                                                                                                                                                                                    |
| `JAVA_OPTS_APPEND`               | User specified Java options to be appended to generated options in `JAVA_OPTS` (example: "-Dsome.property=foo").                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `JAVA_TOOL_OPTIONS`              | This variable is defined and honored by all OpenJDK distros, see [here](https://bugs.openjdk.org/browse/JDK-4971166). Options defined here take precedence over all else; using this variable is generally not necessary, but can be useful e.g. to enforce JVM startup parameters, to set up remote debug, or to define JVM agents.                                                                                                                                                                                                   |
| `JAVA_MAX_MEM_RATIO`             | Is used to calculate a default maximal heap memory based on a containers restriction. If used in a container without any memory constraints for the container then this option has no effect. If there is a memory constraint then `-XX:MaxRAMPercentage` is set to a ratio of the container available memory as set here. The default is `80` which means 80% of the available memory is used as an upper boundary. You can skip this mechanism by setting this value to `0` in which case no `-XX:MaxRAMPercentage` option is added. |
| `JAVA_DEBUG`                     | If set remote debugging will be switched on. Disabled by default (example: true").                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `JAVA_DEBUG_PORT`                | Port used for remote debugging. Defaults to "5005" (tip: use "*:5005" to enable debugging on all network interfaces).                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `GC_MIN_HEAP_FREE_RATIO`         | Minimum percentage of heap free after GC to avoid expansion. Default is 10.                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `GC_MAX_HEAP_FREE_RATIO`         | Maximum percentage of heap free after GC to avoid shrinking. Default is 20.                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `GC_TIME_RATIO`                  | Specifies the ratio of the time spent outside the garbage collection. Default is 4.                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `GC_ADAPTIVE_SIZE_POLICY_WEIGHT` | The weighting given to the current GC time versus previous GC times. Default is 90.                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `GC_METASPACE_SIZE`              | The initial metaspace size. There is no default (example: "20").                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `GC_MAX_METASPACE_SIZE`          | The maximum metaspace size. There is no default (example: "100").                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `GC_CONTAINER_OPTIONS`           | Specify Java GC to use. The value of this variable should contain the necessary JRE command-line options to specify the required GC, which will override the default of `-XX:+UseParallelGC` (example: `-XX:+UseG1GC`).                                                                                                                                                                                                                                                                                                                |
Here are some examples:

| Example                                    | `docker run` option                                                                                                 |
|--------------------------------------------|---------------------------------------------------------------------------------------------------------------------|
| Using another GC                           | `-e GC_CONTAINER_OPTIONS="-XX:+UseShenandoahGC"` lets Polaris use Shenandoah GC instead of the default parallel GC. |
| Set the Java heap size to a _fixed_ amount | `-e JAVA_OPTS_APPEND="-Xms8g -Xmx8g"` lets Polaris use a Java heap of 8g.                                           |
| Set the maximum heap percentage            | `-e JAVA_MAX_MEM_RATIO="70"` lets Polaris use 70% percent of the available memory.                                  |


## Troubleshooting Configuration Issues

If you encounter issues with the configuration, you can ask Polaris to print out the configuration it
is using. To do this, set the log level for the `io.smallrye.config` category to `DEBUG`, and also
set the console appender level to `DEBUG`:

```properties
quarkus.log.console.level=DEBUG
quarkus.log.category."io.smallrye.config".level=DEBUG
```

> [!IMPORTANT] This will print out all configuration values, including sensitive ones like
> passwords. Don't do this in production, and don't share this output with anyone you don't trust!
