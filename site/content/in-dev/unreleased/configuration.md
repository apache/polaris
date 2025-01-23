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
title: Configuring Apache Polaris (Incubating)
linkTitle: Configuring Polaris
type: docs
weight: 550
---

## Overview

This page provides information on how to configure Apache Polaris (Incubating). Unless stated
otherwise, this information is valid both for Polaris Docker images (and Kubernetes deployments) as
well as for Polaris binary distributions.

> Note: for Production tips and best practices, refer to [Configuring Polaris for Production]({{% ref "configuring-polaris-for-production.md" %}}).

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

The following configuration options are available for Polaris:

TODO

## Java Runtime Configuration

> Note: This section is only relevant for Polaris Docker images and Kubernetes deployments.

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
