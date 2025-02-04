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
title: Metastores
type: docs
weight: 700
---

This page documents important configurations for connecting to a production database through [EclipseLink](https://eclipse.dev/eclipselink/).

## Building Polaris with EclipseLink

Polaris doesn't ship with any JDBC driver. You must specify them when building Polaris with
EclipseLink by using Gradle's project property:
`-PeclipseLinkDeps=<jdbc-driver-artifact1>,<jdbc-driver-artifact2>,...`. See below examples for H2
and Postgres.

## Polaris Server Configuration

Configure the `polaris.persistence` section in your Polaris configuration file
(`application.properties`) as follows:

```
polaris.persistence.type=eclipse-link
polaris.persistence.eclipselink.configuration-file=/path/to/persistence.xml
polaris.persistence.eclipselink.persistence-unit=polaris
```

Alternatively, configuration can also be done with environment variables or system properties. Refer
to the [Quarkus Configuration Reference] for more information.

The `configuration-file` option must point to an [EclipseLink configuration file]. This file, named
`persistence.xml`, is used to set up the database connection properties, which can differ depending
on the type of database and its configuration.

> Note: You have to locate the `persistence.xml` at least two folders down to the root folder, e.g. `/deployments/config/persistence.xml` is OK, whereas `/deployments/persistence.xml` will cause an infinity loop.
[Quarkus Configuration Reference]: https://quarkus.io/guides/config-reference
[EclipseLink configuration file]: https://eclipse.dev/eclipselink/documentation/2.5/solutions/testingjpa002.htm

Polaris creates and connects to a separate database for each realm. Specifically, the `{realm}` placeholder in `jakarta.persistence.jdbc.url` is substituted with the actual realm name, allowing the Polaris server to connect to different databases based on the realm.

> Note: some database systems such as Postgres don't create databases automatically. Database admins need to create them manually before running Polaris server.

A single `persistence.xml` can describe multiple [persistence units](https://eclipse.dev/eclipselink/documentation/2.6/concepts/app_dev001.htm). For example, with both a `polaris-dev` and `polaris` persistence unit defined, you could use a single `persistence.xml` to easily switch between development and production databases. Use the `persistence-unit` option in the Polaris server configuration to easily switch between persistence units.

### Using H2

> [!IMPORTANT] H2 is an in-memory database and is not suitable for production!

The default [persistence.xml] in Polaris is already configured for H2, but you can easily customize
your H2 configuration using the persistence unit template below:

[persistence.xml]: https://github.com/apache/polaris/blob/main/extension/persistence/eclipselink/src/main/resources/META-INF/persistence.xml

```xml
<persistence-unit name="polaris" transaction-type="RESOURCE_LOCAL">
    <provider>org.eclipse.persistence.jpa.PersistenceProvider</provider>
    <class>org.apache.polaris.jpa.models.ModelEntity</class>
    <class>org.apache.polaris.jpa.models.ModelEntityActive</class>
    <class>org.apache.polaris.jpa.models.ModelEntityChangeTracking</class>
    <class>org.apache.polaris.jpa.models.ModelEntityDropped</class>
    <class>org.apache.polaris.jpa.models.ModelGrantRecord</class>
    <class>org.apache.polaris.jpa.models.ModelPrincipalSecrets</class>
    <class>org.apache.polaris.jpa.models.ModelSequenceId</class>
    <shared-cache-mode>NONE</shared-cache-mode>
    <properties>
      <property name="jakarta.persistence.jdbc.url"
        value="jdbc:h2:file:tmp/polaris_test/filedb_{realm}"/>
      <property name="jakarta.persistence.jdbc.user" value="sa"/>
      <property name="jakarta.persistence.jdbc.password" value=""/>
      <property name="jakarta.persistence.schema-generation.database.action" value="create"/>
    </properties>
</persistence-unit>
```

To build Polaris with the necessary H2 dependency and start the Polaris service, run the following:

```shell
./gradlew clean :polaris-quarkus-server:assemble -PeclipseLinkDeps=com.h2database:h2:2.3.232
java -Dpolaris.persistence.type=eclipse-link \
     -Dpolaris.persistence.eclipselink.configuration-file=/path/to/persistence.xml \
     -Dpolaris.persistence.eclipselink.persistence-unit=polaris \
     -jar quarkus/server/build/quarkus-app/quarkus-run.jar
```

### Using Postgres

The following shows a sample configuration for integrating Polaris with Postgres.

```xml
<persistence-unit name="polaris" transaction-type="RESOURCE_LOCAL">
  <provider>org.eclipse.persistence.jpa.PersistenceProvider</provider>
  <class>org.apache.polaris.jpa.models.ModelEntity</class>
  <class>org.apache.polaris.jpa.models.ModelEntityActive</class>
  <class>org.apache.polaris.jpa.models.ModelEntityChangeTracking</class>
  <class>org.apache.polaris.jpa.models.ModelEntityDropped</class>
  <class>org.apache.polaris.jpa.models.ModelGrantRecord</class>
  <class>org.apache.polaris.jpa.models.ModelPrincipalSecrets</class>
  <class>org.apache.polaris.jpa.models.ModelSequenceId</class>
  <shared-cache-mode>NONE</shared-cache-mode>
  <properties>
    <property name="jakarta.persistence.jdbc.url"
              value="jdbc:postgresql://localhost:5432/{realm}"/>
    <property name="jakarta.persistence.jdbc.user" value="postgres"/>
    <property name="jakarta.persistence.jdbc.password" value="postgres"/>
    <property name="jakarta.persistence.schema-generation.database.action" value="create"/>
    <property name="eclipselink.persistence-context.flush-mode" value="auto"/>
  </properties>
</persistence-unit>
```

To build Polaris with the necessary Postgres dependency and start the Polaris service, run the following:

```shell
./gradlew clean :polaris-quarkus-server:assemble -PeclipseLinkDeps=org.postgresql:postgresql:42.7.4
java -Dpolaris.persistence.type=eclipse-link \
     -Dpolaris.persistence.eclipselink.configuration-file=/path/to/persistence.xml \
     -Dpolaris.persistence.eclipselink.persistence-unit=polaris \
     -jar quarkus/server/build/quarkus-app/quarkus-run.jar
```
