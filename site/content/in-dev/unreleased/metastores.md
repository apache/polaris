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

This page explains how to configure and use Polaris metastores with either the recommended Relational JDBC or the
deprecated EclipseLink persistence backends.

## Relational JDBC
This implementation leverages Quarkus for datasource management and supports configuration through
environment variables or JVM -D flags at startup. For more information, refer to the [Quarkus configuration reference](https://quarkus.io/guides/config-reference#env-file).


```
POLARIS_PERSISTENCE_TYPE=relational-jdbc

QUARKUS_DATASOURCE_USERNAME=<your-username>
QUARKUS_DATASOURCE_PASSWORD=<your-password>
QUARKUS_DATASOURCE_JDBC_URL=<jdbc-url-of-postgres>
```

The Relational JDBC metastore currently relies on a Quarkus-managed datasource and supports only PostgresSQL and H2 databases. This limitation is similar to that of EclipseLink, primarily due to underlying schema differences. At this time, official documentation is provided exclusively for usage with PostgreSQL.
Please refer to the documentation here:
[Configure data sources in Quarkus](https://quarkus.io/guides/datasource)

Additionally the retries can be configured via `polaris.persistence.relational.jdbc.*` properties please ref [configuration](./configuration.md)

## EclipseLink (Deprecated)
> [!IMPORTANT]
> Eclipse link is deprecated, its recommend to use Relational JDBC as persistence instead.

Polaris includes EclipseLink plugin by default with PostgresSQL driver.

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

> [!NOTE]
> You have to locate the `persistence.xml` at least two folders down to the root folder, e.g. `/deployments/config/persistence.xml` is OK, whereas `/deployments/persistence.xml` will cause an infinity loop.
[Quarkus Configuration Reference]: https://quarkus.io/guides/config-reference
[EclipseLink configuration file]: https://eclipse.dev/eclipselink/documentation/4.0/solutions/solutions.html#TESTINGJPA002

Polaris creates and connects to a separate database for each realm. Specifically, the `{realm}` placeholder in `jakarta.persistence.jdbc.url` is substituted with the actual realm name, allowing the Polaris server to connect to different databases based on the realm.

> [!NOTE]
> Some database systems such as Postgres don't create databases automatically. Database admins need to create them manually before running Polaris server.

A single `persistence.xml` can describe multiple [persistence units](https://eclipse.dev/eclipselink/documentation/4.0/concepts/concepts.html#APPDEV001). For example, with both a `polaris-dev` and `polaris` persistence unit defined, you could use a single `persistence.xml` to easily switch between development and production databases. Use the `persistence-unit` option in the Polaris server configuration to easily switch between persistence units.

### Using H2

> [!IMPORTANT]
> H2 is an in-memory database and is not suitable for production!

The default [persistence.xml] in Polaris is already configured for H2, but you can easily customize
your H2 configuration using the persistence unit template below:

[persistence.xml]: https://github.com/apache/polaris/blob/main/extension/persistence/eclipselink/src/main/resources/META-INF/persistence.xml

```xml
<persistence-unit name="polaris" transaction-type="RESOURCE_LOCAL">
    <provider>org.eclipse.persistence.jpa.PersistenceProvider</provider>
    <class>org.apache.polaris.extension.persistence.impl.eclipselink.models.ModelEntity</class>
    <class>org.apache.polaris.extension.persistence.impl.eclipselink.models.ModelEntityActive</class>
    <class>org.apache.polaris.extension.persistence.impl.eclipselink.models.ModelEntityChangeTracking</class>
    <class>org.apache.polaris.extension.persistence.impl.eclipselink.models.ModelEntityDropped</class>
    <class>org.apache.polaris.extension.persistence.impl.eclipselink.models.ModelGrantRecord</class>
    <class>org.apache.polaris.extension.persistence.impl.eclipselink.models.ModelPrincipalSecrets</class>
    <class>org.apache.polaris.extension.persistence.impl.eclipselink.models.ModelSequenceId</class>
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
./gradlew \
  :polaris-server:assemble \
  :polaris-server:quarkusAppPartsBuild --rerun \
  -PeclipseLinkDeps=com.h2database:h2:2.3.232
java -Dpolaris.persistence.type=eclipse-link \
     -Dpolaris.persistence.eclipselink.configuration-file=/path/to/persistence.xml \
     -Dpolaris.persistence.eclipselink.persistence-unit=polaris \
     -jar runtime/server/build/quarkus-app/quarkus-run.jar
```

### Using Postgres

PostgreSQL is included by default in the Polaris server distribution.

The following shows a sample configuration for integrating Polaris with Postgres.

```xml
<persistence-unit name="polaris" transaction-type="RESOURCE_LOCAL">
  <provider>org.eclipse.persistence.jpa.PersistenceProvider</provider>
  <class>org.apache.polaris.extension.persistence.impl.eclipselink.models.ModelEntity</class>
  <class>org.apache.polaris.extension.persistence.impl.eclipselink.models.ModelEntityActive</class>
  <class>org.apache.polaris.extension.persistence.impl.eclipselink.models.ModelEntityChangeTracking</class>
  <class>org.apache.polaris.extension.persistence.impl.eclipselink.models.ModelEntityDropped</class>
  <class>org.apache.polaris.extension.persistence.impl.eclipselink.models.ModelGrantRecord</class>
  <class>org.apache.polaris.extension.persistence.impl.eclipselink.models.ModelPrincipalSecrets</class>
  <class>org.apache.polaris.extension.persistence.impl.eclipselink.models.ModelSequenceId</class>
  <shared-cache-mode>NONE</shared-cache-mode>
  <properties>
    <property name="jakarta.persistence.jdbc.url"
              value="jdbc:postgresql://localhost:5432/{realm}"/>
    <property name="jakarta.persistence.jdbc.user" value="postgres"/>
    <property name="jakarta.persistence.jdbc.password" value="postgres"/>
    <property name="jakarta.persistence.schema-generation.database.action" value="create"/>
    <property name="eclipselink.persistence-context.flush-mode" value="auto"/>
    <property name="eclipselink.session.customizer" value="org.apache.polaris.extension.persistence.impl.eclipselink.PolarisEclipseLinkSessionCustomizer"/>
    <property name="eclipselink.transaction.join-existing" value="true"/>
  </properties>
</persistence-unit>
```
