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
linkTitle: Metastores
type: docs
weight: 700
---

This page documents important configurations for connecting to production database through [EclipseLink](https://eclipse.dev/eclipselink/).

## Polaris Server Configuration

Configure the `polaris.persistence` section in the Polaris configuration (`application.properties`) as follows:

```
polaris.persistence.eclipselink.configuration-file=/path/to/persistence.xml
polaris.persistence.eclipselink.persistence-unit=polaris
```

`configuration-file` must point to an [EclipseLink configuration file](https://eclipse.dev/eclipselink/documentation/2.5/solutions/testingjpa002.htm).

## EclipseLink Configuration - persistence.xml

The configuration file `persistence.xml` is used to set up the database connection properties, which
can differ depending on the type of database and its configuration.

The path to the `persistence.xml` file and the persistence unit name must be set in the Polaris 
configuration:

```properties
polaris.persistence.eclipselink.configuration-file=/path/to/persistence.xml
polaris.persistence.eclipselink.persistence-unit=polaris
```

Check out the default [persistence.xml] for a complete sample below. An in-memory H2 database is
used by default, but you can easily switch to a different database.

[persistence.xml]: https://github.com/apache/polaris/blob/main/extension/persistence/eclipselink/src/main/resources/META-INF/persistence.xml

```xml
<persistence version="2.0" xmlns="http://java.sun.com/xml/ns/persistence"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://java.sun.com/xml/ns/persistence http://java.sun.com/xml/ns/persistence/persistence_2_0.xsd">

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
      <property name="jakarta.persistence.jdbc.url" value="jdbc:h2:mem:polaris_{realm}"/>
      <property name="jakarta.persistence.jdbc.user" value="sa"/>
      <property name="jakarta.persistence.jdbc.password" value=""/>
      <property name="jakarta.persistence.schema-generation.database.action" value="create"/>
      <property name="eclipselink.logging.level.sql" value="FINE"/>
      <property name="eclipselink.logging.parameters" value="true"/>
      <property name="eclipselink.persistence-context.flush-mode" value="auto"/>
    </properties>
  </persistence-unit>
</persistence>
```

Polaris creates and connects to a separate database for each realm. Specifically, the `{realm}` placeholder in `jakarta.persistence.jdbc.url` is substituted with the actual realm name, allowing the Polaris server to connect to different databases based on the realm.

> Note: some database systems such as Postgres don't create databases automatically. Database admins need to create them manually before running Polaris server.

A single `persistence.xml` can describe multiple [persistence units](https://eclipse.dev/eclipselink/documentation/2.6/concepts/app_dev001.htm). For example, with both a `polaris-dev` and `polaris` persistence unit defined, you could use a single `persistence.xml` to easily switch between development and production databases. Use `polaris.persistence.eclipselink.persistence-unit` in the Polaris server configuration to easily switch between persistence units.

### Postgres

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
