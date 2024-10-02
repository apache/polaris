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

This page documents the general configurations to connect to production database through [EclipseLink](https://eclipse.dev/eclipselink/).

## Polaris Server Configuration
Configure the `metaStoreManager` section in server configuration `polaris-server.yml` as follows:
```
metaStoreManager:
  type: eclipse-link
  conf-file: META-INF/persistence.xml
  persistence-unit: polaris
```
`conf-file` points to the EclipseLink configuration file and `persistence-unit` tells which unit from the configuration file to use for database connection.

E.g., `polaris-dev` and `polaris` persistence units are configured in `persistence.xml` to connect to development database and production database respectively. Updating `persistence-unit` from `polaris-dev` to `polaris` switch from the development to the production.

`conf-file` by default points to the embedded resource file `META-INF/persistence.xml` in `polaris-eclipselink` module.
To specify an external configuration file,
1) Place `persistence.xml` into a jar file: `jar cvf /tmp/conf.jar persistence.xml`.
2) Use `conf-file: /tmp/conf.jar!/persistence.xml`.

## EclipseLink Configuration - persistence.xml
The configuration file `persistence.xml` is used to set up the database connection properties, which can differ depending on the type of database and its configuration.

Check out [default persistence.xml](https://github.com/apache/polaris/blob/main/extension/persistence/eclipselink/src/main/resources/META-INF/persistence.xml) for the complete sample and the following shows the database connection properties against file-based H2 database. Polaris creates and connects to a separate database for each realm. Specifically, `{realm}` placeholder in `jakarta.persistence.jdbc.url` is substituted with the actual realm name, allowing the Polaris server to connect to different databases based on the realm.

> Note: some database systems such as Postgres don't create databases automatically. Database admins need to create them manually before running Polaris server.
```xml
<persistence-unit name="polaris" transaction-type="RESOURCE_LOCAL">
    <provider>org.eclipse.persistence.jpa.PersistenceProvider</provider>
    <class>org.apache.polaris.core.persistence.models.ModelEntity</class>
    <class>org.apache.polaris.core.persistence.models.ModelEntityActive</class>
    <class>org.apache.polaris.core.persistence.models.ModelEntityChangeTracking</class>
    <class>org.apache.polaris.core.persistence.models.ModelEntityDropped</class>
    <class>org.apache.polaris.core.persistence.models.ModelGrantRecord</class>
    <class>org.apache.polaris.core.persistence.models.ModelPrincipalSecrets</class>
    <class>org.apache.polaris.core.persistence.models.ModelSequenceId</class>
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

Build with H2 dependency and run Polaris service:
```bash
polaris> ./gradlew --no-daemon --info -PeclipseLink=true -PeclipseLinkDeps=com.h2database:h2:2.3.232 clean shadowJar
polaris> java -jar  polaris-service/build/libs/polaris-service-*.jar server ./polaris-server.yml
```

### Postgres

The following shows a sample configuration for Postgres database.

```xml
<persistence-unit name="polaris" transaction-type="RESOURCE_LOCAL">
  <provider>org.eclipse.persistence.jpa.PersistenceProvider</provider>
  <class>org.apache.polaris.core.persistence.models.ModelEntity</class>
  <class>org.apache.polaris.core.persistence.models.ModelEntityActive</class>
  <class>org.apache.polaris.core.persistence.models.ModelEntityChangeTracking</class>
  <class>org.apache.polaris.core.persistence.models.ModelEntityDropped</class>
  <class>org.apache.polaris.core.persistence.models.ModelGrantRecord</class>
  <class>org.apache.polaris.core.persistence.models.ModelPrincipalSecrets</class>
  <class>org.apache.polaris.core.persistence.models.ModelSequenceId</class>
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
Build with Postgres dependency and run Polaris service:
```bash
polaris> ./gradlew --no-daemon --info -PeclipseLink=true -PeclipseLinkDeps=org.postgresql:postgresql:42.7.4 clean shadowJar
polaris> java -jar  polaris-service/build/libs/polaris-service-*.jar server ./polaris-server.yml
```