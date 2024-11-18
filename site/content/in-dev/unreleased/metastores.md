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
Configure the `metaStoreManager` section in the Polaris configuration (`polaris-server.yml` by default) as follows:
```
metaStoreManager:
  type: eclipse-link
  conf-file: META-INF/persistence.xml
  persistence-unit: polaris
```

`conf-file` must point to an [EclipseLink configuration file](https://eclipse.dev/eclipselink/documentation/2.5/solutions/testingjpa002.htm)

By default, `conf-file` points to the embedded resource file `META-INF/persistence.xml` in the `polaris-eclipselink` module.

In order to specify a configuration file outside the classpath, follow these steps.
1) Place `persistence.xml` into a jar file: `jar cvf /tmp/conf.jar persistence.xml`
2) Use `conf-file: /tmp/conf.jar!/persistence.xml`

## EclipseLink Configuration - persistence.xml
The configuration file `persistence.xml` is used to set up the database connection properties, which can differ depending on the type of database and its configuration.

Check out the default [persistence.xml](https://github.com/apache/polaris/blob/main/extension/persistence/eclipselink/src/main/resources/META-INF/persistence.xml) for a complete sample for connecting to the file-based H2 database. 

Polaris creates and connects to a separate database for each realm. Specifically, the `{realm}` placeholder in `jakarta.persistence.jdbc.url` is substituted with the actual realm name, allowing the Polaris server to connect to different databases based on the realm.

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
      <property name="eclipselink.ddl-generation" value="create-or-extend-tables"/>
    </properties>
</persistence-unit>
```

A single `persistence.xml` can describe multiple [persistence units](https://eclipse.dev/eclipselink/documentation/2.6/concepts/app_dev001.htm). For example, with both a `polaris-dev` and `polaris` persistence unit defined, you could use a single `persistence.xml` to easily switch between development and production databases. Use `persistence-unit` in the Polaris server configuration to easily switch between persistence units.

To build Polaris with the necessary H2 dependency and start the Polaris service, run the following:
```bash
polaris> ./gradlew --no-daemon --info -PeclipseLink=true -PeclipseLinkDeps=com.h2database:h2:2.3.232 clean shadowJar
polaris> java -jar  polaris-service/build/libs/polaris-service-*.jar server ./polaris-server.yml
```

### Postgres

The following shows a sample configuration for integrating Polaris with Postgres.

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
    <property name="eclipselink.persistence-context.flush-mode" value="auto"/>
    <property name="eclipselink.ddl-generation" value="create-or-extend-tables"/>
  </properties>
</persistence-unit>
```

To build Polaris with the necessary Postgres dependency and start the Polaris service, run the following:
```bash
polaris> ./gradlew --no-daemon --info -PeclipseLink=true -PeclipseLinkDeps=org.postgresql:postgresql:42.7.4 clean shadowJar
polaris> java -jar  polaris-service/build/libs/polaris-service-*.jar server ./polaris-server.yml
```