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
title: Polaris Extensions
type: docs
weight: 1006
---

Apache Polaris is built on [Quarkus](https://quarkus.io) and [CDI](https://quarkus.io/guides/cdi-reference).
Most of its pluggable parts (authorizers, event listeners, federated catalog factories, metastore
implementations, ...) are CDI beans selected at runtime by an `@Identifier` and a configuration property.
You can therefore add your own functionality by writing a **module**, which is a plain jar containing
CDI beans, and packaging it together with the standard Polaris server.

This page explains:

1. [How extensions are discovered](#how-extensions-are-discovered)
2. [How to write an extension module](#step-1-write-an-extension-module)
3. [How to build your own Polaris server distribution](#step-2-build-a-custom-polaris-server-distribution)
   that starts from the standard Polaris server, adds your module, and repackages everything.

{{< alert note >}}
This page complements [Building a Custom Server Based on Polaris]({{% ref "downstream-build" %}}), which
describes the general principles. Read it first if you have not already.
{{< /alert >}}

## How extensions are discovered

Quarkus resolves CDI beans at **build time**, not at startup. During the Quarkus build ("augmentation"),
Quarkus indexes the application classes and the dependencies that it knows to contain beans. Two
consequences follow:

* **A module must be indexed to be seen.** A dependency jar is scanned for beans only if it carries a
  [Jandex](https://github.com/smallrye/jandex) index (`META-INF/jandex.idx`) or an (empty)
  `META-INF/beans.xml`, or if it is explicitly declared with `quarkus.index-dependency.*` properties.
  Polaris modules use the Jandex plugin for this; so should yours.
* **You cannot just drop a jar into an existing distribution.** The `lib/` directory of the binary
  distribution is the *output* of the Quarkus build, and the bean wiring has already been generated.
  A jar copied there afterwards is on the classpath but its beans are never registered. To add a module you
  must run the Quarkus build again with your module as a dependency. That is what "repackaging" means here.

The standard Polaris server is itself assembled this way: the `polaris-server` Gradle module depends on
`polaris-runtime-service` plus a set of extension modules (e.g. `polaris-extensions-auth-opa`,
`polaris-extensions-events-kafka`) and runs the Quarkus build on them.

## Step 1: Write an extension module

This example creates an event listener that logs every Polaris event in a dedicated audit logger.
`PolarisEventListener` is an existing Polaris extension point, implemented for instance by the Kafka
extension (`polaris-extensions-events-kafka`).

### Project layout

```
polaris-audit-extension/
├── pom.xml
└── src/main/java/com/example/polaris/audit/AuditLogEventListener.java
```

### The CDI bean

```java
package com.example.polaris.audit;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
@Identifier("audit-log")
public class AuditLogEventListener implements PolarisEventListener {
  private static final Logger AUDIT = LoggerFactory.getLogger("polaris.audit");

  @Override
  public void onEvent(PolarisEvent event) {
    AUDIT.info("eventId={} type={}", event.metadata().eventId(), event.type());
  }
}
```

The `@Identifier` value is the name used to select the bean in the Polaris configuration. See the
[Configuration]({{% ref "configuration" %}}) pages for the properties that select an implementation
(for example `polaris.event-listener.type`).

### The `pom.xml` (Jandex)

Polaris dependencies are `provided` here: the server build supplies them, and your jar should not bundle them.
The `jandex-maven-plugin` generates the `META-INF/jandex.idx` file that lets Quarkus discover the bean.

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <groupId>com.example.polaris</groupId>
  <artifactId>polaris-audit-extension</artifactId>
  <version>1.0.0</version>

  <properties>
    <maven.compiler.release>21</maven.compiler.release>
    <polaris.version>1.9.0-SNAPSHOT</polaris.version> <!-- use the Polaris release you target -->
    <quarkus.platform.version>3.40.1</quarkus.platform.version> <!-- must match the Polaris release -->
    <jandex.maven.plugin.version>3.5.3</jandex.maven.plugin.version>
  </properties>

  <dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>org.apache.polaris</groupId>
        <artifactId>polaris-bom</artifactId>
        <version>${polaris.version}</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
      <dependency>
        <groupId>io.quarkus.platform</groupId>
        <artifactId>quarkus-bom</artifactId>
        <version>${quarkus.platform.version}</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
    </dependencies>
  </dependencyManagement>

  <dependencies>
    <dependency>
      <groupId>org.apache.polaris</groupId>
      <artifactId>polaris-runtime-service</artifactId>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>jakarta.enterprise</groupId>
      <artifactId>jakarta.enterprise.cdi-api</artifactId>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>io.smallrye.common</groupId>
      <artifactId>smallrye-common-annotation</artifactId>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.slf4j</groupId>
      <artifactId>slf4j-api</artifactId>
      <scope>provided</scope>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>io.smallrye</groupId>
        <artifactId>jandex-maven-plugin</artifactId>
        <version>${jandex.maven.plugin.version}</version>
        <executions>
          <execution>
            <id>make-index</id>
            <goals>
              <goal>jandex</goal>
            </goals>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```

Build and install it:

```bash
mvn clean install
unzip -l target/polaris-audit-extension-1.0.0.jar | grep jandex
# META-INF/jandex.idx
```

{{< alert tip >}}
If you cannot add the Jandex plugin to a module (for example a third-party jar), declare it in the
`application.properties` of the distribution project instead:

```properties
quarkus.index-dependency.audit.group-id=com.example.polaris
quarkus.index-dependency.audit.artifact-id=polaris-audit-extension
```
{{< /alert >}}

If your module is built with Gradle, the equivalent is the `org.kordamp.gradle.jandex` plugin, which is the one
Polaris uses for its own modules.

## Step 2: Build a custom Polaris server distribution

The goal is a server equivalent to the standard Polaris one, plus your module. The standard server is defined
by the `runtime/server/build.gradle.kts` file of the Polaris sources: reading it tells you which dependencies it
includes. Reproduce the ones you need in a new Maven project and add your module.

### Project layout

```
my-polaris-server/
├── pom.xml
└── src/main/resources/application.properties
```

### The `pom.xml` (Quarkus)

The `quarkus-maven-plugin` runs the augmentation step and produces the runnable `target/quarkus-app` directory,
using the same *fast-jar* format as the Polaris distribution.

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <groupId>com.example.polaris</groupId>
  <artifactId>my-polaris-server</artifactId>
  <version>1.0.0</version>

  <properties>
    <maven.compiler.release>21</maven.compiler.release>
    <polaris.version>1.9.0-SNAPSHOT</polaris.version>
    <quarkus.platform.version>3.40.1</quarkus.platform.version>
  </properties>

  <dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>org.apache.polaris</groupId>
        <artifactId>polaris-bom</artifactId>
        <version>${polaris.version}</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
      <dependency>
        <groupId>io.quarkus.platform</groupId>
        <artifactId>quarkus-bom</artifactId>
        <version>${quarkus.platform.version}</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
    </dependencies>
  </dependencyManagement>

  <dependencies>
    <!-- Standard Polaris server: REST endpoints and services -->
    <dependency>
      <groupId>org.apache.polaris</groupId>
      <artifactId>polaris-runtime-service</artifactId>
    </dependency>

    <!-- Metastore: relational JDBC with PostgreSQL, as in the standard distribution -->
    <dependency>
      <groupId>org.apache.polaris</groupId>
      <artifactId>polaris-relational-jdbc</artifactId>
    </dependency>
    <dependency>
      <groupId>io.quarkus</groupId>
      <artifactId>quarkus-jdbc-postgresql</artifactId>
    </dependency>

    <!-- Standard extensions you want to keep (pick the ones you need) -->
    <dependency>
      <groupId>org.apache.polaris</groupId>
      <artifactId>polaris-extensions-auth-opa</artifactId>
    </dependency>

    <!-- Your custom module from step 1 -->
    <dependency>
      <groupId>com.example.polaris</groupId>
      <artifactId>polaris-audit-extension</artifactId>
      <version>1.0.0</version>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>io.quarkus.platform</groupId>
        <artifactId>quarkus-maven-plugin</artifactId>
        <version>${quarkus.platform.version}</version>
        <extensions>true</extensions>
        <executions>
          <execution>
            <goals>
              <goal>build</goal>
            </goals>
          </execution>
        </executions>
        <configuration>
          <properties>
            <quarkus.package.jar.type>fast-jar</quarkus.package.jar.type>
          </properties>
        </configuration>
      </plugin>
    </plugins>
  </build>
</project>
```

{{< alert important >}}
Do **not** depend on `polaris-server` or `polaris-runtime-defaults`: as explained in
[Building a Custom Server Based on Polaris]({{% ref "downstream-build" %}}), your project owns its
`application.properties`. Do not name your application "Apache Polaris".
{{< /alert >}}

{{< alert note >}}
The Polaris Gradle build pins some transitive dependencies (e.g. Jackson annotations) on top of the Quarkus
platform. When you build with Maven, keep `quarkus.platform.version` identical to the version used by your Polaris
release (`quarkus` in `gradle/libs.versions.toml`), and check `mvn dependency:tree` for conflicts.
{{< /alert >}}

### The `application.properties`

Start from the `application.properties` of the Polaris release you target
(`runtime/defaults/src/main/resources/application.properties`), copy it to
`src/main/resources/application.properties` and adjust it. Then enable your module:

```properties
quarkus.application.name=my-polaris-server
quarkus.application.version=${project.version}

# Select the bean identified by @Identifier("audit-log")
polaris.event-listener.type=audit-log

# Optional: route the audit logger to its own category/level
quarkus.log.category."polaris.audit".level=INFO
```

### Build and run

```bash
mvn clean package

# Run the repackaged server (same launcher as the standard distribution)
java -Dpolaris.bootstrap.credentials=POLARIS,root,s3cr3t \
     -jar target/quarkus-app/quarkus-run.jar
```

Because `polaris-runtime-service` is run with the same Quarkus augmentation as the standard server, your
`AuditLogEventListener` is registered and every Polaris event is logged under `polaris.audit`.
Check the startup log, and trigger an event (e.g. create a catalog) to see the output.

### Repackage as a binary distribution

The standard Polaris binary distribution ships `server/` (the Quarkus `quarkus-app` content) and a
`bin/server` launcher script that does `cd ../server && java -jar quarkus-run.jar`. You can replicate this with
the `maven-assembly-plugin`.

Create `src/assembly/dist.xml`:

```xml
<assembly xmlns="http://maven.apache.org/ASSEMBLY/2.2.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/ASSEMBLY/2.2.0 http://maven.apache.org/xsd/assembly-2.2.0.xsd">
  <id>bin</id>
  <formats>
    <format>tar.gz</format>
    <format>zip</format>
  </formats>
  <includeBaseDirectory>true</includeBaseDirectory>
  <fileSets>
    <fileSet>
      <directory>${project.build.directory}/quarkus-app</directory>
      <outputDirectory>server</outputDirectory>
    </fileSet>
    <fileSet>
      <directory>src/main/dist/bin</directory>
      <outputDirectory>bin</outputDirectory>
      <fileMode>0755</fileMode>
    </fileSet>
  </fileSets>
</assembly>
```

Create `src/main/dist/bin/server` (adapted from `runtime/distribution/bin/server` in the Polaris sources):

```bash
#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/../server"
JAVA_CMD="${JAVA_HOME:+${JAVA_HOME%/}/bin/java}"
JAVA_CMD="${JAVA_CMD:-$(command -v java)}"
exec "$JAVA_CMD" ${POLARIS_JAVA_OPTS:-} -jar quarkus-run.jar
```

Add the plugin to the `<build><plugins>` of your `pom.xml`. It must run after the Quarkus build, so
bind it to the `package` phase after `quarkus-maven-plugin` (plugins run in declaration order):

```xml
<plugin>
  <artifactId>maven-assembly-plugin</artifactId>
  <version>3.7.1</version>
  <configuration>
    <descriptors>
      <descriptor>src/assembly/dist.xml</descriptor>
    </descriptors>
    <finalName>my-polaris-server-${project.version}</finalName>
    <appendAssemblyId>false</appendAssemblyId>
  </configuration>
  <executions>
    <execution>
      <id>make-distribution</id>
      <phase>package</phase>
      <goals>
        <goal>single</goal>
      </goals>
    </execution>
  </executions>
</plugin>
```

`mvn clean package` now produces `target/my-polaris-server-1.0.0.tar.gz` and `.zip`:

```
my-polaris-server-1.0.0/
├── bin/server
└── server/
    ├── quarkus-run.jar
    ├── app/
    ├── lib/
    └── quarkus/
```

Start it with `bin/server`.

### Repackage as a container image

The Polaris image is built from `runtime/server/src/main/docker/Dockerfile.jvm`, which copies the
`quarkus-app` directory into `/deployments`. You can do the same with your own Dockerfile:

```dockerfile
FROM registry.access.redhat.com/ubi9/openjdk-21-runtime
ENV JAVA_APP_JAR="/deployments/quarkus-run.jar"
COPY target/quarkus-app/lib/ /deployments/lib/
COPY target/quarkus-app/*.jar /deployments/
COPY target/quarkus-app/app/ /deployments/app/
COPY target/quarkus-app/quarkus/ /deployments/quarkus/
```

```bash
docker build -t example/my-polaris-server:1.0.0 .
```

Alternatively, add the `io.quarkus:quarkus-container-image-docker` dependency (as the Polaris build does) and run
`mvn package -Dquarkus.container-image.build=true`.

## Custom entities

Event listeners, authorizers and factories are plug-in points: a module adds a bean and a property selects it.
**Entity types are not.** `PolarisEntityType`, `PolarisPrivilege` and `PolarisAuthorizableOperation` are closed
enums in `polaris-core`, and the metastore implementations map entity types explicitly. A module in your own jar
therefore cannot register a brand-new entity type on top of a stock Polaris release.

You have two options.

### Option A: model your concept on an existing entity type (no core change)

If your concept can live inside an existing entity, prefer this. For instance, a
[generic table]({{% ref "generic-table" %}}) has a free-form `format` and free-form `properties`, is
namespaced, is persisted by every metastore, and is already covered by RBAC (`TABLE_*` privileges).
Pick a dedicated format name (e.g. `com.example.data-contract`), and keep your fields in `properties`. Clients use the
standard generic table API. Your extension module can add validation, for example by reacting to events in a
`PolarisEventListener`, or its own REST resource on top.

### Option B: add a first-class entity type (patch `polaris-core`)

When you need your own type, privileges and REST API, you carry a small patch against Polaris and ship it in
your own distribution. The Apache Ossie **semantic model** is the reference implementation: it was added exactly
this way, with the entity in `polaris-core` and the REST API and logic in `extensions/semantic-models`.
Use it as a blueprint (the example below is a hypothetical `DATA_CONTRACT` entity living in a namespace).

**1. Define the type in `polaris-core`** (`PolarisEntityType`). The constructor arguments are
`(code, parentType, isGrantee, parentSelfReference)`. The `code` is persisted, so it must never change or be reused.
Choose a value that upstream Polaris is unlikely to allocate, and keep in mind that the reverse-lookup array is sized
by the largest code, so do not use huge values.

```java
SEMANTIC_MODEL(11, NAMESPACE, false, false),
DATA_CONTRACT(12, NAMESPACE, false, false);
```

**2. Add the entity class** (`org.apache.polaris.core.entity`), following `SemanticModelEntity`: a subclass of
`PolarisEntity` that checks the type in its constructor and exposes a `Builder`. Free-form user data goes in
`properties`, internal data (such as the parent namespace) in `internalProperties`.

```java
public class DataContractEntity extends PolarisEntity {
  public static final String CONTENT_KEY = "data-contract.content";

  public DataContractEntity(PolarisBaseEntity sourceEntity) {
    super(sourceEntity);
    Preconditions.checkState(
        getType() == PolarisEntityType.DATA_CONTRACT, "Invalid entity type: %s", getType());
  }

  public static @Nullable DataContractEntity of(@Nullable PolarisBaseEntity sourceEntity) {
    return sourceEntity == null ? null : new DataContractEntity(sourceEntity);
  }

  public static class Builder extends PolarisEntity.BaseBuilder<DataContractEntity, Builder> {
    public Builder(Namespace namespace, String name) {
      super();
      setType(PolarisEntityType.DATA_CONTRACT);
      setName(name);
      internalProperties.put(
          NamespaceEntity.PARENT_NAMESPACE_KEY, PolarisEntityUtils.encodeNamespace(namespace));
    }

    public Builder setContent(String content) {
      properties.put(CONTENT_KEY, content);
      return this;
    }

    @Override
    public DataContractEntity build() {
      Preconditions.checkArgument(properties.containsKey(CONTENT_KEY), "Content is required");
      return new DataContractEntity(buildBase());
    }
  }
}
```

**3. Wire the type into the rest of the core.** Search for `SEMANTIC_MODEL` in the sources: every hit is a place
where a new type must be handled. At the time of writing these are:

| Concern                | Where                                                                                                                      |
|------------------------|----------------------------------------------------------------------------------------------------------------------------|
| Privileges             | `PolarisPrivilege`: `DATA_CONTRACT_LIST`, `_CREATE` (on `NAMESPACE`), `_READ`, `_WRITE`, `_DROP` (on `DATA_CONTRACT`)     |
| Authorization          | `PolarisAuthorizableOperation`, `RbacOperationSemantics`: map each operation to its privileges                             |
| Grants management      | `PolarisAdminService`, `PolarisSecurableMapper`: allow granting the new privileges on the new securable                    |
| Path resolution        | `ResolvedPathKey`: resolve the new entity inside its namespace                                                             |
| REST paths             | `PolarisResourcePaths`                                                                                                     |
| NoSQL metastore        | `EntityObjMappings`, `MutationAttempt`, `ChangeCommitterWrapper` (`persistence/nosql`)                                     |
| Events persistence     | `PolarisPersistenceEventListener`                                                                                          |

The relational JDBC metastore stores entities generically (`type_code` plus JSON columns), so it needs no schema
change for a new type.

**4. Add the logic in an extension module.** This part is a regular module and is packaged as in
[Step 1](#step-1-write-an-extension-module). Create entities through the `PolarisMetaStoreManager` after
resolving the parent namespace, as `SemanticModelCatalog.createSemanticModel` does:

```java
DataContractEntity entity =
    new DataContractEntity.Builder(namespace, name)
        .setCatalogId(catalogId)
        .setParentId(resolvedParent.getRawLeafEntity().getId())
        .setContent(content)
        .setId(metaStoreManager.generateNewEntityId(callContext.getPolarisCallContext()).getId())
        .setCreateTimestamp(System.currentTimeMillis())
        .build();

EntityResult result =
    metaStoreManager.createEntityIfNotExists(
        callContext.getPolarisCallContext(),
        PolarisEntity.toCoreList(resolvedParent.getRawFullPath()),
        entity);
```

Expose it with a `@RequestScoped` JAX-RS adapter that checks authorization through `PolarisAuthorizer` in a handler
(see `SemanticModelCatalogAdapter` and `SemanticModelCatalogHandler`), and gate it behind a feature flag so it
can be enabled per realm.

**5. Build and ship your fork.** Build Polaris from your patched sources and publish its artifacts under your own
version to your Maven repository (or local repository). Set `polaris.version` in the distribution project of
[Step 2](#step-2-build-a-custom-polaris-server-distribution) to that version and add your extension module as a
dependency. Quarkus then packages your patched core, the standard modules and your module together.

{{< alert warning >}}
Option B forks a part of Polaris that is neither a public API nor covered by compatibility guarantees (see
[Polaris Evolution]({{% ref "evolution" %}})). You will need to rebase your patch on each upgrade, and the persisted
entity `code` must stay stable forever. If the entity is of general interest, consider proposing it upstream
on the dev mailing list, as was done for semantic models, instead of maintaining a fork.
{{< /alert >}}

## Troubleshooting

| Symptom                                                                            | Likely cause                                                                                                              |
|------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------|
| `Unsatisfied dependency` / no bean found for `polaris.event-listener.type` value   | The module is not indexed: check for `META-INF/jandex.idx` in the jar, or add `quarkus.index-dependency.*` properties.     |
| Bean is found at build time but ignored at runtime                                 | The `@Identifier` value does not match the configured property value.                                                      |
| The module works in `mvn package` but not after copying the jar in `server/lib`    | Beans are resolved at build time: re-run the Quarkus build with the module as a dependency.                                |
| `NoSuchMethodError` / `ClassNotFoundException` at startup                          | Quarkus or Polaris version mismatch between your module and the server. Align `polaris.version` and the Quarkus version.   |

## Compatibility

Extension points such as `PolarisEventListener` are internal Java interfaces, not a versioned public API: see
[Polaris Evolution]({{% ref "evolution" %}}) for what can be expected between releases. Rebuild and retest your
modules and your distribution on each Polaris upgrade.
