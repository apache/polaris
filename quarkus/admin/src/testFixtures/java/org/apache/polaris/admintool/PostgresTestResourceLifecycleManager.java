/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.admintool;

import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

public class PostgresTestResourceLifecycleManager
    implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {

  public static final String INIT_SCRIPT = "init-script";

  private PostgreSQLContainer<?> postgres;
  private String initScript;
  private DevServicesContext context;
  private Path rootDir;

  @Override
  public void init(Map<String, String> initArgs) {
    initScript = initArgs.get(INIT_SCRIPT);
  }

  @Override
  public void setIntegrationTestContext(DevServicesContext context) {
    this.context = context;
  }

  @Override
  @SuppressWarnings("resource")
  public Map<String, String> start() {
    postgres =
        new PostgreSQLContainer<>(DockerImageName.parse("postgres:17-alpine"))
            .withDatabaseName("polaris_realm1")
            .withUsername("polaris")
            .withPassword("polaris");
    if (initScript != null) {
      postgres.withInitScript(initScript);
    }
    context.containerNetworkId().ifPresent(postgres::withNetworkMode);
    postgres.start();
    return Map.of(
        "polaris.persistence.type",
        "eclipse-link",
        "polaris.persistence.eclipselink.configuration-file",
        createPersistenceXml().toString());
  }

  @Override
  public void stop() {
    if (postgres != null) {
      try {
        postgres.stop();
      } finally {
        postgres = null;
      }
    }
    if (rootDir != null) {
      try {
        FileUtils.deleteDirectory(rootDir.toFile());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      } finally {
        rootDir = null;
      }
    }
  }

  private Path createPersistenceXml() {
    try {
      rootDir = Files.createTempDirectory("root");
      Path archiveDir = rootDir.resolve("archive");
      Files.createDirectory(archiveDir);
      String persistenceXmlContent =
          """
          <persistence version="2.0" xmlns="http://java.sun.com/xml/ns/persistence"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="http://java.sun.com/xml/ns/persistence http://java.sun.com/xml/ns/persistence/persistence_2_0.xsd">
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
                <property name="jakarta.persistence.jdbc.url" value="%s"/>
                <property name="jakarta.persistence.jdbc.user" value="%s"/>
                <property name="jakarta.persistence.jdbc.password" value="%s"/>
                <property name="jakarta.persistence.schema-generation.database.action" value="create"/>
                <property name="eclipselink.logging.level.sql" value="FINE"/>
                <property name="eclipselink.logging.parameters" value="true"/>
                <property name="eclipselink.persistence-context.flush-mode" value="auto"/>
              </properties>
            </persistence-unit>
          </persistence>
          """
              .formatted(
                  postgres.getJdbcUrl().replace("realm1", "{realm}"),
                  postgres.getUsername(),
                  postgres.getPassword());
      Path file = Files.createTempFile(archiveDir, "persistence", "xml");
      Files.writeString(file, persistenceXmlContent);
      return file;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
