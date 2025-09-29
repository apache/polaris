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
package org.apache.polaris.test.commons;

import static org.apache.polaris.containerspec.ContainerSpecHelper.containerSpecHelper;

import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Locale;
import java.util.Map;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;

public class RelationalJdbcLifeCycleManagement
    implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {

  public static final String INIT_SCRIPT = "init-script";
  public static final String DATABASE_TYPE = "database-type";

  public enum DatabaseType {
    POSTGRESQL("postgresql", "postgres", null),
    MYSQL("mysql", "mysql", null);

    private final String dbKind;
    private final String containerName;
    private final String dockerImage;

    DatabaseType(String dbKind, String containerName, String dockerImage) {
      this.dbKind = dbKind;
      this.containerName = containerName;
      this.dockerImage = dockerImage;
    }

    public String getDbKind() {
      return dbKind;
    }

    public String getContainerName() {
      return containerName;
    }

    public String getDockerImage() {
      return dockerImage;
    }
  }

  private JdbcDatabaseContainer<?> container;
  private String initScript;
  private DevServicesContext context;
  private DatabaseType databaseType;

  @Override
  public void init(Map<String, String> initArgs) {
    initScript = initArgs.get(INIT_SCRIPT);
    String dbTypeStr = initArgs.getOrDefault(DATABASE_TYPE, "POSTGRESQL");
    databaseType = DatabaseType.valueOf(dbTypeStr.toUpperCase(Locale.ROOT));
  }

  @Override
  @SuppressWarnings("resource")
  public Map<String, String> start() {
    container = createContainer(databaseType);

    if (initScript != null) {
      container.withInitScript(initScript);
    }

    context.containerNetworkId().ifPresent(container::withNetworkMode);
    container.start();

    return Map.of(
        "polaris.persistence.type", "relational-jdbc",
        "polaris.persistence.relational.jdbc.max-retries", "2",
        "quarkus.datasource.db-kind", databaseType.getDbKind(),
        "quarkus.datasource.jdbc.url", container.getJdbcUrl(),
        "quarkus.datasource.username", container.getUsername(),
        "quarkus.datasource.password", container.getPassword(),
        "quarkus.datasource.jdbc.initial-size", "10");
  }

  private JdbcDatabaseContainer<?> createContainer(DatabaseType dbType) {
    switch (dbType) {
      case POSTGRESQL:
        return new PostgreSQLContainer<>(
                containerSpecHelper(
                        dbType.getContainerName(), RelationalJdbcLifeCycleManagement.class)
                    .dockerImageName(dbType.getDockerImage())
                    .asCompatibleSubstituteFor(dbType.getContainerName()))
            .withDatabaseName("polaris_db")
            .withUsername("polaris")
            .withPassword("polaris");

      case MYSQL:
        return new MySQLContainer<>(
                containerSpecHelper(
                        dbType.getContainerName(), RelationalJdbcLifeCycleManagement.class)
                    .dockerImageName(dbType.getDockerImage())
                    .asCompatibleSubstituteFor(dbType.getContainerName()))
            .withDatabaseName("polaris_db")
            .withUsername("polaris")
            .withPassword("polaris");

      default:
        throw new IllegalArgumentException("Unsupported database type: " + dbType);
    }
  }

  @Override
  public void stop() {
    if (container != null) {
      try {
        container.stop();
      } finally {
        container = null;
      }
    }
  }

  @Override
  public void setIntegrationTestContext(DevServicesContext context) {
    this.context = context;
  }
}
