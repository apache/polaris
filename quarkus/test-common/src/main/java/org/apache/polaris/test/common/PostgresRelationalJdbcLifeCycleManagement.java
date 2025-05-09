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
package org.apache.polaris.test.common;

import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

public class PostgresRelationalJdbcLifeCycleManagement
    implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {
  public static final String INIT_SCRIPT = "init-script";
  public static final String DATABASES = "databases";

  private PostgreSQLContainer<?> postgres;
  private String initScript;
  private List<String> databases;
  private DevServicesContext context;

  @Override
  public void init(Map<String, String> initArgs) {
    initScript = initArgs.get(INIT_SCRIPT);
    String databases = initArgs.get(DATABASES);
    this.databases = databases == null ? new ArrayList<>() : Arrays.asList(databases.split(","));
  }

  @Override
  @SuppressWarnings("resource")
  public Map<String, String> start() {
    postgres =
        new PostgreSQLContainer<>(DockerImageName.parse("postgres:17-alpine"))
            .withDatabaseName("realm1")
            .withUsername("polaris")
            .withPassword("polaris");

    if (initScript != null) {
      postgres.withInitScript(initScript);
    }

    context.containerNetworkId().ifPresent(postgres::withNetworkMode);
    postgres.start();

    if (databases.isEmpty()) {
      Map<String, String> props = generateDataSourceProps(List.of("realm1"));
      // make realm1_ds as the default ds
      props.put("polaris.relational.jdbc.datasource.default-datasource", "realm1_ds");
      return props;
    } else {
      Map<String, String> allProps = new HashMap<>();
      allProps.putAll(generateDataSourceMappingProps(databases));
      allProps.putAll(generateDataSourceProps(databases));
      return allProps;
    }
  }

  private Map<String, String> generateDataSourceProps(List<String> dataSources) {
    Map<String, String> props = new HashMap<>();
    props.put("polaris.persistence.type", "relational-jdbc");
    for (String database : dataSources) {
      props.put(String.format("quarkus.datasource.%s.db-kind", database + "_ds"), "pgsql");
      props.put(String.format("quarkus.datasource.%s.active", database + "_ds"), "true");
      props.put(
              String.format("quarkus.datasource.%s.jdbc.url", database + "_ds"),
              postgres.getJdbcUrl().replace("realm1", database));
      props.put(
              String.format("quarkus.datasource.%s.username", database + "_ds"),
              postgres.getUsername());
      props.put(
              String.format("quarkus.datasource.%s.password", database + "_ds"),
              postgres.getPassword());
    }
    return props;
  }

  private Map<String, String> generateDataSourceMappingProps(List<String> realms) {
    Map<String, String> props = new HashMap<>();
    // polaris.relation.jdbc.datasource.realm=realm_ds
    for (String database : realms) {
      props.put(String.format("polaris.relational.jdbc.datasource.%s", database), database + "_ds");
    }
    return props;
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
  }

  @Override
  public void setIntegrationTestContext(DevServicesContext context) {
    this.context = context;
  }
}
