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
import java.util.Map;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

public class PostgresRelationalJdbcLifeCycleManagement
    implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {
  private static final String INIT_SCRIPT = "init-script";

  private PostgreSQLContainer<?> postgres;
  private String initScript;
  private DevServicesContext context;

  @Override
  public void init(Map<String, String> initArgs) {
    initScript = initArgs.get(INIT_SCRIPT);
  }

  @Override
  @SuppressWarnings("resource")
  public Map<String, String> start() {
    postgres =
        new PostgreSQLContainer<>(DockerImageName.parse("postgres:17"))
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
            "relational-jdbc",
            "quarkus.datasource.\"postgresql1\".active", "true",
            "quarkus.datasource.\"postgresql1\".db-kind", "pgsql",
            "quarkus.datasource.\"postgresql1\".jdbc.url",
            postgres.getJdbcUrl(),
            "quarkus.datasource.\"postgresql1\".username",
            "polaris",
            "quarkus.datasource.\"postgresql1\".password",
            "polaris"
//            "quarkus.datasource.\"postgresql2\".active", "true",
//            "quarkus.datasource.\"postgresql2\".jdbc.url",
//            "jdbc:postgresql://localhost:50689/polaris_realm1?loggerLevel=OFF",
//            "quarkus.datasource.\"postgresql2\".username",
//            "polaris",
//            "quarkus.datasource.\"postgresql2\".password",
//            "polaris"
    );
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
