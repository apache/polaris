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
import java.util.HashMap;
import java.util.Map;
import org.testcontainers.cockroachdb.CockroachContainer;

public class CockroachRelationalJdbcLifeCycleManagement
    implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {
  public static final String INIT_SCRIPT = "init-script";
  public static final String POLARIS_MANAGED_DATASOURCE = "polaris-managed-datasource";

  private CockroachContainer cockroach;
  private String initScript;
  private boolean polarisManagedDatasource;
  private DevServicesContext context;

  @Override
  public void init(Map<String, String> initArgs) {
    initScript = initArgs.get(INIT_SCRIPT);
    polarisManagedDatasource = Boolean.parseBoolean(initArgs.get(POLARIS_MANAGED_DATASOURCE));
  }

  @Override
  @SuppressWarnings("resource")
  public Map<String, String> start() {
    cockroach =
        new CockroachContainer(
            containerSpecHelper("cockroachdb", CockroachRelationalJdbcLifeCycleManagement.class)
                .dockerImageName(null)
                .asCompatibleSubstituteFor("cockroachdb/cockroach"));

    if (initScript != null) {
      cockroach.withInitScript(initScript);
    }

    context.containerNetworkId().ifPresent(cockroach::withNetworkMode);
    cockroach.start();

    // CockroachDB uses PostgreSQL JDBC driver and wire protocol
    // Explicitly configure database type as cockroachdb for proper identification
    Map<String, String> config = new HashMap<>();
    config.put("polaris.persistence.type", "relational-jdbc");
    config.put("polaris.persistence.relational.jdbc.database-type", "cockroachdb");
    config.put("polaris.persistence.relational.jdbc.max-retries", "2");

    if (polarisManagedDatasource) {
      config.put("quarkus.datasource.active", "false");
      config.put("polaris.persistence.relational.jdbc.jdbc-url", cockroach.getJdbcUrl());
      config.put("polaris.persistence.relational.jdbc.driver", "org.postgresql.Driver");
      config.put("polaris.persistence.relational.jdbc.username", cockroach.getUsername());
      config.put("polaris.persistence.relational.jdbc.password", cockroach.getPassword());
      config.put("polaris.persistence.relational.jdbc.maximum-pool-size", "10");
    } else {
      config.put("quarkus.datasource.db-kind", "postgresql");
      config.put("quarkus.datasource.jdbc.url", cockroach.getJdbcUrl());
      config.put("quarkus.datasource.username", cockroach.getUsername());
      config.put("quarkus.datasource.password", cockroach.getPassword());
      config.put("quarkus.datasource.jdbc.initial-size", "10");
    }

    return Map.copyOf(config);
  }

  @Override
  public void stop() {
    if (cockroach != null) {
      try {
        cockroach.stop();
      } finally {
        cockroach = null;
      }
    }
  }

  @Override
  public void setIntegrationTestContext(DevServicesContext context) {
    this.context = context;
  }
}
