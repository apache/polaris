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

import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;
import org.testcontainers.containers.CockroachContainer;

public class CockroachRelationalJdbcLifeCycleManagement
    implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {
  public static final String INIT_SCRIPT = "init-script";

  private CockroachContainer cockroach;
  private String initScript;
  private DevServicesContext context;

  @Override
  public void init(Map<String, String> initArgs) {
    initScript = initArgs.get(INIT_SCRIPT);
  }

  @Override
  @SuppressWarnings("resource")
  public Map<String, String> start() {
    // CockroachDB testcontainers uses PostgreSQL wire protocol compatibility
    cockroach = new CockroachContainer("cockroachdb/cockroach:v24.3.0");

    if (initScript != null) {
      cockroach.withInitScript(initScript);
    }

    context.containerNetworkId().ifPresent(cockroach::withNetworkMode);
    cockroach.start();

    // CockroachDB uses PostgreSQL JDBC driver and wire protocol
    return Map.of(
        "polaris.persistence.type",
        "relational-jdbc",
        "polaris.persistence.relational.jdbc.max-retries",
        "2",
        "quarkus.datasource.db-kind",
        "postgresql",
        "quarkus.datasource.jdbc.url",
        cockroach.getJdbcUrl(),
        "quarkus.datasource.username",
        cockroach.getUsername(),
        "quarkus.datasource.password",
        cockroach.getPassword(),
        "quarkus.datasource.jdbc.initial-size",
        "10");
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
