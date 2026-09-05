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
package org.apache.polaris.persistence.relational.jdbc.mysql.tests;

import static org.apache.polaris.containerspec.ContainerSpecHelper.containerSpecHelper;

import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;
import org.testcontainers.containers.MySQLContainer;

public class MysqlRelationalJdbcLifeCycleManagement
    implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {
  public static final String INIT_SCRIPT = "init-script";

  private MySQLContainer<?> mysql;
  private String initScript;
  private DevServicesContext context;

  @Override
  public void init(Map<String, String> initArgs) {
    initScript = initArgs.get(INIT_SCRIPT);
  }

  @Override
  @SuppressWarnings("resource")
  public Map<String, String> start() {
    mysql =
        new MySQLContainer<>(
                containerSpecHelper("mysql", MysqlRelationalJdbcLifeCycleManagement.class)
                    .dockerImageName(null)
                    .asCompatibleSubstituteFor("mysql"))
            .withDatabaseName("POLARIS_SCHEMA");

    if (initScript != null) {
      mysql.withInitScript(initScript);
    }

    context.containerNetworkId().ifPresent(mysql::withNetworkMode);
    mysql.start();

    return Map.of(
        "polaris.persistence.type",
        "relational-jdbc",
        "polaris.persistence.relational.jdbc.database-type",
        "mysql",
        "polaris.persistence.relational.jdbc.max-retries",
        "2",
        // The named MySQL datasource is declared as inactive in the bundled defaults; tests
        // (and production users) opt in by setting `active=true` on the named datasource.
        "quarkus.datasource.mysql.active",
        "true",
        "quarkus.datasource.mysql.jdbc.url",
        mysql.getJdbcUrl(),
        "quarkus.datasource.mysql.username",
        mysql.getUsername(),
        "quarkus.datasource.mysql.password",
        mysql.getPassword(),
        "quarkus.datasource.mysql.jdbc.initial-size",
        "10");
  }

  @Override
  public void stop() {
    if (mysql != null) {
      try {
        mysql.stop();
      } finally {
        mysql = null;
      }
    }
  }

  @Override
  public void setIntegrationTestContext(DevServicesContext context) {
    this.context = context;
  }
}
