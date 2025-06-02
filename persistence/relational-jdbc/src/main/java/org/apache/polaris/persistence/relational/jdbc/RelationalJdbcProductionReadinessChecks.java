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

package org.apache.polaris.persistence.relational.jdbc;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.sql.SQLException;
import org.apache.polaris.core.config.ProductionReadinessCheck;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;

@ApplicationScoped
public class RelationalJdbcProductionReadinessChecks {
  @Produces
  public ProductionReadinessCheck checkRelationalJdbc(
      MetaStoreManagerFactory metaStoreManagerFactory) {
    // This check should only be applicable when persistence uses RelationalJdbc.
    if (!(metaStoreManagerFactory
        instanceof JdbcMetaStoreManagerFactory jdbcMetaStoreManagerFactory)) {
      return ProductionReadinessCheck.OK;
    }

    try {
      if (jdbcMetaStoreManagerFactory.getDatabaseType().equals(DatabaseType.H2)) {
        return ProductionReadinessCheck.of(
            ProductionReadinessCheck.Error.of(
                "The current persistence (jdbc:h2) is intended for tests only.",
                "quarkus.datasource.jdbc.url"));
      }
    } catch (SQLException e) {
      return ProductionReadinessCheck.of(
          ProductionReadinessCheck.Error.of(
              "Misconfigured JDBC datasource", "quarkus.datasource.jdbc.url"));
    }
    return ProductionReadinessCheck.OK;
  }
}
