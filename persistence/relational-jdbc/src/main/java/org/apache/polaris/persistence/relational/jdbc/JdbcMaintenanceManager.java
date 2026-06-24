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

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.sql.SQLException;
import java.util.Map;
import org.apache.polaris.core.persistence.MaintenanceManager;
import org.apache.polaris.persistence.relational.jdbc.models.ModelEvent;

@ApplicationScoped
@Identifier("relational-jdbc")
public class JdbcMaintenanceManager implements MaintenanceManager {

  @Inject DatasourceOperations datasourceOperations;

  @Override
  public void purgeEvents() {
    try {
      datasourceOperations.executeUpdate(
          QueryGenerator.generateDeleteQuery(
              ModelEvent.ALL_COLUMNS, ModelEvent.TABLE_NAME, Map.of()));
    } catch (SQLException e) {
      throw new RuntimeException(String.format("Failed to purge events: %s", e.getMessage()), e);
    }
  }
}
