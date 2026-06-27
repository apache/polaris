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

package org.apache.polaris.quarkus.common.config.jdbc;

import io.quarkus.agroal.DataSource.DataSourceLiteral;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import javax.sql.DataSource;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;

public class JdbcProducers {

  @Produces
  @ApplicationScoped
  public DatasourceOperations produceDatasourceOperations(
      @Any Instance<DataSource> dataSources,
      QuarkusRelationalJdbcConfiguration relationalJdbcConfiguration) {
    String dataSourceName = relationalJdbcConfiguration.dataSource();
    Instance<DataSource> dataSourceInstance =
        dataSources.select(new DataSourceLiteral(dataSourceName));
    if (dataSourceInstance.isUnsatisfied()) {
      throw new IllegalStateException(
          "datasource %s not found; check the 'polaris.persistence.relational.jdbc.datasource' property in your configuration"
              .formatted(dataSourceName));
    }
    DataSource dataSource = dataSourceInstance.get();
    return new DatasourceOperations(dataSource, relationalJdbcConfiguration);
  }
}
