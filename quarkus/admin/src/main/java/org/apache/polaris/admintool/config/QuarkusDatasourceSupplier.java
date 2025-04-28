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
package org.apache.polaris.admintool.config;

import io.quarkus.arc.InstanceHandle;
import java.util.List;
import javax.sql.DataSource;
import org.apache.polaris.extension.persistence.relational.jdbc.DatasourceSupplier;
import org.apache.polaris.extension.persistence.relational.jdbc.RelationalJdbcConfiguration;

public class QuarkusDatasourceSupplier implements DatasourceSupplier {
  private final List<InstanceHandle<DataSource>> dataSources;
  private final RelationalJdbcConfiguration relationalJdbcConfiguration;

  public static final String DEFAULT_DATA_SOURCE_NAME = "<default>";

  public QuarkusDatasourceSupplier(
      RelationalJdbcConfiguration relationalJdbcConfiguration,
      List<InstanceHandle<DataSource>> dataSources) {
    this.relationalJdbcConfiguration = relationalJdbcConfiguration;
    this.dataSources = dataSources;
  }

  @Override
  public DataSource fromRealmId(String realmId) {
    String dataSourceName =
        relationalJdbcConfiguration.datasource().getOrDefault(realmId, DEFAULT_DATA_SOURCE_NAME);
    for (InstanceHandle<DataSource> handle : dataSources) {
      String name = handle.getBean().getName();
      name = name == null ? DEFAULT_DATA_SOURCE_NAME : name;
      // if realm isolation is DB then there should be only one DS configured.
      if (name.equals(dataSourceName)) {
        return handle.get();
      }
    }
    throw new IllegalStateException("No datasource configured with name: " + realmId);
  }
}
