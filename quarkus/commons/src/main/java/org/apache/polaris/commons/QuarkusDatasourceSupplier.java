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
package org.apache.polaris.commons;

import io.quarkus.arc.All;
import io.quarkus.arc.InstanceHandle;
import java.util.List;
import javax.sql.DataSource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.polaris.core.persistence.DatasourceSupplier;

@ApplicationScoped
public class QuarkusDatasourceSupplier implements DatasourceSupplier {
  private final List<InstanceHandle<DataSource>> dataSources;
  private final RelationalJdbcConfiguration relationalJdbcConfiguration;

  private static final String DEFAULT_DATA_SOURCE_NAME = "default";

  @Inject
  public QuarkusDatasourceSupplier(
      RelationalJdbcConfiguration relationalJdbcConfiguration,
      @All List<InstanceHandle<DataSource>> dataSources) {
    this.relationalJdbcConfiguration = relationalJdbcConfiguration;
    this.dataSources = dataSources;
  }

  @Override
  public DataSource fromRealmId(String realmId) {
    // check if the mapping of realm to DS exists, otherwise fall back to default
    String dataSourceName = relationalJdbcConfiguration.realms().getOrDefault(
            realmId,
            relationalJdbcConfiguration.defaultDatasource().orElse(null)
    );

    // if neither mapping exists nor default DS exists, fail
    if (dataSourceName == null) {
      throw new IllegalStateException(String.format(
              "No datasource configured with name: %s nor default datasource configured", realmId));
    }

    // check if there is actually a datasource of that dataSourceName
    return dataSources.stream()
            .filter(ds -> {
              String name = ds.getBean().getName();
              name = name == null ? DEFAULT_DATA_SOURCE_NAME : name;
              return name.equals(dataSourceName);
            })
            .map(InstanceHandle::get)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(String.format(
                    "No datasource configured with name: %s", dataSourceName)));
  }
}
