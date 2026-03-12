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

import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import javax.sql.DataSource;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.persistence.relational.jdbc.DataSourceResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link DataSourceResolver} that routes all realms and store types to a
 * single default {@link DataSource}. This implementation acts as a fallback; downstream users can
 * provide their own {@link DataSourceResolver} bean to implement custom routing logic.
 */
@ApplicationScoped
@DefaultBean
public class DefaultDataSourceResolver implements DataSourceResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDataSourceResolver.class);

  private final Instance<DataSource> defaultDataSource;

  @Inject
  public DefaultDataSourceResolver(Instance<DataSource> defaultDataSource) {
    this.defaultDataSource = defaultDataSource;
  }

  @Override
  public DataSource resolve(RealmContext realmContext, StoreType storeType) {
    LOGGER.debug(
        "Using default DataSource for realm '{}' and store '{}'",
        realmContext.getRealmIdentifier(),
        storeType);
    return defaultDataSource.get();
  }
}
