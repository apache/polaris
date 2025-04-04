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
package org.apache.polaris.extension.persistence.relational.jdbc;

import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionManager.class);

  private DataSource dataSource;

  public ConnectionManager(RelationalJdbcConfiguration jdbcConfiguration) {
    this.dataSource = initializeDataSource(jdbcConfiguration);
  }

  private BasicDataSource initializeDataSource(RelationalJdbcConfiguration jdbcConfiguration) {
    BasicDataSource bds = new BasicDataSource();

    // set-up jdbc driver
    bds.setUrl(jdbcConfiguration.jdbcUrl().get());
    bds.setUsername(jdbcConfiguration.userName().get());
    bds.setPassword(jdbcConfiguration.password().get());
    bds.setDriverClassName(jdbcConfiguration.driverClassName().get());

    // Optional connection pool configuration
    if (jdbcConfiguration.initialPoolSize().isPresent()) {
      bds.setInitialSize(jdbcConfiguration.initialPoolSize().get());
    }
    if (jdbcConfiguration.maxTotal().isPresent()) {
      bds.setMaxTotal(jdbcConfiguration.maxTotal().get());
    }
    if (jdbcConfiguration.maxIdle().isPresent()) {
      bds.setMaxIdle(jdbcConfiguration.maxIdle().get());
    }
    if (jdbcConfiguration.minIdle().isPresent()) {
      bds.setMinIdle(jdbcConfiguration.minIdle().get());
    }
    if (jdbcConfiguration.maxWaitMillis().isPresent()) {
      bds.setMaxWaitMillis(jdbcConfiguration.maxWaitMillis().get());
    }
    if (jdbcConfiguration.testOnBorrow().isPresent()) {
      bds.setTestOnBorrow(jdbcConfiguration.testOnBorrow().get());
    }
    if (jdbcConfiguration.testOnReturn().isPresent()) {
      bds.setTestOnReturn(jdbcConfiguration.testOnReturn().get());
    }
    if (jdbcConfiguration.testWhileIdle().isPresent()) {
      bds.setTestWhileIdle(jdbcConfiguration.testWhileIdle().get());
    }
    if (jdbcConfiguration.validationQuery().isPresent()) {
      bds.setValidationQuery(jdbcConfiguration.validationQuery().get());
    }
    if (jdbcConfiguration.timeBetweenEvictionRunsMillis().isPresent()) {
      bds.setTimeBetweenEvictionRunsMillis(jdbcConfiguration.timeBetweenEvictionRunsMillis().get());
    }
    if (jdbcConfiguration.minEvictableIdleTimeMillis().isPresent()) {
      bds.setMinEvictableIdleTimeMillis(jdbcConfiguration.minEvictableIdleTimeMillis().get());
    }

    return bds;
  }

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public DataSource getDataSource() {
    return dataSource;
  }

  public void closeDataSource() throws Exception {
    if (dataSource instanceof BasicDataSource) {
      LOGGER.info("Closing BasicDataSource");
      ((BasicDataSource) dataSource).close();
    }
  }
}
