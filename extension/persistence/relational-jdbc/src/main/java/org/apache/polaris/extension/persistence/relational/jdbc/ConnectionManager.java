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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionManager.class);

  private DataSource dataSource;
  private final Properties properties;

  public ConnectionManager(String propsFilePath) throws IOException {
    this.properties = readPropertiesFromResource(propsFilePath);
    this.dataSource = initializeDataSource();
  }

  private static Properties readPropertiesFromResource(String propsFilePath) throws IOException {
    Properties properties = new Properties();
    ClassLoader classLoader = ConnectionManager.class.getClassLoader();
    try {
      properties.load(classLoader.getResourceAsStream(propsFilePath));
    } catch (IOException e) {
      // need to re-throw
      try {
        properties.load(fileSystemPath(propsFilePath).toUri().toURL().openStream());
      } catch (IOException e1) {
        LOGGER.error("Failed to load properties from {}", propsFilePath, e1);
      }
      throw e;
    }
    return properties;
  }

  private BasicDataSource initializeDataSource() throws IOException {
    BasicDataSource bds = new BasicDataSource();
    bds.setUrl(properties.getProperty("jdbc.url"));
    bds.setUsername(properties.getProperty("jdbc.username"));
    bds.setPassword(properties.getProperty("jdbc.password"));
    bds.setDriverClassName(properties.getProperty("jdbc.driverClassName"));

    // Optional connection pool configuration
    if (properties.getProperty("dbcp2.initialSize") != null) {
      bds.setInitialSize(Integer.parseInt(properties.getProperty("dbcp2.initialSize")));
    }
    if (properties.getProperty("dbcp2.maxTotal") != null) {
      bds.setMaxTotal(Integer.parseInt(properties.getProperty("dbcp2.maxTotal")));
    }
    if (properties.getProperty("dbcp2.maxIdle") != null) {
      bds.setMaxIdle(Integer.parseInt(properties.getProperty("dbcp2.maxIdle")));
    }
    if (properties.getProperty("dbcp2.minIdle") != null) {
      bds.setMinIdle(Integer.parseInt(properties.getProperty("dbcp2.minIdle")));
    }
    if (properties.getProperty("dbcp2.maxWaitMillis") != null) {
      bds.setMaxWaitMillis(Long.parseLong(properties.getProperty("dbcp2.maxWaitMillis")));
    }
    if (properties.getProperty("dbcp2.testOnBorrow") != null) {
      bds.setTestOnBorrow(Boolean.parseBoolean(properties.getProperty("dbcp2.testOnBorrow")));
    }
    if (properties.getProperty("dbcp2.testOnReturn") != null) {
      bds.setTestOnReturn(Boolean.parseBoolean(properties.getProperty("dbcp2.testOnReturn")));
    }
    if (properties.getProperty("dbcp2.testWhileIdle") != null) {
      bds.setTestWhileIdle(Boolean.parseBoolean(properties.getProperty("dbcp2.testWhileIdle")));
    }
    if (properties.getProperty("dbcp2.validationQuery") != null) {
      bds.setValidationQuery(properties.getProperty("dbcp2.validationQuery"));
    }
    if (properties.getProperty("dbcp2.timeBetweenEvictionRunsMillis") != null) {
      bds.setTimeBetweenEvictionRunsMillis(
          Long.parseLong(properties.getProperty("dbcp2.timeBetweenEvictionRunsMillis")));
    }
    if (properties.getProperty("dbcp2.minEvictableIdleTimeMillis") != null) {
      bds.setMinEvictableIdleTimeMillis(
          Long.parseLong(properties.getProperty("dbcp2.minEvictableIdleTimeMillis")));
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

  private static Path fileSystemPath(String pathStr) {
    Path path = Paths.get(pathStr);
    if (!Files.exists(path) || !Files.isRegularFile(path)) {
      throw new IllegalStateException("Not a regular file: " + pathStr);
    }
    return path.normalize().toAbsolutePath();
  }
}
