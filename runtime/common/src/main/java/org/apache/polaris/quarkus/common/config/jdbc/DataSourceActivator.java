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

import io.smallrye.config.ConfigSourceInterceptor;
import io.smallrye.config.ConfigSourceInterceptorContext;
import io.smallrye.config.ConfigValue;
import io.smallrye.config.ConfigValue.ConfigValueBuilder;
import io.smallrye.config.Priorities;
import jakarta.annotation.Priority;

/**
 * Activates a data source based on the current Polaris configuration under {@link
 * #POLARIS_DATASOURCE_NAME_PROPERTY}, and deactivates all other data sources.
 *
 * <p>If the persistence type is not JDBC, all data sources are deactivated.
 */
@Priority(Priorities.LIBRARY + 400)
public class DataSourceActivator implements ConfigSourceInterceptor {

  /**
   * The default ordinal to use for modified {@code quarkus.datasource.*.active} properties. It must
   * be higher than the ordinal of the application.properties classpath config source (250) but
   * lower than the ordinal of the user-supplied application.properties (260).
   */
  private static final int DEFAULT_ORDINAL = 251;

  private static final String POLARIS_PERSISTENCE_TYPE_PROPERTY = "polaris.persistence.type";
  private static final String POLARIS_DATASOURCE_NAME_PROPERTY =
      "polaris.persistence.relational.jdbc.datasource";

  private static final String RELATIONAL_JDBC_PERSISTENCE_TYPE = "relational-jdbc";

  private String activeDataSourceName;
  private String persistenceType;

  @Override
  public ConfigValue getValue(ConfigSourceInterceptorContext context, String name) {
    ConfigValue value = context.proceed(name);
    if (name.startsWith("quarkus.datasource.") && name.endsWith(".active")) {
      if (name.equals("quarkus.datasource.*.active")) {
        // Wildcard datasource key: return the unmodified value
        return value;
      }
      boolean active;
      if (persistenceType(context).equals(RELATIONAL_JDBC_PERSISTENCE_TYPE)) {
        if (name.equals("quarkus.datasource.active")) {
          // default (unnamed) datasource should never be activated
          active = false;
        } else {
          String dataSourceName = extractDataSourceName(name);
          active = dataSourceName.equals(targetDataSourceName(context));
        }
      } else {
        // all datasources deactivated if persistence type is not relational-jdbc
        active = false;
      }
      if (value == null
          || value.getValue() == null
          || active != Boolean.parseBoolean(value.getValue())) {
        value = newConfigValue(value, Boolean.toString(active));
      }
    }
    return value;
  }

  private ConfigValue newConfigValue(ConfigValue current, String newValue) {
    ConfigValueBuilder builder = current == null ? ConfigValue.builder() : current.from();
    int ordinal = current == null ? DEFAULT_ORDINAL : current.getConfigSourceOrdinal() + 1;
    return builder.withConfigSourceOrdinal(ordinal).withValue(newValue).build();
  }

  private String extractDataSourceName(String property) {
    String dataSourceName = property.substring("quarkus.datasource.".length());
    dataSourceName = dataSourceName.substring(0, dataSourceName.indexOf('.'));
    return unquote(dataSourceName);
  }

  private synchronized String persistenceType(ConfigSourceInterceptorContext context) {
    if (persistenceType == null) {
      ConfigValue value = context.proceed(POLARIS_PERSISTENCE_TYPE_PROPERTY);
      if (value == null || value.getValue() == null) {
        failOnMissingProperty(POLARIS_PERSISTENCE_TYPE_PROPERTY);
      }
      persistenceType = unquote(value.getValue());
    }
    return persistenceType;
  }

  private synchronized String targetDataSourceName(ConfigSourceInterceptorContext context) {
    if (activeDataSourceName == null) {
      ConfigValue value = context.proceed(POLARIS_DATASOURCE_NAME_PROPERTY);
      if (value == null || value.getValue() == null) {
        failOnMissingProperty(POLARIS_DATASOURCE_NAME_PROPERTY);
      }
      activeDataSourceName = unquote(value.getValue());
    }
    return activeDataSourceName;
  }

  private static void failOnMissingProperty(String polarisPersistenceTypeProperty) {
    throw new IllegalStateException(
        "missing required '%s' property in configuration"
            .formatted(polarisPersistenceTypeProperty));
  }

  private static String unquote(String dataSourceName) {
    if (dataSourceName.startsWith("\"") && dataSourceName.endsWith("\"")) {
      dataSourceName = dataSourceName.substring(1, dataSourceName.length() - 1);
    }
    return dataSourceName;
  }
}
