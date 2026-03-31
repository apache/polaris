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
package org.apache.polaris.quarkus.common.persistence.jdbc;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import org.apache.polaris.persistence.relational.jdbc.DataSourceResolver;
import org.apache.polaris.quarkus.common.config.jdbc.QuarkusRelationalJdbcConfiguration;

/**
 * CDI producers for JDBC-specific beans. Moved to runtime-common to keep the persistence layer
 * implementation-agnostic regarding configuration sources.
 */
public class JdbcCdiProducers {

  /**
   * Produces the active {@link DataSourceResolver} by selecting the bean identified by {@link
   * QuarkusRelationalJdbcConfiguration#dataSourceResolverType()}.
   *
   * <p>The result is {@link ApplicationScoped} because the datasource-resolver type cannot change
   * at runtime.
   */
  @Produces
  @ApplicationScoped
  public DataSourceResolver dataSourceResolver(
      QuarkusRelationalJdbcConfiguration jdbcConfig,
      @Any Instance<DataSourceResolver> dataSourceResolvers) {
    String type = jdbcConfig.dataSourceResolverType();
    return dataSourceResolvers.select(Identifier.Literal.of(type)).get();
  }
}
