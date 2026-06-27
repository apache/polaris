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

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;
import java.util.Optional;
import org.apache.polaris.persistence.relational.jdbc.RelationalJdbcConfiguration;

@ConfigMapping(prefix = "polaris.persistence.relational.jdbc")
public interface QuarkusRelationalJdbcConfiguration extends RelationalJdbcConfiguration {

  /** The maximum number of retries before giving up the operation. */
  @Override
  Optional<Integer> maxRetries();

  /** The maximum retry duration in milliseconds. */
  @Override
  Optional<Long> maxDurationInMs();

  /** The initial retry delay. */
  @Override
  Optional<Long> initialDelayInMs();

  /**
   * Explicitly configured database type. If not specified, the database type will be inferred from
   * the JDBC connection metadata. Supported values: "postgresql", "cockroachdb", "h2"
   */
  @Override
  Optional<String> databaseType();

  /** The datasource name to use. Required. */
  @Override
  @WithName("datasource")
  String dataSource();
}
