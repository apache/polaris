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

import java.util.Optional;

/** Minimal {@link RelationalJdbcConfiguration} for JDBC persistence integration tests. */
final class SimpleRelationalJdbcConfiguration implements RelationalJdbcConfiguration {

  private final String databaseType;

  private SimpleRelationalJdbcConfiguration(String databaseType) {
    this.databaseType = databaseType;
  }

  static SimpleRelationalJdbcConfiguration forDatabaseType(DatabaseType databaseType) {
    return switch (databaseType) {
      case H2 -> new SimpleRelationalJdbcConfiguration("h2");
      case POSTGRES -> new SimpleRelationalJdbcConfiguration("postgresql");
      case COCKROACHDB -> new SimpleRelationalJdbcConfiguration("cockroachdb");
    };
  }

  @Override
  public Optional<Integer> maxRetries() {
    return Optional.of(2);
  }

  @Override
  public Optional<Long> maxDurationInMs() {
    return Optional.of(100L);
  }

  @Override
  public Optional<Long> initialDelayInMs() {
    return Optional.of(100L);
  }

  @Override
  public Optional<String> databaseType() {
    return Optional.of(databaseType);
  }
}
