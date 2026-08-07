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

final class TestingRelationalJdbcConfiguration implements RelationalJdbcConfiguration {

  private final String databaseType;
  private final String jdbcUrl;
  private final String driver;
  private final String driverDirectory;
  private final String username;
  private final String password;
  private final Integer maximumPoolSize;
  private final Integer minimumIdle;
  private final Long connectionTimeoutInMs;

  private TestingRelationalJdbcConfiguration(Builder builder) {
    this.databaseType = builder.databaseType;
    this.jdbcUrl = builder.jdbcUrl;
    this.driver = builder.driver;
    this.driverDirectory = builder.driverDirectory;
    this.username = builder.username;
    this.password = builder.password;
    this.maximumPoolSize = builder.maximumPoolSize;
    this.minimumIdle = builder.minimumIdle;
    this.connectionTimeoutInMs = builder.connectionTimeoutInMs;
  }

  static Builder builder() {
    return new Builder();
  }

  @Override
  public Optional<Integer> maxRetries() {
    return Optional.of(1);
  }

  @Override
  public Optional<Long> maxDurationInMs() {
    return Optional.of(100L);
  }

  @Override
  public Optional<Long> initialDelayInMs() {
    return Optional.of(10L);
  }

  @Override
  public Optional<String> databaseType() {
    return Optional.ofNullable(databaseType);
  }

  @Override
  public Optional<String> jdbcUrl() {
    return Optional.ofNullable(jdbcUrl);
  }

  @Override
  public Optional<String> driver() {
    return Optional.ofNullable(driver);
  }

  @Override
  public Optional<String> driverDirectory() {
    return Optional.ofNullable(driverDirectory);
  }

  @Override
  public Optional<String> username() {
    return Optional.ofNullable(username);
  }

  @Override
  public Optional<String> password() {
    return Optional.ofNullable(password);
  }

  @Override
  public Optional<Integer> maximumPoolSize() {
    return Optional.ofNullable(maximumPoolSize);
  }

  @Override
  public Optional<Integer> minimumIdle() {
    return Optional.ofNullable(minimumIdle);
  }

  @Override
  public Optional<Long> connectionTimeoutInMs() {
    return Optional.ofNullable(connectionTimeoutInMs);
  }

  static final class Builder {
    private String databaseType;
    private String jdbcUrl;
    private String driver;
    private String driverDirectory;
    private String username;
    private String password;
    private Integer maximumPoolSize;
    private Integer minimumIdle;
    private Long connectionTimeoutInMs;

    private Builder() {}

    Builder databaseType(String databaseType) {
      this.databaseType = databaseType;
      return this;
    }

    Builder jdbcUrl(String jdbcUrl) {
      this.jdbcUrl = jdbcUrl;
      return this;
    }

    Builder driver(String driver) {
      this.driver = driver;
      return this;
    }

    Builder driverDirectory(String driverDirectory) {
      this.driverDirectory = driverDirectory;
      return this;
    }

    Builder username(String username) {
      this.username = username;
      return this;
    }

    Builder password(String password) {
      this.password = password;
      return this;
    }

    Builder maximumPoolSize(int maximumPoolSize) {
      this.maximumPoolSize = maximumPoolSize;
      return this;
    }

    Builder minimumIdle(int minimumIdle) {
      this.minimumIdle = minimumIdle;
      return this;
    }

    Builder connectionTimeoutInMs(long connectionTimeoutInMs) {
      this.connectionTimeoutInMs = connectionTimeoutInMs;
      return this;
    }

    TestingRelationalJdbcConfiguration build() {
      return new TestingRelationalJdbcConfiguration(this);
    }
  }
}
