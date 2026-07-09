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

import jakarta.validation.constraints.Pattern;
import java.util.Optional;

public interface RelationalJdbcConfiguration {

  /** Schema used when {@link #schemaName()} is not configured. */
  String DEFAULT_SCHEMA_NAME = "POLARIS_SCHEMA";

  // max retries before giving up
  Optional<Integer> maxRetries();

  // max retry duration
  Optional<Long> maxDurationInMs();

  // initial delay
  Optional<Long> initialDelayInMs();

  /**
   * Explicitly configured database type. If not specified, the database type will be inferred from
   * the JDBC connection metadata. Supported values: "postgresql", "cockroachdb", "h2"
   */
  Optional<String> databaseType();

  /**
   * The database schema (namespace) that holds the Polaris tables. If not specified, it defaults to
   * {@code POLARIS_SCHEMA}. The schema is created during bootstrap if it does not already exist,
   * and is selected as the session schema (search path) on every JDBC connection Polaris acquires.
   *
   * <p>The value must be a plain SQL identifier: it must start with a letter or underscore and
   * contain only letters, digits, and underscores. It is used unquoted, so the database applies its
   * usual identifier case folding (for example PostgreSQL folds it to lowercase, H2 to uppercase),
   * matching how the default schema has always been handled.
   */
  Optional<
          @Pattern(
              regexp = "[A-Za-z_][A-Za-z0-9_]*",
              message =
                  "must start with a letter or underscore and contain only letters, digits, and underscores")
          String>
      schemaName();
}
