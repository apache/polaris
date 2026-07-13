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

import java.util.Arrays;
import java.util.Optional;

/** Separately bootstrapped JDBC schema components. */
enum JdbcSchemaComponent {
  METASTORE("version", "schema"),
  LINEAGE("lineage", "schema/lineage/lineage");

  private final String versionKey;
  private final String resourcePrefix;

  JdbcSchemaComponent(String versionKey, String resourcePrefix) {
    this.versionKey = versionKey;
    this.resourcePrefix = resourcePrefix;
  }

  String versionKey() {
    return versionKey;
  }

  static Optional<JdbcSchemaComponent> fromVersionKey(String versionKey) {
    return Arrays.stream(values())
        .filter(component -> component.versionKey.equals(versionKey))
        .findFirst();
  }

  String resourceName(DatabaseType databaseType, int schemaVersion) {
    return String.format(
        "%s/%s-v%d.sql", databaseType.getDisplayName(), resourcePrefix, schemaVersion);
  }

  int latestVersion(DatabaseType databaseType) {
    return switch (this) {
      case METASTORE -> databaseType.getLatestSchemaVersion();
      case LINEAGE -> databaseType.getLatestLineageSchemaVersion();
    };
  }
}
