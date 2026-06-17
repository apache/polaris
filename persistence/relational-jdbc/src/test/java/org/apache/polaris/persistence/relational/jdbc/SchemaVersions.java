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

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Stream;

/** Discovers JDBC schema versions available on the test classpath for a {@link DatabaseType}. */
final class SchemaVersions {

  private SchemaVersions() {}

  /**
   * Returns sorted schema versions for which {@code <database>/schema-vN.sql} exists on the
   * classpath.
   *
   * <p>Every version from the type's first schema version through {@link
   * DatabaseType#getLatestSchemaVersion()} must be present. H2 {@code schema-v0.sql} lives in test
   * resources to cover legacy bootstrap behavior; production schema files live in main resources.
   */
  static SortedSet<Integer> discover(DatabaseType databaseType) {
    ClassLoader classLoader = SchemaVersions.class.getClassLoader();
    int firstVersion = firstSchemaVersion(databaseType);
    int latestVersion = databaseType.getLatestSchemaVersion();
    SortedSet<Integer> versions = new TreeSet<>();
    List<Integer> missingVersions = new ArrayList<>();
    for (int version = firstVersion; version <= latestVersion; version++) {
      String resource = String.format("%s/schema-v%d.sql", databaseType.getDisplayName(), version);
      if (classLoader.getResource(resource) == null) {
        missingVersions.add(version);
      } else {
        versions.add(version);
      }
    }
    if (!missingVersions.isEmpty()) {
      throw new IllegalStateException(
          String.format(
              "Missing %s schema resource(s) %s for version(s) %s (expected versions %d-%d)",
              databaseType,
              missingVersions.stream()
                  .map(
                      version ->
                          String.format(
                              "%s/schema-v%d.sql", databaseType.getDisplayName(), version))
                  .toList(),
              missingVersions,
              firstVersion,
              latestVersion));
    }
    return versions;
  }

  static Stream<Integer> discoverAsStream(DatabaseType databaseType) {
    return discover(databaseType).stream();
  }

  private static int firstSchemaVersion(DatabaseType databaseType) {
    return switch (databaseType) {
      case H2 -> 0;
      case POSTGRES, COCKROACHDB -> 1;
    };
  }
}
