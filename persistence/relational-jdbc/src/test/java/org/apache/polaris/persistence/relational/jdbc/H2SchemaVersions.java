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

/** Discovers H2 schema versions available on the test classpath. */
final class H2SchemaVersions {

  private H2SchemaVersions() {}

  /**
   * Returns sorted schema versions for which {@code h2/schema-vN.sql} exists on the classpath.
   *
   * <p>Every version from {@code 0} through {@link DatabaseType#H2}{@linkplain
   * DatabaseType#getLatestSchemaVersion() latest} must be present. {@code schema-v0.sql} lives in
   * test resources to cover legacy bootstrap behavior; newer H2 schema files live in main
   * resources.
   */
  static SortedSet<Integer> discover() {
    ClassLoader classLoader = H2SchemaVersions.class.getClassLoader();
    int latestVersion = DatabaseType.H2.getLatestSchemaVersion();
    SortedSet<Integer> versions = new TreeSet<>();
    List<Integer> missingVersions = new ArrayList<>();
    for (int version = 0; version <= latestVersion; version++) {
      String resource =
          String.format("%s/schema-v%d.sql", DatabaseType.H2.getDisplayName(), version);
      if (classLoader.getResource(resource) == null) {
        missingVersions.add(version);
      } else {
        versions.add(version);
      }
    }
    if (!missingVersions.isEmpty()) {
      throw new IllegalStateException(
          String.format(
              "Missing H2 schema resource(s) %s for version(s) %s (expected versions 0-%d)",
              missingVersions.stream()
                  .map(
                      version ->
                          String.format(
                              "%s/schema-v%d.sql", DatabaseType.H2.getDisplayName(), version))
                  .toList(),
              missingVersions,
              latestVersion));
    }
    return versions;
  }

  static Stream<Integer> discoverAsStream() {
    return discover().stream();
  }
}
