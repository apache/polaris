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

import java.util.SortedSet;
import java.util.TreeSet;

/** Discovers H2 schema versions available on the test classpath. */
final class H2SchemaVersions {

  private H2SchemaVersions() {}

  /**
   * Returns sorted schema versions for which {@code h2/schema-vN.sql} exists on the classpath.
   *
   * <p>schema-v0.sql lives in test resources to cover legacy bootstrap behavior; newer H2 schema
   * files live in main resources. Both are merged on the test classpath.
   */
  static SortedSet<Integer> discover() {
    ClassLoader classLoader = H2SchemaVersions.class.getClassLoader();
    SortedSet<Integer> versions = new TreeSet<>();
    for (int version = 0; ; version++) {
      String resource = String.format("h2/schema-v%d.sql", version);
      if (classLoader.getResource(resource) == null) {
        break;
      }
      versions.add(version);
    }
    if (versions.isEmpty()) {
      throw new IllegalStateException("No H2 schema files found on classpath");
    }
    return versions;
  }
}
