/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.polaris.core.persistence.bootstrap.BootstrapOptions;
import org.apache.polaris.core.persistence.bootstrap.SchemaOptions;

public class JdbcBootstrapUtils {

  // Define a pattern to find 'v' followed by one or more digits (\d+)
  private static final Pattern pattern = Pattern.compile("(v\\d+)");

  private JdbcBootstrapUtils() {}

  /**
   * Determines the correct schema version to use for bootstrapping a realm.
   *
   * @param currentSchemaVersion The current version of the database schema.
   * @param requiredSchemaVersion The requested schema version (-1 for auto-detection).
   * @param hasAlreadyBootstrappedRealms Flag indicating if any realms already exist.
   * @return The calculated bootstrap schema version.
   * @throws IllegalStateException if the combination of parameters represents an invalid state.
   */
  public static int getRealmBootstrapSchemaVersion(
      int currentSchemaVersion, int requiredSchemaVersion, boolean hasAlreadyBootstrappedRealms) {

    // If versions already match, no change is needed.
    if (currentSchemaVersion == requiredSchemaVersion) {
      return requiredSchemaVersion;
    }

    // Handle fresh installations where no schema version is recorded (version 0).
    if (currentSchemaVersion == 0) {
      if (hasAlreadyBootstrappedRealms) {
        // System was bootstrapped with v1 before schema versioning was introduced.
        if (requiredSchemaVersion == -1 || requiredSchemaVersion == 1) {
          return 1;
        }
      } else {
        // A truly fresh start. Default to v3 for auto-detection, otherwise use the specified
        // version.
        return requiredSchemaVersion == -1 ? 3 : requiredSchemaVersion;
      }
    }

    // Handle auto-detection on an existing installation (current version > 0).
    if (requiredSchemaVersion == -1) {
      // Use the current version if realms already exist; otherwise, use v3 for the new realm.
      return hasAlreadyBootstrappedRealms ? currentSchemaVersion : 3;
    }

    // Any other combination is an unhandled or invalid migration path.
    throw new IllegalStateException(
        String.format(
            "Cannot determine bootstrap schema version. Current: %d, Required: %d, Bootstrapped: %b",
            currentSchemaVersion, requiredSchemaVersion, hasAlreadyBootstrappedRealms));
  }

  /**
   * Extracts the requested schema version from the provided BootstrapOptions.
   *
   * @param bootstrapOptions: The bootstrap options containing schema information from which to
   *     extract the version.
   * @return The requested schema version, or -1 if not specified.
   */
  public static int getRequestedSchemaVersion(BootstrapOptions bootstrapOptions) {
    SchemaOptions schemaOptions = bootstrapOptions.schemaOptions();
    if (schemaOptions != null) {
      Optional<Integer> version = schemaOptions.schemaVersion();
      if (version.isPresent()) {
        return version.get();
      }
      Optional<String> schemaFile = schemaOptions.schemaFile();
      if (schemaFile.isPresent()) {
        Matcher matcher = pattern.matcher(schemaFile.get());
        if (matcher.find()) {
          String versionStr = matcher.group(1); // "v3"
          return Integer.parseInt(versionStr.substring(1)); // 3
        }
      }
    }
    return -1;
  }
}
