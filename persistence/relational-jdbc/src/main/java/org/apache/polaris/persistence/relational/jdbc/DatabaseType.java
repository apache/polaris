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

import jakarta.annotation.Nonnull;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import org.apache.polaris.core.persistence.bootstrap.SchemaOptions;

public enum DatabaseType {
  POSTGRES("postgres"),
  H2("h2");

  private final String displayName; // Store the user-friendly name

  DatabaseType(String displayName) {
    this.displayName = displayName;
  }

  // Method to get the user-friendly display name
  public String getDisplayName() {
    return displayName;
  }

  public static DatabaseType fromDisplayName(String displayName) {
    return switch (displayName.toLowerCase(Locale.ROOT)) {
      case "h2" -> DatabaseType.H2;
      case "postgresql" -> DatabaseType.POSTGRES;
      default -> throw new IllegalStateException("Unsupported DatabaseType: '" + displayName + "'");
    };
  }

  /**
   * Open an InputStream that contains data from an init script. This stream should be closed by the
   * caller.
   */
  public InputStream openInitScriptResource(@Nonnull SchemaOptions schemaOptions) {
    if (schemaOptions.schemaFile() != null) {
      try {
        return new FileInputStream(schemaOptions.schemaFile());
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to load file " + schemaOptions.schemaFile(), e);
      }
    } else {
      final String schemaSuffix;
      switch (schemaOptions.schemaVersion()) {
        case null -> schemaSuffix = "schema-v2.sql";
        case 1 -> schemaSuffix = "schema-v1.sql";
        case 2 -> schemaSuffix = "schema-v2.sql";
        default ->
            throw new IllegalArgumentException(
                "Unknown schema version " + schemaOptions.schemaVersion());
      }
      ClassLoader classLoader = DatasourceOperations.class.getClassLoader();
      return classLoader.getResourceAsStream(this.getDisplayName() + "/" + schemaSuffix);
    }
  }
}
