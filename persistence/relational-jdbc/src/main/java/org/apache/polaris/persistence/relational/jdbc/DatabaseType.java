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

import java.util.Locale;

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

  public String getInitScriptResource() {
    return String.format("%s/schema-v1.sql", this.getDisplayName());
  }
}
