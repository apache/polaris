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
package org.apache.polaris.service.catalog.validation;

import java.util.regex.Pattern;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Validates entity names provided by clients at the REST layer (tables, views, namespaces, generic
 * tables, ...). A valid name:
 *
 * <ul>
 *   <li>is not null or empty;
 *   <li>is not {@code .} or {@code ..};
 *   <li>does not contain ISO control characters (U+0000–U+001F or U+007F–U+009F);
 *   <li>does not contain any of: {@code / \ : * ? " < > | # + `};
 *   <li>does not start or end with whitespace.
 * </ul>
 */
public final class EntityNameValidator {

  private EntityNameValidator() {}

  /**
   * Characters forbidden in entity names beyond control characters and leading/trailing whitespace.
   * Covers characters rejected or strongly discouraged by S3, GCS, Azure, Windows filesystem
   * semantics, URL encoding, and shell/template/SQL quoting.
   */
  private static final String FORBIDDEN_CHARS = "/\\:*?\"<>|#+`";

  private static final Pattern CONTROL_CHARS = Pattern.compile("\\p{C}");

  /** Validates a single entity name (table, view, namespace level, ...). */
  public static void validateName(String name) {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("Entity name must not be empty");
    }
    if (name.equals(".") || name.equals("..")) {
      throw new IllegalArgumentException("Entity name must not be '.' or '..'");
    }
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      if (Character.isISOControl(c)) {
        throw new IllegalArgumentException(
            String.format(
                "Entity name must not contain control characters (U+%04X): %s",
                (int) c, sanitizeForMessage(name)));
      }
      if (FORBIDDEN_CHARS.indexOf(c) >= 0) {
        throw new IllegalArgumentException("Entity name must not contain '" + c + "': " + name);
      }
      if ((i == 0 || i == name.length() - 1) && Character.isWhitespace(c)) {
        throw new IllegalArgumentException(
            "Entity name must not have leading or trailing whitespace: " + name);
      }
    }
  }

  /** Validates each level of a namespace. */
  public static void validateNamespace(Namespace namespace) {
    for (String level : namespace.levels()) {
      validateName(level);
    }
  }

  public static void validateIdentifier(TableIdentifier identifier) {
    validateNamespace(identifier.namespace());
    validateName(identifier.name());
  }

  private static String sanitizeForMessage(String name) {
    return CONTROL_CHARS
        .matcher(name)
        .replaceAll(m -> String.format("\\\\u%04X", (int) m.group().charAt(0)));
  }
}
