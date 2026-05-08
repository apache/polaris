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

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Validates entity names provided by clients at the REST layer (tables, views, namespaces, generic
 * tables, ...). A valid name:
 *
 * <ul>
 *   <li>is not null or empty;
 *   <li>does not contain a forward slash ({@code /});
 *   <li>does not start or end with whitespace.
 * </ul>
 */
public final class EntityNameValidator {

  private EntityNameValidator() {}

  /** Validates a single entity name (table, view, namespace level, ...). */
  public static void validateName(String name) {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("Entity name must not be empty");
    }
    if (name.indexOf('/') >= 0) {
      throw new IllegalArgumentException("Entity name must not contain '/': " + name);
    }
    if (!name.equals(name.strip())) {
      throw new IllegalArgumentException(
          "Entity name must not have leading or trailing whitespace: " + name);
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
}
