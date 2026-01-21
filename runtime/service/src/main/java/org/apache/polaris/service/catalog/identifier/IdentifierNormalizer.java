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
package org.apache.polaris.service.catalog.identifier;

import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Strategy interface for normalizing identifiers in catalog operations.
 *
 * <p>Implementations determine how namespace, table, and view names are transformed before
 * persistence and resolution.
 *
 * <p>Default implementation preserves original casing. Case-insensitive catalogs use an
 * implementation that lowercases all identifiers.
 */
public interface IdentifierNormalizer {

  /**
   * Normalize a namespace.
   *
   * @param namespace the original namespace
   * @return normalized namespace
   */
  Namespace normalizeNamespace(Namespace namespace);

  /**
   * Normalize a table identifier (namespace + table name).
   *
   * @param identifier the original table identifier
   * @return normalized table identifier
   */
  TableIdentifier normalizeTableIdentifier(TableIdentifier identifier);

  /**
   * Normalize a single identifier string (e.g., table name, view name).
   *
   * @param name the original name
   * @return normalized name
   */
  String normalizeName(String name);

  /**
   * Normalize a list of identifier names (e.g., namespace levels).
   *
   * @param names the original names
   * @return normalized names
   */
  List<String> normalizeNames(List<String> names);

  /**
   * Check if this normalizer performs case normalization.
   *
   * @return true if identifiers are case-normalized
   */
  boolean isCaseNormalizing();
}
