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

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Normalizer that lowercases all identifiers using {@link Locale#ROOT}.
 *
 * <p>This provides predictable, locale-independent normalization suitable for ASCII-based
 * identifiers commonly used in data lake environments. For non-ASCII characters, behavior follows
 * Java's {@code String.toLowerCase(Locale.ROOT)}.
 *
 * <p>Aligns with:
 *
 * <ul>
 *   <li>Iceberg's case-insensitive field lookup using Locale.ROOT
 *   <li>Trino's identifier normalization using Locale.ENGLISH
 *   <li>Unity Catalog and Glue IRC's lowercase normalization
 * </ul>
 */
public class LowercaseNormalizer implements IdentifierNormalizer {

  public static final LowercaseNormalizer INSTANCE = new LowercaseNormalizer();

  /** Use {@link #INSTANCE} instead. */
  private LowercaseNormalizer() {}

  @Override
  public Namespace normalizeNamespace(Namespace namespace) {
    if (namespace == null || namespace.isEmpty()) {
      return namespace;
    }
    String[] levels = namespace.levels();
    String[] normalizedLevels =
        Arrays.stream(levels).map(this::normalizeName).toArray(String[]::new);
    return Namespace.of(normalizedLevels);
  }

  @Override
  public TableIdentifier normalizeTableIdentifier(TableIdentifier identifier) {
    if (identifier == null) {
      return null;
    }
    return TableIdentifier.of(
        normalizeNamespace(identifier.namespace()), normalizeName(identifier.name()));
  }

  @Override
  public String normalizeName(String name) {
    if (name == null) {
      return null;
    }
    return name.toLowerCase(Locale.ROOT);
  }

  @Override
  public List<String> normalizeNames(List<String> names) {
    if (names == null) {
      return null;
    }
    return names.stream().map(this::normalizeName).collect(Collectors.toList());
  }

  @Override
  public boolean isCaseNormalizing() {
    return true;
  }
}
