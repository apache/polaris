/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.core.catalog;

import io.polaris.core.entity.PolarisEntity;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds helper methods translating between persistence-layer structs and Iceberg objects shared by
 * different Polaris components.
 */
public class PolarisCatalogHelpers {
  private static final Logger LOG = LoggerFactory.getLogger(PolarisCatalogHelpers.class);

  /** Not intended for instantiation. */
  private PolarisCatalogHelpers() {}

  public static List<String> tableIdentifierToList(TableIdentifier identifier) {
    List<String> fullList = new ArrayList<>(Arrays.asList(identifier.namespace().levels()));
    fullList.add(identifier.name());
    return fullList;
  }

  public static TableIdentifier listToTableIdentifier(List<String> ids) {
    return TableIdentifier.of(ids.toArray(new String[0]));
  }

  public static Namespace getParentNamespace(Namespace namespace) {
    if (namespace.isEmpty() || namespace.length() == 1) {
      return Namespace.empty();
    }
    String[] parentLevels = new String[namespace.length() - 1];
    for (int i = 0; i < parentLevels.length; ++i) {
      parentLevels[i] = namespace.level(i);
    }
    return Namespace.of(parentLevels);
  }

  public static List<Namespace> nameAndIdToNamespaces(
      List<PolarisEntity> catalogPath, List<PolarisEntity.NameAndId> entities) {
    // Skip element 0 which is the catalog entity
    String[] parentNamespaces = new String[catalogPath.size() - 1];
    for (int i = 0; i < parentNamespaces.length; ++i) {
      parentNamespaces[i] = catalogPath.get(i + 1).getName();
    }
    List<Namespace> namespaces = new ArrayList<>();
    for (PolarisEntity.NameAndId entity : entities) {
      String[] fullName = Arrays.copyOf(parentNamespaces, parentNamespaces.length + 1);
      fullName[fullName.length - 1] = entity.getName();
      namespaces.add(Namespace.of(fullName));
    }
    return namespaces;
  }

  /**
   * Given the shortnames/ids of entities that all live under the given catalogPath, reconstructs
   * TableIdentifier objects for each that all hold the catalogPath excluding the catalog entity.
   */
  public static List<TableIdentifier> nameAndIdToTableIdentifiers(
      List<PolarisEntity> catalogPath, List<PolarisEntity.NameAndId> entities) {
    // Skip element 0 which is the catalog entity
    String[] parentNamespaces = new String[catalogPath.size() - 1];
    for (int i = 0; i < parentNamespaces.length; ++i) {
      parentNamespaces[i] = catalogPath.get(i + 1).getName();
    }
    Namespace sharedNamespace = Namespace.of(parentNamespaces);
    return entities.stream()
        .map(entity -> TableIdentifier.of(sharedNamespace, entity.getName()))
        .collect(Collectors.toList());
  }
}
