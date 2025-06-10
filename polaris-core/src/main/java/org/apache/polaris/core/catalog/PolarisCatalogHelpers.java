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
package org.apache.polaris.core.catalog;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.entity.PolarisEntity;

/**
 * Holds helper methods translating between persistence-layer structs and Iceberg objects shared by
 * different Polaris components.
 */
public class PolarisCatalogHelpers {
  /** Not intended for instantiation. */
  private PolarisCatalogHelpers() {}

  public static List<String> tableIdentifierToList(TableIdentifier identifier) {
    return identifierToList(identifier.namespace(), identifier.name());
  }

  public static List<String> identifierToList(Namespace namespace, String name) {
    ImmutableList.Builder<String> fullList =
        ImmutableList.builderWithExpectedSize(namespace.length() + 1);
    fullList.addAll(Arrays.asList(namespace.levels()));
    fullList.add(name);
    return fullList.build();
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

  public static Namespace nameAndIdToNamespace(
      List<PolarisEntity> catalogPath, PolarisEntity.NameAndId entity) {
    // Skip element 0 which is the catalog entity
    String[] fullName = new String[catalogPath.size()];
    for (int i = 0; i < fullName.length - 1; ++i) {
      fullName[i] = catalogPath.get(i + 1).getName();
    }
    fullName[fullName.length - 1] = entity.getName();
    return Namespace.of(fullName);
  }

  /**
   * Given the shortnames/ids of entities that all live under the given catalogPath, reconstructs
   * TableIdentifier objects for each that all hold the catalogPath excluding the catalog entity.
   */
  public static Namespace parentNamespace(List<PolarisEntity> catalogPath) {
    // Skip element 0 which is the catalog entity
    String[] parentNamespaces = new String[catalogPath.size() - 1];
    for (int i = 0; i < parentNamespaces.length; ++i) {
      parentNamespaces[i] = catalogPath.get(i + 1).getName();
    }
    return Namespace.of(parentNamespaces);
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
