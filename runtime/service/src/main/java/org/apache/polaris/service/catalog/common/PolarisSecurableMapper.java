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
package org.apache.polaris.service.catalog.common;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.ImmutablePolarisSecurable;
import org.apache.polaris.core.auth.PathSegment;
import org.apache.polaris.core.auth.PolarisSecurable;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.service.types.PolicyAttachmentTarget;
import org.apache.polaris.service.types.PolicyIdentifier;

/**
 * Utility mapper for translating Polaris domain identifiers into canonical {@link
 * org.apache.polaris.core.auth.PolarisSecurable} paths.
 */
public final class PolarisSecurableMapper {
  private PolarisSecurableMapper() {}

  public static PolarisSecurable catalog(String catalogName) {
    return PolarisSecurable.of(new PathSegment(PolarisEntityType.CATALOG, catalogName));
  }

  public static PolarisSecurable catalogRole(String catalogName, String catalogRoleName) {
    return ImmutablePolarisSecurable.builder()
        .addPathSegment(new PathSegment(PolarisEntityType.CATALOG, catalogName))
        .addPathSegment(new PathSegment(PolarisEntityType.CATALOG_ROLE, catalogRoleName))
        .build();
  }

  public static PolarisSecurable namespace(String catalogName, Namespace namespace) {
    if (namespace.isEmpty()) {
      throw new IllegalArgumentException("Namespace target must not be empty");
    }
    ImmutablePolarisSecurable.Builder builder =
        ImmutablePolarisSecurable.builder()
            .addPathSegment(new PathSegment(PolarisEntityType.CATALOG, catalogName));
    Arrays.stream(namespace.levels())
        .map(level -> new PathSegment(PolarisEntityType.NAMESPACE, level))
        .forEach(builder::addPathSegment);
    return builder.build();
  }

  public static PolarisSecurable tableLike(String catalogName, TableIdentifier identifier) {
    if (identifier.namespace().isEmpty()) {
      throw new IllegalArgumentException("Table-like target cannot have an empty namespace");
    }
    ImmutablePolarisSecurable.Builder builder =
        ImmutablePolarisSecurable.builder()
            .addPathSegment(new PathSegment(PolarisEntityType.CATALOG, catalogName));
    Arrays.stream(identifier.namespace().levels())
        .map(level -> new PathSegment(PolarisEntityType.NAMESPACE, level))
        .forEach(builder::addPathSegment);
    return builder
        .addPathSegment(new PathSegment(PolarisEntityType.TABLE_LIKE, identifier.name()))
        .build();
  }

  public static PolarisSecurable policy(String catalogName, PolicyIdentifier identifier) {
    ImmutablePolarisSecurable.Builder builder =
        ImmutablePolarisSecurable.builder()
            .addPathSegment(new PathSegment(PolarisEntityType.CATALOG, catalogName));
    Arrays.stream(identifier.namespace().levels())
        .map(level -> new PathSegment(PolarisEntityType.NAMESPACE, level))
        .forEach(builder::addPathSegment);
    return builder
        .addPathSegment(new PathSegment(PolarisEntityType.POLICY, identifier.name()))
        .build();
  }

  public static PolarisSecurable policyAttachmentTarget(
      String catalogName, PolicyAttachmentTarget target) {
    ImmutablePolarisSecurable.Builder builder =
        ImmutablePolarisSecurable.builder()
            .addPathSegment(new PathSegment(PolarisEntityType.CATALOG, catalogName));
    switch (target.getType()) {
      case CATALOG:
        return catalog(catalogName);
      case NAMESPACE:
        if (target.getPath().isEmpty()) {
          throw new IllegalArgumentException("Namespace target path must not be empty");
        }
        target.getPath().stream()
            .map(level -> new PathSegment(PolarisEntityType.NAMESPACE, level))
            .forEach(builder::addPathSegment);
        return builder.build();
      case TABLE_LIKE:
        List<String> path = target.getPath();
        path.subList(0, path.size() - 1).stream()
            .map(level -> new PathSegment(PolarisEntityType.NAMESPACE, level))
            .forEach(builder::addPathSegment);
        return builder
            .addPathSegment(
                new PathSegment(PolarisEntityType.TABLE_LIKE, path.get(path.size() - 1)))
            .build();
      default:
        throw new IllegalArgumentException("Unsupported target type: " + target.getType());
    }
  }
}
