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
package org.apache.polaris.core.auth;

import com.google.common.base.Preconditions;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;

/** Lexical fully qualified resource path split into leaf and ordered parent segments. */
public record FullyQualifiedPath(PathSegment leaf, List<PathSegment> parents) {

  public FullyQualifiedPath {
    Preconditions.checkNotNull(leaf, "leaf");
    Preconditions.checkNotNull(parents, "parents");
    parents = List.copyOf(parents);
  }

  public static FullyQualifiedPath of(
      PolarisSecurable securable, @Nullable String referenceCatalogName) {
    List<PathSegment> namePartSegments = namePartSegments(securable);
    Preconditions.checkState(
        !namePartSegments.isEmpty(),
        "FullyQualifiedPath produced no segments for entityType=%s nameParts=%s",
        securable.getEntityType(),
        securable.getNameParts());

    int leafIndex = namePartSegments.size() - 1;
    PathSegment leaf = namePartSegments.get(leafIndex);
    List<PathSegment> parents = new ArrayList<>();
    if (isCatalogScopedType(securable.getEntityType())) {
      Preconditions.checkArgument(
          referenceCatalogName != null && !referenceCatalogName.isBlank(),
          "referenceCatalogName must be non-empty for catalog-scoped securable entityType=%s nameParts=%s",
          securable.getEntityType(),
          securable.getNameParts());
      parents.add(new PathSegment(PolarisEntityType.CATALOG, referenceCatalogName));
    }
    parents.addAll(namePartSegments.subList(0, leafIndex));
    return new FullyQualifiedPath(leaf, parents);
  }

  /**
   * Temporary adapter used while OPA serialization still accepts resolved paths alongside intent
   * paths. Remove this overload when resolved-path callers are retired.
   */
  public static FullyQualifiedPath of(PolarisResolvedPathWrapper wrapper) {
    Preconditions.checkNotNull(wrapper, "wrapper");
    ResolvedPolarisEntity resolvedLeaf = wrapper.getResolvedLeafEntity();
    Preconditions.checkNotNull(resolvedLeaf, "wrapper.getResolvedLeafEntity()");

    PathSegment leaf =
        new PathSegment(resolvedLeaf.getEntity().getType(), resolvedLeaf.getEntity().getName());
    List<PathSegment> parents = new ArrayList<>();
    List<ResolvedPolarisEntity> resolvedParents = wrapper.getResolvedParentPath();
    if (resolvedParents != null) {
      for (ResolvedPolarisEntity resolvedParent : resolvedParents) {
        // Omit ROOT from fully qualified paths. It is an internal RBAC-only ancestor
        if (resolvedParent.getEntity().getType() == PolarisEntityType.ROOT) {
          continue;
        }
        parents.add(
            new PathSegment(
                resolvedParent.getEntity().getType(), resolvedParent.getEntity().getName()));
      }
    }

    return new FullyQualifiedPath(leaf, parents);
  }

  private static List<PathSegment> namePartSegments(PolarisSecurable securable) {
    List<String> nameParts = securable.getNameParts();
    List<PathSegment> segments = new ArrayList<>(nameParts.size());
    for (int i = 0; i < nameParts.size(); i++) {
      PolarisEntityType segmentType =
          i == nameParts.size() - 1 ? securable.getEntityType() : PolarisEntityType.NAMESPACE;
      segments.add(new PathSegment(segmentType, nameParts.get(i)));
    }
    return segments;
  }

  private static boolean isCatalogScopedType(PolarisEntityType entityType) {
    PolarisEntityType parent = entityType.getParentType();
    while (parent != null) {
      if (parent == PolarisEntityType.CATALOG) {
        return true;
      }
      parent = parent.getParentType();
    }
    return false;
  }
}
