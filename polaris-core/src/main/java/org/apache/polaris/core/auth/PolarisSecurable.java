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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;
import org.jspecify.annotations.NonNull;

/** Fully qualified resource path represented as ordered PathSegments. */
@PolarisImmutable
public interface PolarisSecurable {
  static PolarisSecurable of(@NonNull PathSegment leaf) {
    return ImmutablePolarisSecurable.builder().addPathSegment(leaf).build();
  }

  /**
   * Creates a securable from a full ordered path.
   *
   * <p>The segments must be ordered from the furthest parent segment to the leaf segment. For
   * example, a table path should be ordered as {@code CATALOG, NAMESPACE, TABLE_LIKE}.
   */
  static PolarisSecurable of(@NonNull PathSegment first, @NonNull PathSegment... rest) {
    return ImmutablePolarisSecurable.builder().addPathSegment(first).addPathSegments(rest).build();
  }

  /**
   * Returns the full ordered path from the highest parent segment to the leaf segment.
   *
   * <p>For example, a table path would be ordered as {@code [CATALOG, NAMESPACE, TABLE_LIKE]}.
   */
  @NonNull List<PathSegment> getPathSegments();

  /** Returns the leaf segment of the path. */
  @NonNull
  @Value.Derived
  default PathSegment getLeaf() {
    List<PathSegment> pathSegments = getPathSegments();
    Preconditions.checkState(
        !pathSegments.isEmpty(), "PathSegments must contain at least one segment");
    return pathSegments.get(pathSegments.size() - 1);
  }

  /** Returns ordered parent segments from furthest parent to immediate parent. */
  @NonNull
  @Value.Derived
  default List<PathSegment> getParents() {
    List<PathSegment> pathSegments = getPathSegments();
    Preconditions.checkState(
        !pathSegments.isEmpty(), "PathSegments must contain at least one segment");
    return pathSegments.subList(0, pathSegments.size() - 1);
  }

  /**
   * Returns a stable debug string for authorization messages.
   *
   * <p>For example, a table securable may render as {@code
   * CATALOG:catalog1.NAMESPACE:ns1.TABLE_LIKE:table1}.
   */
  @NonNull
  default String formatForAuthorizationMessage() {
    return getPathSegments().stream()
        .map(segment -> segment.entityType() + ":" + segment.name())
        .collect(Collectors.joining("."));
  }

  @Value.Check
  default void validate() {
    Preconditions.checkState(
        !getPathSegments().isEmpty(), "PathSegments must contain at least one segment");
    Preconditions.checkState(
        getPathSegments().get(0).entityType().isTopLevel(),
        "PathSegments must start with a top-level entity");
    for (PathSegment segment : getPathSegments()) {
      Preconditions.checkState(
          segment.entityType() != PolarisEntityType.ROOT, "PathSegments must not include ROOT");
    }
    if (getLeaf().entityType().isTopLevel()) {
      Preconditions.checkState(
          getParents().isEmpty(),
          "top-level securable leaf=%s must not declare parents",
          getLeaf());
    } else {
      for (int i = 1; i < getPathSegments().size(); i++) {
        PathSegment parent = getPathSegments().get(i - 1);
        PathSegment child = getPathSegments().get(i);
        Preconditions.checkState(
            child.entityType().getParentType() == parent.entityType()
                || (child.entityType().isParentSelfReference()
                    && child.entityType() == parent.entityType()),
            "PathSegments must follow declared parent hierarchy for child=%s",
            child);
      }
    }
  }
}
