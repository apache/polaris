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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.junit.jupiter.api.Test;

public class FullyQualifiedPathTest {

  @Test
  void fullyQualifiedPathPrependsCatalogForCatalogScopedTypes() {
    PolarisSecurable table =
        PolarisSecurable.of(PolarisEntityType.TABLE_LIKE, List.of("ns1", "table1"));

    FullyQualifiedPath path = FullyQualifiedPath.of(table, "catalogA");

    assertThat(path.leaf()).isEqualTo(new PathSegment(PolarisEntityType.TABLE_LIKE, "table1"));
    assertThat(path.parents())
        .containsExactly(
            new PathSegment(PolarisEntityType.CATALOG, "catalogA"),
            new PathSegment(PolarisEntityType.NAMESPACE, "ns1"));
  }

  @Test
  void fullyQualifiedPathRejectsMissingCatalogForCatalogScopedTypes() {
    PolarisSecurable table =
        PolarisSecurable.of(PolarisEntityType.TABLE_LIKE, List.of("ns1", "table1"));

    assertThatThrownBy(() -> FullyQualifiedPath.of(table, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("referenceCatalogName must be non-empty");
    assertThatThrownBy(() -> FullyQualifiedPath.of(table, "  "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("referenceCatalogName must be non-empty");
  }

  @Test
  void fullyQualifiedPathLeavesParentsEmptyForTopLevelTypes() {
    PolarisSecurable principal =
        PolarisSecurable.of(PolarisEntityType.PRINCIPAL, List.of("principalA"));

    FullyQualifiedPath path = FullyQualifiedPath.of(principal, null);

    assertThat(path.leaf().entityType()).isEqualTo(PolarisEntityType.PRINCIPAL);
    assertThat(path.leaf().name()).isEqualTo("principalA");
    assertThat(path.parents()).isEmpty();
  }

  @Test
  void fullyQualifiedPathSkipsCatalogForNonCatalogScopedSecurables() {
    PolarisSecurable principal =
        PolarisSecurable.of(PolarisEntityType.PRINCIPAL, List.of("principalA"));

    FullyQualifiedPath path = FullyQualifiedPath.of(principal, "catalogA");

    assertThat(path.leaf()).isEqualTo(new PathSegment(PolarisEntityType.PRINCIPAL, "principalA"));

    // catalog is not included for top level entities
    assertThat(path.parents()).isEmpty();
  }

  @Test
  void fullyQualifiedPathPreservesResolvedParentOrdering() {
    PolarisResolvedPathWrapper wrapper =
        new PolarisResolvedPathWrapper(
            List.of(
                resolvedEntity(PolarisEntityType.CATALOG, "catalogA"),
                resolvedEntity(PolarisEntityType.NAMESPACE, "ns1"),
                resolvedEntity(PolarisEntityType.TABLE_LIKE, "table1")));

    FullyQualifiedPath path = FullyQualifiedPath.of(wrapper);

    assertThat(path.leaf()).isEqualTo(new PathSegment(PolarisEntityType.TABLE_LIKE, "table1"));
    assertThat(path.parents())
        .containsExactly(
            new PathSegment(PolarisEntityType.CATALOG, "catalogA"),
            new PathSegment(PolarisEntityType.NAMESPACE, "ns1"));
  }

  @Test
  void fullyQualifiedPathDropsRootFromResolvedParents() {
    PolarisResolvedPathWrapper wrapper =
        new PolarisResolvedPathWrapper(
            List.of(
                resolvedEntity(PolarisEntityType.ROOT, "root"),
                resolvedEntity(PolarisEntityType.CATALOG, "catalogA")));

    FullyQualifiedPath path = FullyQualifiedPath.of(wrapper);

    assertThat(path.leaf()).isEqualTo(new PathSegment(PolarisEntityType.CATALOG, "catalogA"));
    assertThat(path.parents()).isEmpty();
  }

  private ResolvedPolarisEntity resolvedEntity(PolarisEntityType type, String name) {
    var entity = mock(org.apache.polaris.core.entity.PolarisEntity.class);
    when(entity.getType()).thenReturn(type);
    when(entity.getName()).thenReturn(name);
    return new ResolvedPolarisEntity(entity, List.of(), List.of());
  }
}
