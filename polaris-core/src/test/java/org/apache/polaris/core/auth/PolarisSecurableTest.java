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

import java.util.List;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.junit.jupiter.api.Test;

public class PolarisSecurableTest {

  @Test
  void topLevelSecurableHasLeafOnly() {
    PolarisSecurable principal =
        PolarisSecurable.of(List.of(new PathSegment(PolarisEntityType.PRINCIPAL, "principalA")));

    assertThat(principal.getLeaf())
        .isEqualTo(new PathSegment(PolarisEntityType.PRINCIPAL, "principalA"));
    assertThat(principal.getParents()).isEmpty();
    assertThat(principal.getPathSegments())
        .containsExactly(new PathSegment(PolarisEntityType.PRINCIPAL, "principalA"));
  }

  @Test
  void catalogScopedSecurableHasCorrectParentEntities() {
    PolarisSecurable table =
        PolarisSecurable.of(
            List.of(
                new PathSegment(PolarisEntityType.CATALOG, "catalogA"),
                new PathSegment(PolarisEntityType.NAMESPACE, "ns1"),
                new PathSegment(PolarisEntityType.TABLE_LIKE, "table1")));

    assertThat(table.getLeaf()).isEqualTo(new PathSegment(PolarisEntityType.TABLE_LIKE, "table1"));
    assertThat(table.getParents())
        .containsExactly(
            new PathSegment(PolarisEntityType.CATALOG, "catalogA"),
            new PathSegment(PolarisEntityType.NAMESPACE, "ns1"));
    assertThat(table.getPathSegments())
        .containsExactly(
            new PathSegment(PolarisEntityType.CATALOG, "catalogA"),
            new PathSegment(PolarisEntityType.NAMESPACE, "ns1"),
            new PathSegment(PolarisEntityType.TABLE_LIKE, "table1"));
  }

  @Test
  void securableAllowsNestedNamespaceLeaf() {
    PolarisSecurable nestedNamespace =
        PolarisSecurable.of(
            List.of(
                new PathSegment(PolarisEntityType.CATALOG, "catalogA"),
                new PathSegment(PolarisEntityType.NAMESPACE, "ns1"),
                new PathSegment(PolarisEntityType.NAMESPACE, "ns2")));

    assertThat(nestedNamespace.getLeaf())
        .isEqualTo(new PathSegment(PolarisEntityType.NAMESPACE, "ns2"));
    assertThat(nestedNamespace.getParents())
        .containsExactly(
            new PathSegment(PolarisEntityType.CATALOG, "catalogA"),
            new PathSegment(PolarisEntityType.NAMESPACE, "ns1"));
    assertThat(nestedNamespace.getPathSegments())
        .containsExactly(
            new PathSegment(PolarisEntityType.CATALOG, "catalogA"),
            new PathSegment(PolarisEntityType.NAMESPACE, "ns1"),
            new PathSegment(PolarisEntityType.NAMESPACE, "ns2"));
  }

  @Test
  void securableAllowsNestedNamespaces() {
    PolarisSecurable table =
        PolarisSecurable.of(
            List.of(
                new PathSegment(PolarisEntityType.CATALOG, "catalogA"),
                new PathSegment(PolarisEntityType.NAMESPACE, "ns1"),
                new PathSegment(PolarisEntityType.NAMESPACE, "ns2"),
                new PathSegment(PolarisEntityType.TABLE_LIKE, "table1")));

    assertThat(table.getLeaf()).isEqualTo(new PathSegment(PolarisEntityType.TABLE_LIKE, "table1"));
    assertThat(table.getParents())
        .containsExactly(
            new PathSegment(PolarisEntityType.CATALOG, "catalogA"),
            new PathSegment(PolarisEntityType.NAMESPACE, "ns1"),
            new PathSegment(PolarisEntityType.NAMESPACE, "ns2"));
  }

  @Test
  void securableRejectsInvalidInChainParentHierarchy() {
    assertThatThrownBy(
            () ->
                PolarisSecurable.of(
                    List.of(
                        new PathSegment(PolarisEntityType.CATALOG, "catalogA"),
                        new PathSegment(PolarisEntityType.TABLE_LIKE, "table1"),
                        new PathSegment(PolarisEntityType.NAMESPACE, "ns1"))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("must follow declared parent hierarchy");
  }

  @Test
  void securableRejectsInvalidParentHierarchy() {
    assertThatThrownBy(
            () ->
                PolarisSecurable.of(
                    List.of(
                        new PathSegment(PolarisEntityType.NAMESPACE, "ns1"),
                        new PathSegment(PolarisEntityType.TABLE_LIKE, "table1"))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("must start with a top-level entity");
  }

  @Test
  void topLevelSecurableRejectsParents() {
    assertThatThrownBy(
            () ->
                PolarisSecurable.of(
                    List.of(
                        new PathSegment(PolarisEntityType.CATALOG, "catalogParent"),
                        new PathSegment(PolarisEntityType.CATALOG, "catalogA"))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("must not declare parents");
  }
}
