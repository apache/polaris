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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.PathSegment;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.junit.jupiter.api.Test;

class PolarisSecurableMapperTest {

  @Test
  void tableLikeRejectsEmptyNamespace() {
    TableIdentifier identifier = TableIdentifier.of(Namespace.empty(), "table");

    assertThatThrownBy(() -> PolarisSecurableMapper.tableLike("catalog", identifier))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Table-like target cannot have an empty namespace");
  }

  @Test
  void tableLikeMapsNamespaceAndTableName() {
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("ns1", "ns2"), "table");

    assertThat(PolarisSecurableMapper.tableLike("catalog", identifier).getPathSegments())
        .containsExactly(
            new PathSegment(PolarisEntityType.CATALOG, "catalog"),
            new PathSegment(PolarisEntityType.NAMESPACE, "ns1"),
            new PathSegment(PolarisEntityType.NAMESPACE, "ns2"),
            new PathSegment(PolarisEntityType.TABLE_LIKE, "table"));
  }
}
