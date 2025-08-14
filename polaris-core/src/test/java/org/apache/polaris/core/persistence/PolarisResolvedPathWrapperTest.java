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
package org.apache.polaris.core.persistence;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.junit.jupiter.api.Test;

public class PolarisResolvedPathWrapperTest {

  @Test
  void testIsFullyResolvedNamespace_NullResolvedPath() {
    PolarisResolvedPathWrapper wrapper = new PolarisResolvedPathWrapper(null);

    assertFalse(wrapper.isFullyResolvedNamespace("test-catalog", Namespace.of("ns1")));
  }

  @Test
  void testIsFullyResolvedNamespace_InsufficientPathLength() {
    String catalogName = "test-catalog";
    Namespace namespace = Namespace.of("ns1", "ns2");

    PolarisEntity catalogEntity = createEntity(catalogName, PolarisEntityType.CATALOG);
    List<ResolvedPolarisEntity> shortPath = List.of(createResolvedEntity(catalogEntity));
    PolarisResolvedPathWrapper wrapper = new PolarisResolvedPathWrapper(shortPath);

    assertFalse(wrapper.isFullyResolvedNamespace(catalogName, namespace));
  }

  @Test
  void testIsFullyResolvedNamespace_WrongCatalogName() {
    String catalogName = "test-catalog";
    String wrongCatalogName = "wrong-catalog";
    Namespace namespace = Namespace.of("ns1");

    PolarisEntity wrongCatalogEntity = createEntity(wrongCatalogName, PolarisEntityType.CATALOG);
    PolarisEntity namespaceEntity = createEntity("ns1", PolarisEntityType.NAMESPACE);
    List<ResolvedPolarisEntity> path =
        List.of(createResolvedEntity(wrongCatalogEntity), createResolvedEntity(namespaceEntity));
    PolarisResolvedPathWrapper wrapper = new PolarisResolvedPathWrapper(path);

    assertFalse(wrapper.isFullyResolvedNamespace(catalogName, namespace));
  }

  @Test
  void testIsFullyResolvedNamespace_WrongNamespaceNames() {
    String catalogName = "test-catalog";
    Namespace namespace = Namespace.of("ns1", "ns2");

    PolarisEntity catalogEntity = createEntity(catalogName, PolarisEntityType.CATALOG);
    PolarisEntity namespace1Entity = createEntity("ns1", PolarisEntityType.NAMESPACE);
    PolarisEntity namespace2WrongEntity = createEntity("wrong-ns2", PolarisEntityType.NAMESPACE);
    List<ResolvedPolarisEntity> path =
        List.of(
            createResolvedEntity(catalogEntity),
            createResolvedEntity(namespace1Entity),
            createResolvedEntity(namespace2WrongEntity));
    PolarisResolvedPathWrapper wrapper = new PolarisResolvedPathWrapper(path);

    assertFalse(wrapper.isFullyResolvedNamespace(catalogName, namespace));
  }

  @Test
  void testIsFullyResolvedNamespace_CorrectPath() {
    String catalogName = "test-catalog";
    Namespace namespace = Namespace.of("ns1", "ns2");

    PolarisEntity catalogEntity = createEntity(catalogName, PolarisEntityType.CATALOG);
    PolarisEntity namespace1Entity = createEntity("ns1", PolarisEntityType.NAMESPACE);
    PolarisEntity namespace2Entity = createEntity("ns2", PolarisEntityType.NAMESPACE);
    List<ResolvedPolarisEntity> path =
        List.of(
            createResolvedEntity(catalogEntity),
            createResolvedEntity(namespace1Entity),
            createResolvedEntity(namespace2Entity));
    PolarisResolvedPathWrapper wrapper = new PolarisResolvedPathWrapper(path);

    assertTrue(wrapper.isFullyResolvedNamespace(catalogName, namespace));
  }

  @Test
  void testIsFullyResolvedNamespace_WrongEntityType() {
    String catalogName = "test-catalog";
    Namespace namespace = Namespace.of("ns1", "ns2");

    PolarisEntity catalogEntity = createEntity(catalogName, PolarisEntityType.CATALOG);
    PolarisEntity correctEntityType = createEntity("ns1", PolarisEntityType.NAMESPACE);
    PolarisEntity wrongEntityType = createEntity("ns2", PolarisEntityType.TABLE_LIKE);
    List<ResolvedPolarisEntity> path =
        List.of(
            createResolvedEntity(catalogEntity),
            createResolvedEntity(correctEntityType),
            createResolvedEntity(wrongEntityType));
    PolarisResolvedPathWrapper wrapper = new PolarisResolvedPathWrapper(path);

    assertFalse(wrapper.isFullyResolvedNamespace(catalogName, namespace));
  }

  @Test
  void testIsFullyResolvedNamespace_EmptyNamespace() {
    String catalogName = "test-catalog";
    Namespace namespace = Namespace.empty();

    PolarisEntity catalogEntity = createEntity(catalogName, PolarisEntityType.CATALOG);
    List<ResolvedPolarisEntity> path = List.of(createResolvedEntity(catalogEntity));
    PolarisResolvedPathWrapper wrapper = new PolarisResolvedPathWrapper(path);

    assertTrue(wrapper.isFullyResolvedNamespace(catalogName, namespace));
  }

  private PolarisEntity createEntity(String name, PolarisEntityType type) {
    return new PolarisEntity.Builder()
        .setName(name)
        .setType(type)
        .setId(1L)
        .setCatalogId(1L)
        .setParentId(1L)
        .setCreateTimestamp(System.currentTimeMillis())
        .build();
  }

  private ResolvedPolarisEntity createResolvedEntity(PolarisEntity entity) {
    return new ResolvedPolarisEntity(entity, null, null);
  }
}
