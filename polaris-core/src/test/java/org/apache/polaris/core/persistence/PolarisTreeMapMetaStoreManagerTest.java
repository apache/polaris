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

import static org.apache.polaris.core.persistence.PrincipalSecretsGenerator.RANDOM_SECRETS;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.LocationBasedEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.transactional.TransactionalMetaStoreManagerImpl;
import org.apache.polaris.core.persistence.transactional.TreeMapMetaStore;
import org.apache.polaris.core.persistence.transactional.TreeMapTransactionalPersistenceImpl;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class PolarisTreeMapMetaStoreManagerTest extends BasePolarisMetaStoreManagerTest {
  @Override
  public PolarisTestMetaStoreManager createPolarisTestMetaStoreManager() {
    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    TreeMapMetaStore store = new TreeMapMetaStore(diagServices);
    TreeMapTransactionalPersistenceImpl metaStore =
        new TreeMapTransactionalPersistenceImpl(
            diagServices, store, Mockito.mock(), RANDOM_SECRETS);
    TransactionalMetaStoreManagerImpl metaStoreManager =
        new TransactionalMetaStoreManagerImpl(clock, diagServices);
    PolarisCallContext callCtx = new PolarisCallContext(() -> "testRealm", metaStore);
    return new PolarisTestMetaStoreManager(metaStoreManager, callCtx);
  }

  @Test
  void hasOverlappingSiblingsIgnoresEntityItself() {
    var metaStoreManager = polarisTestMetaStoreManager.polarisMetaStoreManager();
    var callContext = polarisTestMetaStoreManager.polarisCallContext();
    PolarisBaseEntity catalog =
        new PolarisBaseEntity(
            PolarisEntityConstants.getNullId(),
            metaStoreManager.generateNewEntityId(callContext).getId(),
            PolarisEntityType.CATALOG,
            PolarisEntitySubType.NULL_SUBTYPE,
            PolarisEntityConstants.getRootEntityId(),
            "overlap_test_catalog");
    catalog = metaStoreManager.createCatalog(callContext, catalog, List.of()).getCatalog();

    PolarisBaseEntity existing =
        buildNamespace(catalog, metaStoreManager.generateNewEntityId(callContext).getId());
    metaStoreManager.createEntityIfNotExists(
        callContext, List.of(PolarisEntity.toCore(catalog)), existing);

    var allowedContainerIds = Set.of(catalog.getId());

    // A different entity at the same location is a conflict.
    var other = new LocationBasedTestEntity(buildNamespace(catalog, existing.getId() + 1));
    var conflict = metaStoreManager.hasOverlappingSiblings(callContext, other, allowedContainerIds);
    assertThat(conflict).isInstanceOf(LocationOverlapResult.Overlap.class);

    // The persisted entity itself, e.g. when renamed, is not a conflict.
    var self = new LocationBasedTestEntity(buildNamespace(catalog, existing.getId()));
    assertThat(metaStoreManager.hasOverlappingSiblings(callContext, self, allowedContainerIds))
        .isEqualTo(new LocationOverlapResult.NoOverlap());
  }

  private static PolarisBaseEntity buildNamespace(PolarisBaseEntity catalog, long entityId) {
    return new PolarisBaseEntity.Builder()
        .catalogId(catalog.getId())
        .parentId(catalog.getId())
        .id(entityId)
        .typeCode(PolarisEntityType.NAMESPACE.getCode())
        .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
        .name("existing")
        .propertiesAsMap(
            Map.of(PolarisEntityConstants.ENTITY_BASE_LOCATION, "s3://bucket/warehouse/existing/"))
        .internalPropertiesAsMap(Map.of())
        .build();
  }

  private static class LocationBasedTestEntity extends PolarisEntity
      implements LocationBasedEntity {
    LocationBasedTestEntity(PolarisBaseEntity sourceEntity) {
      super(sourceEntity);
    }

    @Override
    public String getBaseLocation() {
      return getPropertiesAsMap().get(PolarisEntityConstants.ENTITY_BASE_LOCATION);
    }
  }
}
