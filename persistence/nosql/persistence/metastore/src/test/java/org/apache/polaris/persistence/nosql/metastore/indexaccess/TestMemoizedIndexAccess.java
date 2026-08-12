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

package org.apache.polaris.persistence.nosql.metastore.indexaccess;

import static org.apache.polaris.core.entity.PolarisEntityConstants.getNullId;
import static org.apache.polaris.core.entity.PolarisEntityConstants.getRootEntityId;
import static org.apache.polaris.core.entity.PolarisEntitySubType.NULL_SUBTYPE;
import static org.apache.polaris.core.entity.PolarisEntityType.CATALOG;
import static org.apache.polaris.core.entity.PolarisEntityType.CATALOG_ROLE;
import static org.apache.polaris.core.entity.PolarisEntityType.NAMESPACE;
import static org.apache.polaris.core.entity.PolarisEntityType.POLICY;
import static org.apache.polaris.core.entity.PolarisEntityType.PRINCIPAL;
import static org.apache.polaris.core.entity.PolarisEntityType.PRINCIPAL_ROLE;
import static org.apache.polaris.core.entity.PolarisEntityType.TABLE_LIKE;
import static org.apache.polaris.core.entity.PolarisEntityType.TASK;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings.byEntityType;

import io.smallrye.common.annotation.Identifier;
import jakarta.inject.Inject;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.RealmPersistenceFactory;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.jboss.weld.junit5.EnableWeld;
import org.jboss.weld.junit5.WeldInitiator;
import org.jboss.weld.junit5.WeldSetup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@SuppressWarnings("CdiInjectionPointsInspection")
@ExtendWith(SoftAssertionsExtension.class)
@EnableWeld
public class TestMemoizedIndexAccess {
  @InjectSoftAssertions public SoftAssertions soft;

  @WeldSetup WeldInitiator weld = WeldInitiator.performDefaultDiscovery();

  @Inject RealmPersistenceFactory realmPersistenceFactory;

  @Inject
  @Identifier("nosql")
  MetaStoreManagerFactory metaStoreManagerFactory;

  PolarisCallContext callContext;
  PolarisMetaStoreManager metaStoreManager;

  Persistence persistence;
  MemoizedIndexedAccess memoized;

  @BeforeEach
  public void setup(TestInfo testInfo) {
    var realmId = testInfo.getTestMethod().orElseThrow().getName();
    persistence = realmPersistenceFactory.newBuilder().realmId(realmId).build();

    metaStoreManagerFactory.bootstrapRealms(List.of(realmId), RootCredentialsSet.EMPTY);

    var realmContext = (RealmContext) () -> realmId;
    callContext =
        new PolarisCallContext(
            realmContext, metaStoreManagerFactory.getOrCreateSession(realmContext));
    metaStoreManager = metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);

    memoized = MemoizedIndexedAccess.newMemoizedIndexedAccess(persistence);
  }

  @ParameterizedTest
  @MethodSource("realmScopedEntityTypes")
  public void realmScoped(PolarisEntityType entityType, PolarisEntitySubType subType) {
    checkType(0L, List.of(), entityType, subType);
  }

  @ParameterizedTest
  @MethodSource("catalogScopedEntityTypes")
  public void catalogScoped(PolarisEntityType entityType, PolarisEntitySubType subType) {
    var catalogId = persistence.generateId();
    var catalogEntity =
        new PolarisBaseEntity(
            getNullId(), catalogId, CATALOG, NULL_SUBTYPE, getRootEntityId(), "my-catalog");
    metaStoreManager.createCatalog(callContext, catalogEntity, List.of());

    checkType(catalogId, List.of(catalogEntity), entityType, subType);
  }

  private void checkType(
      long catalogId,
      List<PolarisEntityCore> catalogPath,
      PolarisEntityType entityType,
      PolarisEntitySubType subType) {
    var entityName = "entity-for-" + entityType.name();
    long entityId = persistence.generateId();
    var entityForType =
        new PolarisBaseEntity(
            catalogId, entityId, entityType, subType, getRootEntityId(), entityName);

    if (entityType == PRINCIPAL) {
      metaStoreManager.createPrincipal(callContext, new PrincipalEntity(entityForType));
    } else {
      metaStoreManager.createEntityIfNotExists(callContext, catalogPath, entityForType);
    }

    var ia = memoized.indexedAccess(catalogId, entityType.getCode());
    soft.assertThat(ia).isInstanceOf(IndexedContainerAccessImpl.class);
    soft.assertThat(memoized.indexedAccess(catalogId, entityType.getCode())).isSameAs(ia);

    var mapping = byEntityType(entityType);
    var refName = mapping.refNameForCatalog(catalogId);
    var referenceHead = memoized.referenceHead(refName, mapping.containerObjTypeClass());
    soft.assertThat(referenceHead).isNotEmpty().get().isInstanceOf(mapping.containerObjTypeClass());

    // "populate" the ICA's data
    soft.assertThat(ia.byNameOnRoot(entityName)).isNotEmpty();

    var entityName2 = "entity2-for-" + entityType.name();
    long entityId2 = persistence.generateId();
    var entityForType2 =
        new PolarisBaseEntity(
            catalogId, entityId2, entityType, subType, getRootEntityId(), entityName2);
    if (entityType == PRINCIPAL) {
      metaStoreManager.createPrincipal(callContext, new PrincipalEntity(entityForType2));
    } else {
      metaStoreManager.createEntityIfNotExists(callContext, catalogPath, entityForType2);
    }

    soft.assertThat(ia.byNameOnRoot(entityName)).isNotEmpty();
    soft.assertThat(ia.byNameOnRoot(entityName2)).isEmpty();

    var ia2 = memoized.indexedAccess(catalogId, entityType.getCode());
    // Memoized index detects reference-pointer bump.
    soft.assertThat(ia2).isNotSameAs(ia);
    soft.assertThat(memoized.indexedAccess(catalogId, entityType.getCode())).isSameAs(ia2);

    memoized.invalidateIndexedAccess(catalogId, entityType.getCode());

    var ia3 = memoized.indexedAccess(catalogId, entityType.getCode());
    soft.assertThat(ia3.byNameOnRoot(entityName)).isNotEmpty();
    soft.assertThat(ia3.byNameOnRoot(entityName2)).isNotEmpty();

    // reference head changed
    soft.assertThat(persistence.fetchReferenceHead(refName, mapping.containerObjTypeClass()))
        .isNotEqualTo(referenceHead);
    // memoized value should have refreshed
    soft.assertThat(memoized.referenceHead(refName, mapping.containerObjTypeClass()))
        .isNotEqualTo(referenceHead);
    memoized.invalidateReferenceHead(refName);
    soft.assertThat(memoized.referenceHead(refName, mapping.containerObjTypeClass()))
        .get()
        .isNotEqualTo(referenceHead);
  }

  static Stream<Arguments> realmScopedEntityTypes() {
    return Stream.of(CATALOG, PRINCIPAL, PRINCIPAL_ROLE, TASK)
        .flatMap(
            t -> {
              var subTypes =
                  Arrays.stream(PolarisEntitySubType.values())
                      .filter(s -> s.getParentType() == t)
                      .toList();

              return subTypes.isEmpty()
                  ? Stream.of(Arguments.of(t, NULL_SUBTYPE))
                  : subTypes.stream().map(s -> Arguments.of(t, s));
            });
  }

  static Stream<Arguments> catalogScopedEntityTypes() {
    return Stream.of(CATALOG_ROLE, NAMESPACE, TABLE_LIKE, POLICY)
        .flatMap(
            t -> {
              var subTypes =
                  Arrays.stream(PolarisEntitySubType.values())
                      .filter(s -> s.getParentType() == t)
                      .toList();

              return subTypes.isEmpty()
                  ? Stream.of(Arguments.of(t, NULL_SUBTYPE))
                  : subTypes.stream().map(s -> Arguments.of(t, s));
            });
  }
}
