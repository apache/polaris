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
import static org.apache.polaris.core.entity.PolarisEntityConstants.getRootContainerName;
import static org.apache.polaris.core.entity.PolarisEntityConstants.getRootEntityId;
import static org.apache.polaris.core.entity.PolarisEntitySubType.NULL_SUBTYPE;
import static org.apache.polaris.core.entity.PolarisEntityType.CATALOG;
import static org.apache.polaris.core.entity.PolarisEntityType.CATALOG_ROLE;
import static org.apache.polaris.core.entity.PolarisEntityType.NAMESPACE;
import static org.apache.polaris.core.entity.PolarisEntityType.POLICY;
import static org.apache.polaris.core.entity.PolarisEntityType.PRINCIPAL;
import static org.apache.polaris.core.entity.PolarisEntityType.PRINCIPAL_ROLE;
import static org.apache.polaris.core.entity.PolarisEntityType.ROOT;
import static org.apache.polaris.core.entity.PolarisEntityType.TABLE_LIKE;
import static org.apache.polaris.core.entity.PolarisEntityType.TASK;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings.byEntityType;
import static org.apache.polaris.persistence.nosql.metastore.indexaccess.IndexedContainerAccess.indexedAccessDirect;
import static org.apache.polaris.persistence.nosql.metastore.indexaccess.IndexedContainerAccess.indexedAccessForEntityType;

import io.smallrye.common.annotation.Identifier;
import jakarta.inject.Inject;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.RealmPersistenceFactory;
import org.apache.polaris.persistence.nosql.coretypes.realm.RootObj;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.jboss.weld.junit5.EnableWeld;
import org.jboss.weld.junit5.WeldInitiator;
import org.jboss.weld.junit5.WeldSetup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@SuppressWarnings("CdiInjectionPointsInspection")
@ExtendWith(SoftAssertionsExtension.class)
@EnableWeld
public class TestIndexedContainerAccess {
  @InjectSoftAssertions public SoftAssertions soft;

  @WeldSetup WeldInitiator weld = WeldInitiator.performDefaultDiscovery();

  @Inject RealmPersistenceFactory realmPersistenceFactory;

  @Inject
  @Identifier("nosql")
  MetaStoreManagerFactory metaStoreManagerFactory;

  PolarisCallContext callContext;
  PolarisMetaStoreManager metaStoreManager;

  Persistence persistence;

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
  }

  @Test
  public void indexedAccessForRoot() {
    var ia = indexedAccessForEntityType(ROOT.getCode(), persistence, 0L);

    soft.assertThat(ia).isInstanceOf(IndexedContainerAccessRoot.class);
    soft.assertThat(ia.catalogStableId()).isEqualTo(0L);
    soft.assertThat(ia.nameIndex()).isNotEmpty();
    soft.assertThat(ia.stableIdIndex()).isNotEmpty();
    soft.assertThat(ia.byNameOnRoot(getRootContainerName())).get().isInstanceOf(RootObj.class);
    soft.assertThat(ia.byNameOnRoot("foo")).isEmpty();
    soft.assertThat(ia.byParentIdAndName(0L, getRootContainerName()))
        .get()
        .isInstanceOf(RootObj.class);
    soft.assertThat(ia.byParentIdAndName(42L, getRootContainerName())).isEmpty();
    soft.assertThat(ia.byParentIdAndName(0L, "foo")).isEmpty();
    soft.assertThat(ia.byId(0L)).get().isInstanceOf(RootObj.class);
    soft.assertThat(ia.byId(42L)).isEmpty();
    soft.assertThatThrownBy(ia::refObj).isInstanceOf(UnsupportedOperationException.class);
  }

  @ParameterizedTest
  @MethodSource("realmScopedEntityTypes")
  public void indexedAccessForRealmScopedTypes(
      PolarisEntityType entityType, PolarisEntitySubType subType) {
    // Accessing a catalog SHOULD lead to an empty index.
    // See org.apache.polaris.persistence.nosql.coretypes.mapping.BaseMapping.fixCatalogId():
    // ... some tests use a non-0 catalog ID for non-catalog entity types.
    //  soft.assertThatIllegalArgumentException()
    //    .isThrownBy(() ->
    //      IndexedContainerAccess.indexedAccessForEntityType(
    //        entityType.getCode(), persistence, catalogId));

    // Accessing no-catalog gives the implementation.
    var ia = indexedAccessForEntityType(entityType.getCode(), persistence, 0L);
    soft.assertThat(ia).isInstanceOf(IndexedContainerAccessImpl.class);
    soft.assertThat(ia.catalogStableId()).isEqualTo(0L);

    var entityName = "entity-for-" + entityType.name();
    long entityId = persistence.generateId();
    var entityForType =
        new PolarisBaseEntity(
            getNullId(), entityId, entityType, subType, getRootEntityId(), entityName);
    if (entityType == PRINCIPAL) {
      metaStoreManager.createPrincipal(callContext, new PrincipalEntity(entityForType));
    } else {
      metaStoreManager.createEntityIfNotExists(callContext, List.of(), entityForType);
    }

    var mapping = byEntityType(entityType);
    var typeClass = mapping.objTypeForSubType(subType).targetClass();

    soft.assertThat(ia).isInstanceOf(IndexedContainerAccessImpl.class);
    soft.assertThat(ia.catalogStableId()).isEqualTo(0L);
    soft.assertThat(ia.nameIndex()).isNotEmpty();
    soft.assertThat(ia.stableIdIndex()).isNotEmpty();
    soft.assertThat(ia.byNameOnRoot(entityName)).get().isInstanceOf(typeClass);
    soft.assertThat(ia.byNameOnRoot("foo")).isEmpty();
    soft.assertThat(ia.byParentIdAndName(0L, entityName)).get().isInstanceOf(typeClass);
    soft.assertThat(ia.byParentIdAndName(42L, entityName)).isEmpty();
    soft.assertThat(ia.byParentIdAndName(0L, "foo")).isEmpty();
    soft.assertThat(ia.byId(entityId)).get().isInstanceOf(typeClass);
    soft.assertThat(ia.byId(42L)).isEmpty();
    soft.assertThat(ia.refObj()).get().isInstanceOf(mapping.containerObjTypeClass());

    // verify via indexedAccessDirect()
    ia = indexedAccessDirect(persistence, objRef(ia.refObj().orElseThrow()));
    soft.assertThat(ia).isInstanceOf(IndexedContainerAccessImpl.class);
    soft.assertThat(ia.catalogStableId()).isEqualTo(-1L);
    soft.assertThat(ia.nameIndex()).isNotEmpty();
    soft.assertThat(ia.stableIdIndex()).isNotEmpty();
    soft.assertThat(ia.byNameOnRoot(entityName)).get().isInstanceOf(typeClass);
    soft.assertThat(ia.byNameOnRoot("foo")).isEmpty();
    soft.assertThat(ia.byParentIdAndName(0L, entityName)).get().isInstanceOf(typeClass);
    soft.assertThat(ia.byParentIdAndName(42L, entityName)).isEmpty();
    soft.assertThat(ia.byParentIdAndName(0L, "foo")).isEmpty();
    soft.assertThat(ia.byId(entityId)).get().isInstanceOf(typeClass);
    soft.assertThat(ia.byId(42L)).isEmpty();
    soft.assertThat(ia.refObj()).get().isInstanceOf(mapping.containerObjTypeClass());
  }

  @ParameterizedTest
  @MethodSource("catalogScopedEntityTypes")
  public void indexedAccessForCatalogScopedTypes(
      PolarisEntityType entityType, PolarisEntitySubType subType) {

    // Accessing no-catalog must lead to an exception.
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> indexedAccessForEntityType(entityType.getCode(), persistence, 0L));

    var catalogId = persistence.generateId();
    var catalogEntity =
        new PolarisBaseEntity(
            getNullId(), catalogId, CATALOG, NULL_SUBTYPE, getRootEntityId(), "my-catalog");
    metaStoreManager.createCatalog(callContext, catalogEntity, List.of());

    var entityName = "entity-for-" + entityType.name();
    long entityId = persistence.generateId();
    var entityForType =
        new PolarisBaseEntity(
            catalogId, entityId, entityType, subType, getRootEntityId(), entityName);
    metaStoreManager.createEntityIfNotExists(callContext, List.of(catalogEntity), entityForType);

    var mapping = byEntityType(entityType);
    var typeClass = mapping.objTypeForSubType(subType).targetClass();

    // Accessing a catalog gives the implementation.
    var ia = indexedAccessForEntityType(entityType.getCode(), persistence, catalogId);
    soft.assertThat(ia).isInstanceOf(IndexedContainerAccessImpl.class);
    soft.assertThat(ia.catalogStableId()).isEqualTo(catalogId);
    soft.assertThat(ia.nameIndex()).isNotEmpty();
    soft.assertThat(ia.stableIdIndex()).isNotEmpty();
    soft.assertThat(ia.byNameOnRoot(entityName)).get().isInstanceOf(typeClass);
    soft.assertThat(ia.byNameOnRoot("foo")).isEmpty();
    soft.assertThat(ia.byParentIdAndName(0L, entityName)).get().isInstanceOf(typeClass);
    soft.assertThat(ia.byParentIdAndName(42L, entityName)).isEmpty();
    soft.assertThat(ia.byParentIdAndName(0L, "foo")).isEmpty();
    soft.assertThat(ia.byId(entityId)).get().isInstanceOf(typeClass);
    soft.assertThat(ia.byId(42L)).isEmpty();
    soft.assertThat(ia.refObj()).get().isInstanceOf(mapping.containerObjTypeClass());

    // verify via indexedAccessDirect()
    ia = indexedAccessDirect(persistence, objRef(ia.refObj().orElseThrow()));
    soft.assertThat(ia).isInstanceOf(IndexedContainerAccessImpl.class);
    soft.assertThat(ia.catalogStableId()).isEqualTo(-1L);
    soft.assertThat(ia.nameIndex()).isNotEmpty();
    soft.assertThat(ia.stableIdIndex()).isNotEmpty();
    soft.assertThat(ia.byNameOnRoot(entityName)).get().isInstanceOf(typeClass);
    soft.assertThat(ia.byNameOnRoot("foo")).isEmpty();
    soft.assertThat(ia.byParentIdAndName(0L, entityName)).get().isInstanceOf(typeClass);
    soft.assertThat(ia.byParentIdAndName(42L, entityName)).isEmpty();
    soft.assertThat(ia.byParentIdAndName(0L, "foo")).isEmpty();
    soft.assertThat(ia.byId(entityId)).get().isInstanceOf(typeClass);
    soft.assertThat(ia.byId(42L)).isEmpty();
    soft.assertThat(ia.refObj()).get().isInstanceOf(mapping.containerObjTypeClass());

    if (mapping.catalogContent()) {

      var namespaceName = "namespace-for-" + entityType.name();
      long namespaceId = persistence.generateId();
      var namespace =
          new PolarisBaseEntity(
              catalogId, namespaceId, NAMESPACE, NULL_SUBTYPE, getRootEntityId(), namespaceName);
      metaStoreManager.createEntityIfNotExists(callContext, List.of(catalogEntity), namespace);

      var subName = "sub-for-" + entityType.name();
      long subId = persistence.generateId();
      var sub = new PolarisBaseEntity(catalogId, subId, entityType, subType, namespaceId, subName);
      metaStoreManager.createEntityIfNotExists(callContext, List.of(catalogEntity, namespace), sub);

      // Get a new ICA as the previous one has cached the index.
      ia = indexedAccessForEntityType(entityType.getCode(), persistence, catalogId);
      soft.assertThat(ia).isInstanceOf(IndexedContainerAccessImpl.class);
      soft.assertThat(ia.catalogStableId()).isEqualTo(catalogId);
      soft.assertThat(ia.nameIndex()).isNotEmpty();
      soft.assertThat(ia.stableIdIndex()).isNotEmpty();
      soft.assertThat(ia.byNameOnRoot(subName)).isEmpty();
      soft.assertThat(ia.byParentIdAndName(namespaceId, subName)).get().isInstanceOf(typeClass);
      soft.assertThat(ia.byParentIdAndName(42L, subName)).isEmpty();
      soft.assertThat(ia.byParentIdAndName(0L, subName)).isEmpty();
      soft.assertThat(ia.byId(subId)).get().isInstanceOf(typeClass);
      soft.assertThat(ia.byId(42L)).isEmpty();
      soft.assertThat(ia.refObj()).get().isInstanceOf(mapping.containerObjTypeClass());

      // verify via indexedAccessDirect()
      ia = indexedAccessDirect(persistence, objRef(ia.refObj().orElseThrow()));
      soft.assertThat(ia).isInstanceOf(IndexedContainerAccessImpl.class);
      soft.assertThat(ia.catalogStableId()).isEqualTo(-1L);
      soft.assertThat(ia.nameIndex()).isNotEmpty();
      soft.assertThat(ia.stableIdIndex()).isNotEmpty();
      soft.assertThat(ia.byNameOnRoot(subName)).isEmpty();
      soft.assertThat(ia.byParentIdAndName(namespaceId, subName)).get().isInstanceOf(typeClass);
      soft.assertThat(ia.byParentIdAndName(42L, subName)).isEmpty();
      soft.assertThat(ia.byParentIdAndName(0L, subName)).isEmpty();
      soft.assertThat(ia.byId(subId)).get().isInstanceOf(typeClass);
      soft.assertThat(ia.byId(42L)).isEmpty();
      soft.assertThat(ia.refObj()).get().isInstanceOf(mapping.containerObjTypeClass());
    }
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
