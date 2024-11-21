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
package org.apache.polaris.core.storage.cache;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisGrantManager;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.CatalogRoleEntity;
import org.apache.polaris.core.entity.NamespaceEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.entity.TableLikeEntity;
import org.apache.polaris.core.persistence.BaseResult;
import org.apache.polaris.core.persistence.LocalPolarisMetaStoreManagerFactory;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.PolarisTreeMapMetaStoreSessionImpl;
import org.apache.polaris.core.persistence.PolarisTreeMapStore;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.core.persistence.cache.EntityCacheGrantManager;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class EntityCacheGrantManagerTest {

  public static final String NS_1 = "ns1";
  public static final String NS_2 = "ns2";
  public static final String TABLE_1 = "table1";
  private MetaStoreManagerFactory metastoreManagerFactory;
  private RealmContext realmContext;
  private PolarisMetaStoreManager metaStoreManager;
  private PolarisCallContext polarisCallContext;
  private CatalogEntity catalog;
  private PrincipalEntity principal;
  private PrincipalRoleEntity principalRole1;
  private PrincipalRoleEntity principalRole2;
  private CatalogRoleEntity catalogRole1;
  private CatalogRoleEntity catalogRole2;

  @BeforeEach
  public void setup() {
    // Create a MetastoreManager
    PolarisDefaultDiagServiceImpl diagServices = new PolarisDefaultDiagServiceImpl();
    PolarisTreeMapStore treeMapStore = new PolarisTreeMapStore(diagServices);
    PolarisTreeMapMetaStoreSessionImpl session =
        new PolarisTreeMapMetaStoreSessionImpl(treeMapStore, Mockito.mock());
    metastoreManagerFactory =
        new LocalPolarisMetaStoreManagerFactory<PolarisTreeMapStore>() {

          @Override
          protected PolarisTreeMapStore createBackingStore(
              @NotNull PolarisDiagnostics diagnostics) {
            return treeMapStore;
          }

          @Override
          protected PolarisMetaStoreSession createMetaStoreSession(
              @NotNull PolarisTreeMapStore store, @NotNull RealmContext realmContext) {
            return session;
          }
        };
    // Create a Resolver and an EntityCacheGrantManager
    realmContext = () -> "realm";
    metastoreManagerFactory.bootstrapRealms(List.of(realmContext.getRealmIdentifier()));
    metaStoreManager = metastoreManagerFactory.getOrCreateMetaStoreManager(realmContext);
    polarisCallContext = new PolarisCallContext(session, diagServices);

    // Create a principal with three Principal roles
    principal =
        new PrincipalEntity.Builder()
            .setId(metaStoreManager.generateNewEntityId(polarisCallContext).getId())
            .setName("principal1")
            .setCreateTimestamp(System.currentTimeMillis())
            .setEntityVersion(1)
            .build();
    principalRole1 =
        new PrincipalRoleEntity.Builder()
            .setId(metaStoreManager.generateNewEntityId(polarisCallContext).getId())
            .setName("principalRole1")
            .setCreateTimestamp(System.currentTimeMillis())
            .setEntityVersion(1)
            .build();
    principalRole2 =
        new PrincipalRoleEntity.Builder()
            .setId(metaStoreManager.generateNewEntityId(polarisCallContext).getId())
            .setName("principalRole2")
            .setCreateTimestamp(System.currentTimeMillis())
            .setEntityVersion(1)
            .build();

    // Persist the principal and principal roles
    metaStoreManager.createPrincipal(polarisCallContext, principal);
    metaStoreManager.createEntityIfNotExists(polarisCallContext, null, principalRole1);
    metaStoreManager.createEntityIfNotExists(polarisCallContext, null, principalRole2);

    // Create a catalog with two catalog roles
    catalog =
        new CatalogEntity.Builder()
            .setId(metaStoreManager.generateNewEntityId(polarisCallContext).getId())
            .setName("mycatalog")
            .setCreateTimestamp(System.currentTimeMillis())
            .setEntityVersion(1)
            .build();
    catalogRole1 =
        new CatalogRoleEntity.Builder()
            .setId(metaStoreManager.generateNewEntityId(polarisCallContext).getId())
            .setName("mycatrole1")
            .setCatalogId(catalog.getId())
            .setCreateTimestamp(System.currentTimeMillis())
            .setEntityVersion(1)
            .build();
    catalogRole2 =
        new CatalogRoleEntity.Builder()
            .setId(metaStoreManager.generateNewEntityId(polarisCallContext).getId())
            .setName("mycatrole2")
            .setCatalogId(catalog.getId())
            .setCreateTimestamp(System.currentTimeMillis())
            .setEntityVersion(1)
            .build();
    metaStoreManager.createCatalog(polarisCallContext, catalog, List.of(principalRole1));
    metaStoreManager.createEntityIfNotExists(polarisCallContext, List.of(catalog), catalogRole1);
    metaStoreManager.createEntityIfNotExists(polarisCallContext, List.of(catalog), catalogRole2);
    metaStoreManager.grantUsageOnRoleToGrantee(
        polarisCallContext, catalog, catalogRole1, principalRole1);
    metaStoreManager.grantUsageOnRoleToGrantee(
        polarisCallContext, catalog, catalogRole2, principalRole2);
    metaStoreManager.grantUsageOnRoleToGrantee(polarisCallContext, null, principalRole1, principal);
    metaStoreManager.grantUsageOnRoleToGrantee(polarisCallContext, null, principalRole2, principal);

    PolarisBaseEntity ns1 =
        metaStoreManager
            .createEntityIfNotExists(
                polarisCallContext,
                List.of(catalog),
                new NamespaceEntity.Builder(Namespace.of(NS_1))
                    .setId(metaStoreManager.generateNewEntityId(polarisCallContext).getId())
                    .setCatalogId(catalog.getId())
                    .setParentId(catalog.getId())
                    .setCreateTimestamp(System.currentTimeMillis())
                    .setEntityVersion(1)
                    .build())
            .getEntity();

    Namespace ns2Namespace = Namespace.of(NS_1, NS_2);
    PolarisBaseEntity ns2 =
        metaStoreManager
            .createEntityIfNotExists(
                polarisCallContext,
                List.of(catalog, ns1),
                new NamespaceEntity.Builder(ns2Namespace)
                    .setId(metaStoreManager.generateNewEntityId(polarisCallContext).getId())
                    .setCatalogId(catalog.getId())
                    .setParentId(ns1.getId())
                    .setCreateTimestamp(System.currentTimeMillis())
                    .setEntityVersion(1)
                    .build())
            .getEntity();

    TableIdentifier tbId = TableIdentifier.of(ns2Namespace, TABLE_1);
    PolarisBaseEntity tb1 =
        metaStoreManager
            .createEntityIfNotExists(
                polarisCallContext,
                List.of(catalog),
                new TableLikeEntity.Builder(tbId, "file://abc")
                    .setId(metaStoreManager.generateNewEntityId(polarisCallContext).getId())
                    .setCatalogId(catalog.getId())
                    .setParentId(ns2.getId())
                    .setCreateTimestamp(System.currentTimeMillis())
                    .setEntityVersion(1)
                    .build())
            .getEntity();
    metaStoreManager.grantPrivilegeOnSecurableToRole(
        polarisCallContext,
        new PolarisEntityCore(catalogRole1),
        List.of(PolarisEntity.toCore(catalog)),
        PolarisEntity.toCore(ns1),
        PolarisPrivilege.NAMESPACE_LIST);
    metaStoreManager.grantPrivilegeOnSecurableToRole(
        polarisCallContext,
        new PolarisEntityCore(catalogRole1),
        List.of(PolarisEntity.toCore(catalog), PolarisEntity.toCore(ns1)),
        PolarisEntity.toCore(ns2),
        PolarisPrivilege.TABLE_CREATE);
    metaStoreManager.grantPrivilegeOnSecurableToRole(
        polarisCallContext,
        new PolarisEntityCore(catalogRole2),
        List.of(
            PolarisEntity.toCore(catalog), PolarisEntity.toCore(ns1), PolarisEntity.toCore(ns2)),
        PolarisEntity.toCore(tb1),
        PolarisPrivilege.TABLE_READ_DATA);
  }

  @Test
  public void testReadFromResolverPopulatedCache() {
    Namespace ns2 = Namespace.of(NS_1, NS_2);
    TableIdentifier tbId = TableIdentifier.of(ns2, TABLE_1);
    EntityCache entityCache = new EntityCache(metaStoreManager);
    PolarisResolutionManifest manifest =
        new PolarisResolutionManifest(
            CallContext.of(realmContext, polarisCallContext),
            new PolarisEntityManager(metaStoreManager, entityCache, new StorageCredentialCache()),
            new AuthenticatedPolarisPrincipal(
                principal, Set.of(principalRole1.getName(), principalRole2.getName())),
            catalog.getName());
    manifest.addPath(new ResolverPath(List.of(NS_1, NS_2), PolarisEntityType.NAMESPACE), ns2);
    manifest.addPath(
        new ResolverPath(List.of(NS_1, NS_2, TABLE_1), PolarisEntityType.TABLE_LIKE), tbId);

    ResolverStatus resolverStatus = manifest.resolveAll();
    Assertions.assertThat(resolverStatus.getStatus()).isEqualTo(ResolverStatus.StatusEnum.SUCCESS);

    // now create an EntityCacheGrantManager with a dummy grantManager implementation.
    // everything should be returned from the cache
    EntityCacheGrantManager grantManager = new EntityCacheGrantManager(Mockito.mock(), entityCache);
    PolarisResolvedPathWrapper resolvedNamespace = manifest.getResolvedPath(ns2, false);

    PolarisGrantManager.LoadGrantsResult grantsResult =
        grantManager.loadGrantsOnSecurable(
            polarisCallContext,
            manifest.getResolvedReferenceCatalogEntity().getRawLeafEntity().getId(),
            resolvedNamespace.getRawLeafEntity().getId());
    Assertions.assertThat(grantsResult)
        .returns(true, PolarisGrantManager.LoadGrantsResult::isSuccess)
        .extracting(PolarisGrantManager.LoadGrantsResult::getGrantRecords)
        .asInstanceOf(InstanceOfAssertFactories.list(PolarisGrantRecord.class))
        .hasSize(1)
        .satisfiesExactly(
            grant -> {
              Assertions.assertThat(grant)
                  .returns(catalogRole1.getId(), PolarisGrantRecord::getGranteeId)
                  .returns(
                      PolarisPrivilege.TABLE_CREATE.getCode(),
                      PolarisGrantRecord::getPrivilegeCode);
            });

    PolarisResolvedPathWrapper resolvedTable = manifest.getResolvedPath(tbId, false);
    PolarisGrantManager.LoadGrantsResult tbGrantsResult =
        grantManager.loadGrantsOnSecurable(
            polarisCallContext,
            manifest.getResolvedReferenceCatalogEntity().getRawLeafEntity().getId(),
            resolvedTable.getRawLeafEntity().getId());
    Assertions.assertThat(tbGrantsResult)
        .returns(true, PolarisGrantManager.LoadGrantsResult::isSuccess)
        .extracting(PolarisGrantManager.LoadGrantsResult::getGrantRecords)
        .asInstanceOf(InstanceOfAssertFactories.list(PolarisGrantRecord.class))
        .hasSize(1)
        .satisfiesExactly(
            grant -> {
              Assertions.assertThat(grant)
                  .returns(catalogRole2.getId(), PolarisGrantRecord::getGranteeId)
                  .returns(
                      PolarisPrivilege.TABLE_READ_DATA.getCode(),
                      PolarisGrantRecord::getPrivilegeCode);
            });
  }

  @Test
  public void testRevokeGrantsClearsCache() {
    Namespace ns2 = Namespace.of(NS_1, NS_2);
    TableIdentifier tbId = TableIdentifier.of(ns2, TABLE_1);
    EntityCache entityCache = new EntityCache(metaStoreManager);
    PolarisResolutionManifest manifest =
        new PolarisResolutionManifest(
            CallContext.of(realmContext, polarisCallContext),
            new PolarisEntityManager(metaStoreManager, entityCache, new StorageCredentialCache()),
            new AuthenticatedPolarisPrincipal(
                principal, Set.of(principalRole1.getName(), principalRole2.getName())),
            catalog.getName());
    manifest.addPath(new ResolverPath(List.of(NS_1, NS_2), PolarisEntityType.NAMESPACE), ns2);
    manifest.addPath(
        new ResolverPath(List.of(NS_1, NS_2, TABLE_1), PolarisEntityType.TABLE_LIKE), tbId);

    ResolverStatus resolverStatus = manifest.resolveAll();
    Assertions.assertThat(resolverStatus.getStatus()).isEqualTo(ResolverStatus.StatusEnum.SUCCESS);

    PolarisGrantManager mockGrantManager = Mockito.mock();
    EntityCacheGrantManager grantManager =
        new EntityCacheGrantManager(mockGrantManager, entityCache);
    PolarisResolvedPathWrapper resolvedNamespace = manifest.getResolvedPath(ns2, false);

    // update the mock to return a successful response when we revoke the privilege
    Mockito.when(
            grantManager.revokePrivilegeOnSecurableFromRole(
                polarisCallContext,
                catalogRole1,
                resolvedNamespace.getRawParentPath().stream()
                    .map(PolarisEntity::toCore)
                    .collect(Collectors.toList()),
                resolvedNamespace.getRawLeafEntity(),
                PolarisPrivilege.TABLE_CREATE))
        .thenReturn(new PolarisGrantManager.PrivilegeResult(BaseResult.ReturnStatus.SUCCESS, null));
    PolarisGrantManager.PrivilegeResult result =
        grantManager.revokePrivilegeOnSecurableFromRole(
            polarisCallContext,
            catalogRole1,
            resolvedNamespace.getRawParentPath().stream()
                .map(PolarisEntity::toCore)
                .collect(Collectors.toList()),
            resolvedNamespace.getRawLeafEntity(),
            PolarisPrivilege.TABLE_CREATE);
    Assertions.assertThat(result)
        .returns(
            BaseResult.ReturnStatus.SUCCESS, PolarisGrantManager.PrivilegeResult::getReturnStatus);

    // the cache is cleared
    Assertions.assertThat(entityCache.getEntityById(resolvedNamespace.getRawLeafEntity().getId()))
        .isNull();

    // update the mock to return a successful, but empty response when we load the grants
    Mockito.when(
            mockGrantManager.loadGrantsOnSecurable(
                polarisCallContext,
                manifest.getResolvedReferenceCatalogEntity().getRawLeafEntity().getId(),
                resolvedNamespace.getRawLeafEntity().getId()))
        .thenReturn(new PolarisGrantManager.LoadGrantsResult(2, List.of(), List.of()));
    PolarisGrantManager.LoadGrantsResult grantsResult =
        mockGrantManager.loadGrantsOnSecurable(
            polarisCallContext,
            manifest.getResolvedReferenceCatalogEntity().getRawLeafEntity().getId(),
            resolvedNamespace.getRawLeafEntity().getId());
    Assertions.assertThat(grantsResult)
        .returns(true, PolarisGrantManager.LoadGrantsResult::isSuccess)
        .extracting(PolarisGrantManager.LoadGrantsResult::getGrantRecords)
        .asInstanceOf(InstanceOfAssertFactories.list(PolarisGrantRecord.class))
        .isEmpty();
  }

  @Test
  public void testUpdateGrantsClearsCache() {
    Namespace ns2 = Namespace.of(NS_1, NS_2);
    TableIdentifier tbId = TableIdentifier.of(ns2, TABLE_1);
    EntityCache entityCache = new EntityCache(metaStoreManager);
    PolarisResolutionManifest manifest =
        new PolarisResolutionManifest(
            CallContext.of(realmContext, polarisCallContext),
            new PolarisEntityManager(metaStoreManager, entityCache, new StorageCredentialCache()),
            new AuthenticatedPolarisPrincipal(
                principal, Set.of(principalRole1.getName(), principalRole2.getName())),
            catalog.getName());
    manifest.addPath(new ResolverPath(List.of(NS_1, NS_2), PolarisEntityType.NAMESPACE), ns2);
    manifest.addPath(
        new ResolverPath(List.of(NS_1, NS_2, TABLE_1), PolarisEntityType.TABLE_LIKE), tbId);

    ResolverStatus resolverStatus = manifest.resolveAll();
    Assertions.assertThat(resolverStatus.getStatus()).isEqualTo(ResolverStatus.StatusEnum.SUCCESS);

    EntityCacheGrantManager grantManager =
        new EntityCacheGrantManager(metaStoreManager, entityCache);
    PolarisResolvedPathWrapper resolvedTable = manifest.getResolvedPath(tbId, false);

    PolarisGrantManager.PrivilegeResult result =
        grantManager.grantPrivilegeOnSecurableToRole(
            polarisCallContext,
            catalogRole2,
            resolvedTable.getRawParentPath().stream()
                .map(PolarisEntity::toCore)
                .collect(Collectors.toList()),
            resolvedTable.getRawLeafEntity(),
            PolarisPrivilege.TABLE_DROP);
    Assertions.assertThat(result)
        .returns(
            BaseResult.ReturnStatus.SUCCESS, PolarisGrantManager.PrivilegeResult::getReturnStatus);

    // the cache is cleared
    Assertions.assertThat(entityCache.getEntityById(resolvedTable.getRawLeafEntity().getId()))
        .isNull();

    PolarisGrantManager.LoadGrantsResult grantsResult =
        grantManager.loadGrantsOnSecurable(
            polarisCallContext,
            manifest.getResolvedReferenceCatalogEntity().getRawLeafEntity().getId(),
            resolvedTable.getRawLeafEntity().getId());
    Assertions.assertThat(grantsResult)
        .returns(true, PolarisGrantManager.LoadGrantsResult::isSuccess)
        .extracting(PolarisGrantManager.LoadGrantsResult::getGrantRecords)
        .asInstanceOf(InstanceOfAssertFactories.list(PolarisGrantRecord.class))
        .hasSize(2)
        .satisfiesExactlyInAnyOrder(
            (PolarisGrantRecord gr) ->
                Assertions.assertThat(gr)
                    .returns(catalogRole2.getId(), PolarisGrantRecord::getGranteeId)
                    .returns(
                        PolarisPrivilege.TABLE_READ_DATA.getCode(),
                        PolarisGrantRecord::getPrivilegeCode),
            (PolarisGrantRecord gr) ->
                Assertions.assertThat(gr)
                    .returns(catalogRole2.getId(), PolarisGrantRecord::getGranteeId)
                    .returns(
                        PolarisPrivilege.TABLE_DROP.getCode(),
                        PolarisGrantRecord::getPrivilegeCode));
  }
}
