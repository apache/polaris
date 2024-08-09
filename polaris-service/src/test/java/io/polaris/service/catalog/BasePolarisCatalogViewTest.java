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
package io.polaris.service.catalog;

import com.google.common.collect.ImmutableMap;
import io.polaris.core.PolarisCallContext;
import io.polaris.core.PolarisConfiguration;
import io.polaris.core.PolarisConfigurationStore;
import io.polaris.core.PolarisDefaultDiagServiceImpl;
import io.polaris.core.PolarisDiagnostics;
import io.polaris.core.admin.model.FileStorageConfigInfo;
import io.polaris.core.admin.model.StorageConfigInfo;
import io.polaris.core.auth.AuthenticatedPolarisPrincipal;
import io.polaris.core.auth.PolarisAuthorizer;
import io.polaris.core.context.CallContext;
import io.polaris.core.context.RealmContext;
import io.polaris.core.entity.CatalogEntity;
import io.polaris.core.entity.PolarisEntity;
import io.polaris.core.entity.PolarisEntitySubType;
import io.polaris.core.entity.PolarisEntityType;
import io.polaris.core.entity.PrincipalEntity;
import io.polaris.core.persistence.PolarisEntityManager;
import io.polaris.core.persistence.PolarisMetaStoreManager;
import io.polaris.core.storage.cache.StorageCredentialCache;
import io.polaris.service.admin.PolarisAdminService;
import io.polaris.service.persistence.InMemoryPolarisMetaStoreManagerFactory;
import io.polaris.service.storage.PolarisStorageIntegrationProviderImpl;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.view.ViewCatalogTests;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;

public class BasePolarisCatalogViewTest extends ViewCatalogTests<BasePolarisCatalog> {
  public static final String CATALOG_NAME = "polaris-catalog";
  private BasePolarisCatalog catalog;

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void before() {
    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    RealmContext realmContext = () -> "realm";
    InMemoryPolarisMetaStoreManagerFactory managerFactory =
        new InMemoryPolarisMetaStoreManagerFactory();
    managerFactory.setStorageIntegrationProvider(
        new PolarisStorageIntegrationProviderImpl(Mockito::mock));
    PolarisMetaStoreManager metaStoreManager =
        managerFactory.getOrCreateMetaStoreManager(realmContext);
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("ALLOW_WILDCARD_LOCATION", true);
    configMap.put("ALLOW_SPECIFYING_FILE_IO_IMPL", true);
    PolarisCallContext polarisContext =
        new PolarisCallContext(
            managerFactory.getOrCreateSessionSupplier(realmContext).get(),
            diagServices,
            new PolarisConfigurationStore() {
              @Override
              public <T> @Nullable T getConfiguration(PolarisCallContext ctx, String configName) {
                return (T) configMap.get(configName);
              }
            },
            Clock.systemDefaultZone());

    PolarisEntityManager entityManager =
        new PolarisEntityManager(
            metaStoreManager, polarisContext::getMetaStore, new StorageCredentialCache());

    CallContext callContext = CallContext.of(null, polarisContext);
    CallContext.setCurrentContext(callContext);

    PrincipalEntity rootEntity =
        new PrincipalEntity(
            PolarisEntity.of(
                entityManager
                    .getMetaStoreManager()
                    .readEntityByName(
                        polarisContext,
                        null,
                        PolarisEntityType.PRINCIPAL,
                        PolarisEntitySubType.NULL_SUBTYPE,
                        "root")
                    .getEntity()));
    AuthenticatedPolarisPrincipal authenticatedRoot =
        new AuthenticatedPolarisPrincipal(rootEntity, Set.of());

    PolarisAdminService adminService =
        new PolarisAdminService(
            callContext,
            entityManager,
            authenticatedRoot,
            new PolarisAuthorizer(new PolarisConfigurationStore() {}));
    adminService.createCatalog(
        new CatalogEntity.Builder()
            .setName(CATALOG_NAME)
            .addProperty(PolarisConfiguration.ALLOW_EXTERNAL_TABLE_LOCATION.catalogConfig(), "true")
            .addProperty(
                PolarisConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(), "true")
            .setDefaultBaseLocation("file://tmp")
            .setStorageConfigurationInfo(
                new FileStorageConfigInfo(
                    StorageConfigInfo.StorageTypeEnum.FILE, List.of("file://", "/", "*")),
                "file://tmp")
            .build());

    PolarisPassthroughResolutionView passthroughView =
        new PolarisPassthroughResolutionView(
            callContext, entityManager, authenticatedRoot, CATALOG_NAME);
    this.catalog =
        new BasePolarisCatalog(
            entityManager, callContext, passthroughView, authenticatedRoot, Mockito.mock());
    this.catalog.initialize(
        CATALOG_NAME,
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));
  }

  @Override
  protected BasePolarisCatalog catalog() {
    return catalog;
  }

  @Override
  protected Catalog tableCatalog() {
    return catalog;
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }
}
