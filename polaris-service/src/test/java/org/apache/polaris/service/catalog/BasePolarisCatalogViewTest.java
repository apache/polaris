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
package org.apache.polaris.service.catalog;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Nullable;
import java.time.Clock;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.view.ViewCatalogTests;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisConfiguration;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.DefaultPolarisAuthorizer;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.service.admin.PolarisAdminService;
import org.apache.polaris.service.auth.AuthenticatedPolarisPrincipalImpl;
import org.apache.polaris.service.catalog.io.DefaultFileIOFactory;
import org.apache.polaris.service.persistence.InMemoryPolarisMetaStoreManagerFactory;
import org.apache.polaris.service.storage.PolarisStorageIntegrationProviderImpl;
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
        new PolarisStorageIntegrationProviderImpl(
            Mockito::mock, () -> GoogleCredentials.create(new AccessToken("abc", new Date()))));
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
        new PolarisEntityManager(metaStoreManager, new StorageCredentialCache());

    CallContext callContext = CallContext.of(null, polarisContext);
    CallContext.setCurrentContext(callContext);

    PrincipalEntity rootEntity =
        new PrincipalEntity(
            PolarisEntity.of(
                metaStoreManager
                    .readEntityByName(
                        polarisContext,
                        null,
                        PolarisEntityType.PRINCIPAL,
                        PolarisEntitySubType.NULL_SUBTYPE,
                        "root")
                    .getEntity()));
    AuthenticatedPolarisPrincipal authenticatedRoot =
        new AuthenticatedPolarisPrincipalImpl(rootEntity.getId(), rootEntity.getName(), Set.of());

    PolarisAdminService adminService =
        new PolarisAdminService(
            callContext,
            entityManager,
            metaStoreManager,
            authenticatedRoot,
            new DefaultPolarisAuthorizer(new PolarisConfigurationStore() {}));
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
            entityManager,
            metaStoreManager,
            callContext,
            passthroughView,
            authenticatedRoot,
            Mockito.mock(),
            new DefaultFileIOFactory());
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
