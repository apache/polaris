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
package org.apache.polaris.service.quarkus;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import jakarta.ws.rs.core.SecurityContext;
import java.security.Principal;
import java.time.Clock;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.context.RealmId;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;
import org.apache.polaris.service.admin.PolarisServiceImpl;
import org.apache.polaris.service.admin.api.PolarisCatalogsApi;
import org.apache.polaris.service.catalog.IcebergCatalogAdapter;
import org.apache.polaris.service.catalog.api.IcebergRestCatalogApi;
import org.apache.polaris.service.catalog.api.IcebergRestCatalogApiService;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.config.DefaultConfigurationStore;
import org.apache.polaris.service.config.RealmEntityManagerFactory;
import org.apache.polaris.service.events.DefaultPolarisEventListener;
import org.apache.polaris.service.persistence.InMemoryPolarisMetaStoreManagerFactory;
import org.apache.polaris.service.quarkus.catalog.io.TestFileIOFactory;
import org.apache.polaris.service.storage.PolarisStorageIntegrationProviderImpl;
import org.apache.polaris.service.task.TaskExecutor;
import org.mockito.Mockito;

public record TestServices(
    IcebergRestCatalogApi restApi,
    PolarisCatalogsApi catalogsApi,
    RealmId realmId,
    SecurityContext securityContext) {

  private static final RealmId testRealm = RealmId.newRealmId("test-realm");

  public static TestServices inMemory(Map<String, Object> config) {
    return inMemory(new TestFileIOFactory(), config);
  }

  public static TestServices inMemory(FileIOFactory ioFactory) {
    return inMemory(ioFactory, Map.of());
  }

  public static TestServices inMemory(FileIOFactory ioFactory, Map<String, Object> config) {

    DefaultConfigurationStore configurationStore = new DefaultConfigurationStore(config);
    PolarisDiagnostics polarisDiagnostics = Mockito.mock(PolarisDiagnostics.class);

    PolarisStorageIntegrationProviderImpl storageIntegrationProvider =
        new PolarisStorageIntegrationProviderImpl(
            Mockito::mock,
            () -> GoogleCredentials.create(new AccessToken("abc", new Date())),
            configurationStore);

    InMemoryPolarisMetaStoreManagerFactory metaStoreManagerFactory =
        new InMemoryPolarisMetaStoreManagerFactory(
            storageIntegrationProvider,
            configurationStore,
            polarisDiagnostics,
            Clock.systemDefaultZone());

    PolarisMetaStoreManager metaStoreManager =
        metaStoreManagerFactory.getOrCreateMetaStoreManager(testRealm);

    PolarisMetaStoreSession session =
        metaStoreManagerFactory.getOrCreateSessionSupplier(testRealm).get();

    RealmEntityManagerFactory realmEntityManagerFactory =
        new RealmEntityManagerFactory(metaStoreManagerFactory, polarisDiagnostics) {};

    PolarisEntityManager entityManager =
        realmEntityManagerFactory.getOrCreateEntityManager(testRealm);

    PolarisAuthorizer authorizer = Mockito.mock(PolarisAuthorizer.class);

    IcebergRestCatalogApiService service =
        new IcebergCatalogAdapter(
            testRealm,
            entityManager,
            metaStoreManager,
            session,
            configurationStore,
            polarisDiagnostics,
            authorizer,
            Mockito.mock(TaskExecutor.class),
            ioFactory,
                new DefaultPolarisEventListener());

    IcebergRestCatalogApi restApi = new IcebergRestCatalogApi(service);

    PolarisMetaStoreManager.CreatePrincipalResult createdPrincipal =
        metaStoreManager.createPrincipal(
            session,
            new PrincipalEntity.Builder()
                .setName("test-principal")
                .setCreateTimestamp(Instant.now().toEpochMilli())
                .setCredentialRotationRequiredState()
                .build());

    AuthenticatedPolarisPrincipal principal =
        new AuthenticatedPolarisPrincipal(
            PolarisEntity.of(createdPrincipal.getPrincipal()), Set.of());

    SecurityContext securityContext =
        new SecurityContext() {
          @Override
          public Principal getUserPrincipal() {
            return principal;
          }

          @Override
          public boolean isUserInRole(String s) {
            return false;
          }

          @Override
          public boolean isSecure() {
            return true;
          }

          @Override
          public String getAuthenticationScheme() {
            return "";
          }
        };

    PolarisCatalogsApi catalogsApi =
        new PolarisCatalogsApi(
            new PolarisServiceImpl(
                entityManager,
                metaStoreManager,
                session,
                configurationStore,
                authorizer,
                polarisDiagnostics));

    return new TestServices(restApi, catalogsApi, testRealm, securityContext);
  }
}
