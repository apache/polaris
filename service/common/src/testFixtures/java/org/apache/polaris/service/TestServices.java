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
package org.apache.polaris.service;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import jakarta.ws.rs.core.SecurityContext;
import java.security.Principal;
import java.time.Clock;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;
import org.apache.polaris.service.admin.PolarisServiceImpl;
import org.apache.polaris.service.admin.api.PolarisCatalogsApi;
import org.apache.polaris.service.catalog.IcebergCatalogAdapter;
import org.apache.polaris.service.catalog.api.IcebergRestCatalogApi;
import org.apache.polaris.service.catalog.api.IcebergRestCatalogApiService;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.MeasuredFileIOFactory;
import org.apache.polaris.service.config.DefaultConfigurationStore;
import org.apache.polaris.service.config.RealmEntityManagerFactory;
import org.apache.polaris.service.context.CallContextCatalogFactory;
import org.apache.polaris.service.context.PolarisCallContextCatalogFactory;
import org.apache.polaris.service.persistence.InMemoryPolarisMetaStoreManagerFactory;
import org.apache.polaris.service.storage.PolarisStorageIntegrationProviderImpl;
import org.apache.polaris.service.task.TaskExecutor;
import org.assertj.core.util.TriFunction;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sts.StsClient;

public record TestServices(
    PolarisCatalogsApi catalogsApi,
    IcebergRestCatalogApi restApi,
    PolarisConfigurationStore configurationStore,
    PolarisDiagnostics polarisDiagnostics,
    RealmEntityManagerFactory entityManagerFactory,
    MetaStoreManagerFactory metaStoreManagerFactory,
    RealmContext realmContext,
    SecurityContext securityContext,
    FileIOFactory fileIOFactory,
    TaskExecutor taskExecutor) {

  private static final RealmContext TEST_REALM = () -> "test-realm";
  private static final String GCP_ACCESS_TOKEN = "abc";

  @FunctionalInterface
  public interface FileIOFactorySupplier
      extends TriFunction<
          RealmEntityManagerFactory,
          MetaStoreManagerFactory,
          PolarisConfigurationStore,
          FileIOFactory> {}

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private RealmContext realm = TEST_REALM;
    private Map<String, Object> config = Map.of();
    private StsClient stsClient = Mockito.mock(StsClient.class);
    private FileIOFactorySupplier fileIOFactorySupplier = MeasuredFileIOFactory::new;

    private Builder() {}

    public Builder realmId(RealmContext realmId) {
      this.realm = realmId;
      return this;
    }

    public Builder config(Map<String, Object> config) {
      this.config = config;
      return this;
    }

    public Builder fileIOFactorySupplier(FileIOFactorySupplier fileIOFactorySupplier) {
      this.fileIOFactorySupplier = fileIOFactorySupplier;
      return this;
    }

    public Builder stsClient(StsClient stsClient) {
      this.stsClient = stsClient;
      return this;
    }

    public TestServices build() {
      DefaultConfigurationStore configurationStore = new DefaultConfigurationStore(config);
      PolarisDiagnostics polarisDiagnostics = Mockito.mock(PolarisDiagnostics.class);
      PolarisAuthorizer authorizer = Mockito.mock(PolarisAuthorizer.class);

      // Application level
      PolarisStorageIntegrationProviderImpl storageIntegrationProvider =
          new PolarisStorageIntegrationProviderImpl(
              () -> stsClient,
              () -> GoogleCredentials.create(new AccessToken(GCP_ACCESS_TOKEN, new Date())),
              configurationStore);
      InMemoryPolarisMetaStoreManagerFactory metaStoreManagerFactory =
          new InMemoryPolarisMetaStoreManagerFactory(
              storageIntegrationProvider,
              configurationStore,
              polarisDiagnostics,
              Clock.systemDefaultZone());
      RealmEntityManagerFactory realmEntityManagerFactory =
          new RealmEntityManagerFactory(metaStoreManagerFactory, polarisDiagnostics) {};

      PolarisEntityManager entityManager =
          realmEntityManagerFactory.getOrCreateEntityManager(realm);
      PolarisMetaStoreManager metaStoreManager =
          metaStoreManagerFactory.getOrCreateMetaStoreManager(realm);
      PolarisMetaStoreSession metaStoreSession =
          metaStoreManagerFactory.getOrCreateSessionSupplier(realm).get();

      FileIOFactory fileIOFactory =
          fileIOFactorySupplier.apply(
              realmEntityManagerFactory, metaStoreManagerFactory, configurationStore);

      TaskExecutor taskExecutor = Mockito.mock(TaskExecutor.class);

      CallContextCatalogFactory callContextFactory =
          new PolarisCallContextCatalogFactory(
              entityManager,
              metaStoreManager,
              metaStoreSession,
              configurationStore,
              polarisDiagnostics,
              Mockito.mock(TaskExecutor.class),
              fileIOFactory);

      IcebergRestCatalogApiService service =
          new IcebergCatalogAdapter(
              realm,
              callContextFactory,
              entityManager,
              metaStoreManager,
              metaStoreSession,
              configurationStore,
              polarisDiagnostics,
              authorizer);

      IcebergRestCatalogApi restApi = new IcebergRestCatalogApi(service);

      PolarisMetaStoreManager.CreatePrincipalResult createdPrincipal =
          metaStoreManager.createPrincipal(
              metaStoreSession,
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
                  metaStoreSession,
                  configurationStore,
                  authorizer,
                  polarisDiagnostics));

      return new TestServices(
          catalogsApi,
          restApi,
          configurationStore,
          polarisDiagnostics,
          realmEntityManagerFactory,
          metaStoreManagerFactory,
          realm,
          securityContext,
          fileIOFactory,
          taskExecutor);
    }
  }
}
