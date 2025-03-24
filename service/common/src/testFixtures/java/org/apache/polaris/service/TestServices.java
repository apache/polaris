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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.CreatePrincipalResult;
import org.apache.polaris.core.persistence.transactional.TransactionalPersistence;
import org.apache.polaris.service.admin.PolarisServiceImpl;
import org.apache.polaris.service.admin.api.PolarisCatalogsApi;
import org.apache.polaris.service.catalog.DefaultCatalogPrefixParser;
import org.apache.polaris.service.catalog.api.IcebergRestCatalogApi;
import org.apache.polaris.service.catalog.api.IcebergRestCatalogApiService;
import org.apache.polaris.service.catalog.iceberg.IcebergCatalogAdapter;
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
    private RealmContext realmContext = TEST_REALM;
    private Map<String, Object> config = Map.of();
    private StsClient stsClient = Mockito.mock(StsClient.class);
    private FileIOFactorySupplier fileIOFactorySupplier = MeasuredFileIOFactory::new;

    private Builder() {}

    public Builder realmContext(RealmContext realmContext) {
      this.realmContext = realmContext;
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
              () -> GoogleCredentials.create(new AccessToken(GCP_ACCESS_TOKEN, new Date())));
      InMemoryPolarisMetaStoreManagerFactory metaStoreManagerFactory =
          new InMemoryPolarisMetaStoreManagerFactory(
              storageIntegrationProvider, polarisDiagnostics);
      RealmEntityManagerFactory realmEntityManagerFactory =
          new RealmEntityManagerFactory(metaStoreManagerFactory) {};

      PolarisEntityManager entityManager =
          realmEntityManagerFactory.getOrCreateEntityManager(realmContext);
      PolarisMetaStoreManager metaStoreManager =
          metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);
      TransactionalPersistence metaStoreSession =
          metaStoreManagerFactory.getOrCreateSessionSupplier(realmContext).get();
      CallContext callContext =
          new CallContext() {
            @Override
            public RealmContext getRealmContext() {
              return realmContext;
            }

            @Override
            public PolarisCallContext getPolarisCallContext() {
              return new PolarisCallContext(
                  metaStoreSession,
                  polarisDiagnostics,
                  configurationStore,
                  Mockito.mock(Clock.class));
            }

            @Override
            public Map<String, Object> contextVariables() {
              return new HashMap<>();
            }
          };

      FileIOFactory fileIOFactory =
          fileIOFactorySupplier.apply(
              realmEntityManagerFactory, metaStoreManagerFactory, configurationStore);

      TaskExecutor taskExecutor = Mockito.mock(TaskExecutor.class);

      CallContextCatalogFactory callContextFactory =
          new PolarisCallContextCatalogFactory(
              realmEntityManagerFactory, metaStoreManagerFactory, taskExecutor, fileIOFactory);

      IcebergRestCatalogApiService service =
          new IcebergCatalogAdapter(
              realmContext,
              callContext,
              callContextFactory,
              entityManager,
              metaStoreManager,
              authorizer,
              new DefaultCatalogPrefixParser());

      IcebergRestCatalogApi restApi = new IcebergRestCatalogApi(service);

      CreatePrincipalResult createdPrincipal =
          metaStoreManager.createPrincipal(
              callContext.getPolarisCallContext(),
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
                  realmEntityManagerFactory, metaStoreManagerFactory, authorizer, callContext));

      return new TestServices(
          catalogsApi,
          restApi,
          configurationStore,
          polarisDiagnostics,
          realmEntityManagerFactory,
          metaStoreManagerFactory,
          realmContext,
          securityContext,
          fileIOFactory,
          taskExecutor);
    }
  }
}
