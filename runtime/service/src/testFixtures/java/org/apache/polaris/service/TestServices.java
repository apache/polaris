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
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.enterprise.inject.Instance;
import jakarta.ws.rs.core.SecurityContext;
import java.security.Principal;
import java.time.Clock;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.catalog.ExternalCatalogFactory;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.core.persistence.dao.entity.CreatePrincipalResult;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactoryImpl;
import org.apache.polaris.core.persistence.resolver.Resolver;
import org.apache.polaris.core.persistence.resolver.ResolverFactory;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.core.secrets.UserSecretsManagerFactory;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.core.storage.cache.StorageCredentialCacheConfig;
import org.apache.polaris.service.admin.PolarisServiceImpl;
import org.apache.polaris.service.admin.api.PolarisCatalogsApi;
import org.apache.polaris.service.catalog.DefaultCatalogPrefixParser;
import org.apache.polaris.service.catalog.api.IcebergRestCatalogApi;
import org.apache.polaris.service.catalog.api.IcebergRestConfigurationApi;
import org.apache.polaris.service.catalog.iceberg.CatalogHandlerUtils;
import org.apache.polaris.service.catalog.iceberg.IcebergCatalogAdapter;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.MeasuredFileIOFactory;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.context.catalog.CallContextCatalogFactory;
import org.apache.polaris.service.context.catalog.PolarisCallContextCatalogFactory;
import org.apache.polaris.service.events.PolarisEventListener;
import org.apache.polaris.service.events.TestPolarisEventListener;
import org.apache.polaris.service.persistence.InMemoryPolarisMetaStoreManagerFactory;
import org.apache.polaris.service.secrets.UnsafeInMemorySecretsManagerFactory;
import org.apache.polaris.service.storage.PolarisStorageIntegrationProviderImpl;
import org.apache.polaris.service.task.TaskExecutor;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sts.StsClient;

public record TestServices(
    Clock clock,
    PolarisCatalogsApi catalogsApi,
    IcebergRestCatalogApi restApi,
    IcebergRestConfigurationApi restConfigurationApi,
    IcebergCatalogAdapter catalogAdapter,
    PolarisConfigurationStore configurationStore,
    PolarisDiagnostics polarisDiagnostics,
    StorageCredentialCache storageCredentialCache,
    ResolverFactory resolverFactory,
    ResolutionManifestFactory resolutionManifestFactory,
    MetaStoreManagerFactory metaStoreManagerFactory,
    RealmContext realmContext,
    SecurityContext securityContext,
    FileIOFactory fileIOFactory,
    TaskExecutor taskExecutor,
    PolarisEventListener polarisEventListener) {

  private static final RealmContext TEST_REALM = () -> "test-realm";
  private static final String GCP_ACCESS_TOKEN = "abc";

  @FunctionalInterface
  public interface FileIOFactorySupplier
      extends BiFunction<StorageCredentialCache, MetaStoreManagerFactory, FileIOFactory> {}

  private static class MockedConfigurationStore implements PolarisConfigurationStore {
    private final Map<String, Object> defaults;

    public MockedConfigurationStore(Map<String, Object> defaults) {
      this.defaults = Map.copyOf(defaults);
    }

    @Override
    public <T> @Nullable T getConfiguration(@Nonnull RealmContext realmContext, String configName) {
      @SuppressWarnings("unchecked")
      T confgValue = (T) defaults.get(configName);
      return confgValue;
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Clock clock = Clock.systemUTC();
    private PolarisDiagnostics polarisDiagnostics = new PolarisDefaultDiagServiceImpl();
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
      PolarisConfigurationStore configurationStore = new MockedConfigurationStore(config);
      PolarisAuthorizer authorizer = Mockito.mock(PolarisAuthorizer.class);

      // Application level
      PolarisStorageIntegrationProviderImpl storageIntegrationProvider =
          new PolarisStorageIntegrationProviderImpl(
              (destination) -> stsClient,
              Optional.empty(),
              () -> GoogleCredentials.create(new AccessToken(GCP_ACCESS_TOKEN, new Date())));
      InMemoryPolarisMetaStoreManagerFactory metaStoreManagerFactory =
          new InMemoryPolarisMetaStoreManagerFactory(
              clock, polarisDiagnostics, storageIntegrationProvider);

      StorageCredentialCacheConfig storageCredentialCacheConfig = () -> 10_000;
      StorageCredentialCache storageCredentialCache =
          new StorageCredentialCache(storageCredentialCacheConfig);

      UserSecretsManagerFactory userSecretsManagerFactory =
          new UnsafeInMemorySecretsManagerFactory();

      BasePersistence metaStoreSession = metaStoreManagerFactory.getOrCreateSession(realmContext);
      CallContext callContext =
          new PolarisCallContext(
              realmContext, metaStoreSession, polarisDiagnostics, configurationStore);

      PolarisMetaStoreManager metaStoreManager =
          metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);

      EntityCache entityCache =
          metaStoreManagerFactory.getOrCreateEntityCache(
              realmContext, callContext.getRealmConfig());
      ResolverFactory resolverFactory =
          (_callContext, securityContext, referenceCatalogName) ->
              new Resolver(
                  _callContext.getPolarisCallContext(),
                  metaStoreManager,
                  securityContext,
                  entityCache,
                  referenceCatalogName);

      ResolutionManifestFactory resolutionManifestFactory =
          new ResolutionManifestFactoryImpl(resolverFactory);
      UserSecretsManager userSecretsManager =
          userSecretsManagerFactory.getOrCreateUserSecretsManager(realmContext);

      FileIOFactory fileIOFactory =
          fileIOFactorySupplier.apply(storageCredentialCache, metaStoreManagerFactory);

      TaskExecutor taskExecutor = Mockito.mock(TaskExecutor.class);

      PolarisEventListener polarisEventListener = new TestPolarisEventListener();
      CallContextCatalogFactory callContextFactory =
          new PolarisCallContextCatalogFactory(
              storageCredentialCache,
              resolverFactory,
              metaStoreManagerFactory,
              taskExecutor,
              fileIOFactory,
              polarisEventListener);

      ReservedProperties reservedProperties = ReservedProperties.NONE;

      CatalogHandlerUtils catalogHandlerUtils =
          new CatalogHandlerUtils(callContext.getRealmConfig());

      @SuppressWarnings("unchecked")
      Instance<ExternalCatalogFactory> externalCatalogFactory = Mockito.mock(Instance.class);
      Mockito.when(externalCatalogFactory.select(Mockito.any())).thenReturn(externalCatalogFactory);
      Mockito.when(externalCatalogFactory.isUnsatisfied()).thenReturn(true);

      IcebergCatalogAdapter catalogService =
          new IcebergCatalogAdapter(
              realmContext,
              callContext,
              callContextFactory,
              resolverFactory,
              resolutionManifestFactory,
              metaStoreManager,
              userSecretsManager,
              authorizer,
              new DefaultCatalogPrefixParser(),
              reservedProperties,
              catalogHandlerUtils,
              externalCatalogFactory);

      IcebergRestCatalogApi restApi = new IcebergRestCatalogApi(catalogService);
      IcebergRestConfigurationApi restConfigurationApi =
          new IcebergRestConfigurationApi(catalogService);

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
                  resolutionManifestFactory,
                  metaStoreManagerFactory,
                  userSecretsManagerFactory,
                  authorizer,
                  callContext,
                  reservedProperties));

      return new TestServices(
          clock,
          catalogsApi,
          restApi,
          restConfigurationApi,
          catalogService,
          configurationStore,
          polarisDiagnostics,
          storageCredentialCache,
          resolverFactory,
          resolutionManifestFactory,
          metaStoreManagerFactory,
          realmContext,
          securityContext,
          fileIOFactory,
          taskExecutor,
          polarisEventListener);
    }
  }

  public PolarisCallContext newCallContext() {
    BasePersistence metaStore = metaStoreManagerFactory.getOrCreateSession(realmContext);
    return new PolarisCallContext(realmContext, metaStore, polarisDiagnostics, configurationStore);
  }
}
