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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;

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
import java.util.function.Function;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.catalog.ExternalCatalogFactory;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.credentials.connection.ConnectionCredentialVendor;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;
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
import org.apache.polaris.service.admin.PolarisAdminService;
import org.apache.polaris.service.admin.PolarisServiceImpl;
import org.apache.polaris.service.admin.api.PolarisCatalogsApi;
import org.apache.polaris.service.catalog.DefaultCatalogPrefixParser;
import org.apache.polaris.service.catalog.api.IcebergRestCatalogApi;
import org.apache.polaris.service.catalog.api.IcebergRestConfigurationApi;
import org.apache.polaris.service.catalog.iceberg.CatalogHandlerUtils;
import org.apache.polaris.service.catalog.iceberg.IcebergCatalogAdapter;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.MeasuredFileIOFactory;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.context.catalog.CallContextCatalogFactory;
import org.apache.polaris.service.context.catalog.PolarisCallContextCatalogFactory;
import org.apache.polaris.service.credentials.DefaultPolarisCredentialManager;
import org.apache.polaris.service.credentials.connection.SigV4ConnectionCredentialVendor;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.apache.polaris.service.events.listeners.TestPolarisEventListener;
import org.apache.polaris.service.identity.provider.DefaultServiceIdentityProvider;
import org.apache.polaris.service.persistence.InMemoryPolarisMetaStoreManagerFactory;
import org.apache.polaris.service.secrets.UnsafeInMemorySecretsManagerFactory;
import org.apache.polaris.service.storage.PolarisStorageIntegrationProviderImpl;
import org.apache.polaris.service.task.TaskExecutor;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

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
    RealmConfig realmConfig,
    SecurityContext securityContext,
    PolarisMetaStoreManager metaStoreManager,
    FileIOFactory fileIOFactory,
    TaskExecutor taskExecutor,
    PolarisEventListener polarisEventListener,
    StorageAccessConfigProvider storageAccessConfigProvider) {

  private static final RealmContext TEST_REALM = () -> "test-realm";
  private static final String GCP_ACCESS_TOKEN = "abc";

  @FunctionalInterface
  public interface FileIOFactorySupplier
      extends Function<StorageAccessConfigProvider, FileIOFactory> {}

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
    private PolarisDiagnostics diagnostics = new PolarisDefaultDiagServiceImpl();
    private RealmContext realmContext = TEST_REALM;
    private Map<String, Object> config = Map.of();
    private StsClient stsClient;
    private FileIOFactorySupplier fileIOFactorySupplier =
        metaStoreManagerFactory1 -> new MeasuredFileIOFactory(metaStoreManagerFactory1);

    private Builder() {
      stsClient = Mockito.mock(StsClient.class, RETURNS_DEEP_STUBS);
      AssumeRoleResponse arr = Mockito.mock(AssumeRoleResponse.class, RETURNS_DEEP_STUBS);
      Mockito.when(stsClient.assumeRole(any(AssumeRoleRequest.class))).thenReturn(arr);
      Mockito.when(arr.credentials())
          .thenReturn(
              Credentials.builder()
                  .accessKeyId("test-access-key-id-111")
                  .secretAccessKey("test-secret-access-key-222")
                  .sessionToken("test-session-token-333")
                  .build());
    }

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
              clock, diagnostics, storageIntegrationProvider);

      StorageCredentialCacheConfig storageCredentialCacheConfig = () -> 10_000;
      StorageCredentialCache storageCredentialCache =
          new StorageCredentialCache(diagnostics, storageCredentialCacheConfig);

      UserSecretsManagerFactory userSecretsManagerFactory =
          new UnsafeInMemorySecretsManagerFactory();

      BasePersistence metaStoreSession = metaStoreManagerFactory.getOrCreateSession(realmContext);
      CallContext callContext =
          new PolarisCallContext(realmContext, metaStoreSession, configurationStore);
      RealmConfig realmConfig = callContext.getRealmConfig();

      PolarisMetaStoreManager metaStoreManager =
          metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);

      EntityCache entityCache =
          metaStoreManagerFactory.getOrCreateEntityCache(realmContext, realmConfig);
      ResolverFactory resolverFactory =
          (securityContext, referenceCatalogName) ->
              new Resolver(
                  diagnostics,
                  callContext.getPolarisCallContext(),
                  metaStoreManager,
                  securityContext,
                  entityCache,
                  referenceCatalogName);

      ResolutionManifestFactory resolutionManifestFactory =
          new ResolutionManifestFactoryImpl(diagnostics, realmContext, resolverFactory);

      UserSecretsManager userSecretsManager =
          userSecretsManagerFactory.getOrCreateUserSecretsManager(realmContext);
      ServiceIdentityProvider serviceIdentityProvider = new DefaultServiceIdentityProvider();

      // Create credential vendors for testing
      @SuppressWarnings("unchecked")
      Instance<ConnectionCredentialVendor> mockCredentialVendors = Mockito.mock(Instance.class);
      SigV4ConnectionCredentialVendor sigV4Vendor =
          new SigV4ConnectionCredentialVendor((destination) -> stsClient, serviceIdentityProvider);
      Mockito.when(
              mockCredentialVendors.select(
                  any(org.apache.polaris.service.credentials.connection.AuthType.Literal.class)))
          .thenReturn(mockCredentialVendors);
      Mockito.when(mockCredentialVendors.isUnsatisfied()).thenReturn(false);
      Mockito.when(mockCredentialVendors.get()).thenReturn(sigV4Vendor);

      PolarisCredentialManager credentialManager =
          new DefaultPolarisCredentialManager(realmContext, mockCredentialVendors);

      StorageAccessConfigProvider storageAccessConfigProvider =
          new StorageAccessConfigProvider(storageCredentialCache, metaStoreManagerFactory);
      FileIOFactory fileIOFactory = fileIOFactorySupplier.apply(storageAccessConfigProvider);

      TaskExecutor taskExecutor = Mockito.mock(TaskExecutor.class);

      PolarisEventListener polarisEventListener = new TestPolarisEventListener();
      CallContextCatalogFactory callContextFactory =
          new PolarisCallContextCatalogFactory(
              diagnostics,
              resolverFactory,
              metaStoreManagerFactory,
              taskExecutor,
              fileIOFactory,
              polarisEventListener);

      ReservedProperties reservedProperties = ReservedProperties.NONE;

      CatalogHandlerUtils catalogHandlerUtils = new CatalogHandlerUtils(realmConfig);

      @SuppressWarnings("unchecked")
      Instance<ExternalCatalogFactory> externalCatalogFactory = Mockito.mock(Instance.class);
      Mockito.when(externalCatalogFactory.select(any())).thenReturn(externalCatalogFactory);
      Mockito.when(externalCatalogFactory.isUnsatisfied()).thenReturn(true);

      IcebergCatalogAdapter catalogService =
          new IcebergCatalogAdapter(
              diagnostics,
              realmContext,
              callContext,
              callContextFactory,
              resolverFactory,
              resolutionManifestFactory,
              metaStoreManager,
              credentialManager,
              authorizer,
              new DefaultCatalogPrefixParser(),
              reservedProperties,
              catalogHandlerUtils,
              externalCatalogFactory,
              polarisEventListener,
              storageAccessConfigProvider);

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
      PolarisPrincipal principal = PolarisPrincipal.of(createdPrincipal.getPrincipal(), Set.of());

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

      PolarisAdminService adminService =
          new PolarisAdminService(
              diagnostics,
              callContext,
              resolutionManifestFactory,
              metaStoreManager,
              userSecretsManager,
              serviceIdentityProvider,
              securityContext,
              authorizer,
              reservedProperties);
      PolarisCatalogsApi catalogsApi =
          new PolarisCatalogsApi(
              new PolarisServiceImpl(
                  realmConfig, reservedProperties, adminService, serviceIdentityProvider));

      return new TestServices(
          clock,
          catalogsApi,
          restApi,
          restConfigurationApi,
          catalogService,
          configurationStore,
          diagnostics,
          storageCredentialCache,
          resolverFactory,
          resolutionManifestFactory,
          metaStoreManagerFactory,
          realmContext,
          realmConfig,
          securityContext,
          metaStoreManager,
          fileIOFactory,
          taskExecutor,
          polarisEventListener,
          storageAccessConfigProvider);
    }
  }

  public PolarisCallContext newCallContext() {
    BasePersistence metaStore = metaStoreManagerFactory.getOrCreateSession(realmContext);
    return new PolarisCallContext(realmContext, metaStore, configurationStore);
  }
}
