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
package org.apache.polaris.service.storage;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.persistence.BaseMetaStoreManager;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.aws.AwsCredentialsStorageIntegration;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.StsClientProvider;
import org.apache.polaris.core.storage.azure.AzureCredentialsStorageIntegration;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.core.storage.cache.StorageCredentialCacheKey;
import org.apache.polaris.core.storage.gcp.GcpCredentialsStorageIntegration;
import org.apache.polaris.core.storage.gcp.GcpStorageConfigurationInfo;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Provider that returns a {@link PolarisStorageIntegration} for a resolved entity path.
 *
 * <p>Integration instances are bound to a specific {@link PolarisStorageConfigurationInfo} at
 * construction and cached per-config in an internal map: every unique config gets exactly one
 * integration instance for the lifetime of the provider. Construction is driven by a per-storage-
 * type factory so we can still share expensive request-scope state (STS client provider, GCP
 * credentials supplier, etc.) across all instances of the same backend.
 */
@ApplicationScoped
public class PolarisStorageIntegrationProviderImpl implements PolarisStorageIntegrationProvider {

  private final PolarisDiagnostics diagnostics;
  private final RealmConfig realmConfig;
  private final Function<AwsStorageConfigurationInfo, AwsCredentialsStorageIntegration> awsFactory;
  private final Function<GcpStorageConfigurationInfo, GcpCredentialsStorageIntegration> gcpFactory;
  private final Function<AzureStorageConfigurationInfo, AzureCredentialsStorageIntegration>
      azureFactory;

  private final ConcurrentMap<PolarisStorageConfigurationInfo, PolarisStorageIntegration<?>>
      integrationCache = new ConcurrentHashMap<>();

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  public PolarisStorageIntegrationProviderImpl(
      StorageConfiguration storageConfiguration,
      StsClientProvider stsClientProvider,
      RealmConfig realmConfig,
      Clock clock,
      StorageCredentialCache cache,
      PolarisDiagnostics diagnostics) {
    this.diagnostics = diagnostics;
    this.realmConfig = realmConfig;
    this.awsFactory =
        storageConfig ->
            new AwsCredentialsStorageIntegration(
                stsClientProvider,
                config -> {
                  if (realmConfig.getConfig(
                      FeatureConfiguration.RESOLVE_CREDENTIALS_BY_STORAGE_NAME)) {
                    return Optional.of(
                        storageConfiguration.stsCredentials(config.getStorageName()));
                  }
                  return Optional.of(storageConfiguration.stsCredentials());
                },
                cache,
                storageConfig,
                realmConfig);
    Supplier<GoogleCredentials> gcpCredsProvider =
        storageConfiguration.gcpCredentialsSupplier(clock);
    this.gcpFactory =
        storageConfig ->
            new GcpCredentialsStorageIntegration(
                gcpCredsProvider.get(),
                ServiceOptions.getFromServiceLoader(
                    HttpTransportFactory.class, NetHttpTransport::new),
                cache,
                storageConfig,
                realmConfig);
    this.azureFactory =
        storageConfig -> new AzureCredentialsStorageIntegration(cache, storageConfig, realmConfig);
  }

  public PolarisStorageIntegrationProviderImpl(
      StsClientProvider stsClientProvider,
      Optional<AwsCredentialsProvider> stsCredentials,
      Supplier<GoogleCredentials> gcpCredsProvider,
      StorageCredentialCache cache,
      RealmConfig realmConfig,
      PolarisDiagnostics diagnostics) {
    this.diagnostics = diagnostics;
    this.realmConfig = realmConfig;
    this.awsFactory =
        storageConfig ->
            new AwsCredentialsStorageIntegration(
                stsClientProvider, config -> stsCredentials, cache, storageConfig, realmConfig);
    this.gcpFactory =
        storageConfig ->
            new GcpCredentialsStorageIntegration(
                gcpCredsProvider.get(),
                ServiceOptions.getFromServiceLoader(
                    HttpTransportFactory.class, NetHttpTransport::new),
                cache,
                storageConfig,
                realmConfig);
    this.azureFactory =
        storageConfig -> new AzureCredentialsStorageIntegration(cache, storageConfig, realmConfig);
  }

  @Override
  public @Nullable PolarisStorageIntegration<?> getStorageIntegration(
      @Nonnull List<PolarisEntity> resolvedEntityPath) {
    return PolarisStorageConfigurationInfo.findStorageInfoFromHierarchy(resolvedEntityPath)
        .map(entity -> BaseMetaStoreManager.extractStorageConfiguration(diagnostics, entity))
        .map(this::getOrCreateIntegration)
        .orElse(null);
  }

  private PolarisStorageIntegration<?> getOrCreateIntegration(
      PolarisStorageConfigurationInfo storageConfig) {
    return integrationCache.computeIfAbsent(storageConfig, this::createIntegration);
  }

  private PolarisStorageIntegration<?> createIntegration(
      PolarisStorageConfigurationInfo storageConfig) {
    return switch (storageConfig.getStorageType()) {
      case S3 -> awsFactory.apply((AwsStorageConfigurationInfo) storageConfig);
      case GCS -> gcpFactory.apply((GcpStorageConfigurationInfo) storageConfig);
      case AZURE -> azureFactory.apply((AzureStorageConfigurationInfo) storageConfig);
      case FILE -> createFileIntegration(storageConfig, realmConfig);
      default ->
          throw new IllegalArgumentException(
              "Unknown storage type " + storageConfig.getStorageType());
    };
  }

  private static PolarisStorageIntegration<?> createFileIntegration(
      PolarisStorageConfigurationInfo storageConfig, RealmConfig realmConfig) {
    return new PolarisStorageIntegration<PolarisStorageConfigurationInfo>(
        "file", null, realmConfig, storageConfig) {
      @Override
      protected StorageCredentialCacheKey buildCacheKey(
          boolean allowList,
          @Nonnull Set<String> readLocations,
          @Nonnull Set<String> writeLocations,
          @Nonnull Optional<String> refreshEndpoint,
          @Nonnull CredentialVendingContext context) {
        return null; // FILE does not support credential vending
      }

      @Override
      public StorageAccessConfig getSubscopedCreds(
          boolean allowList,
          @Nonnull Set<String> readLocations,
          @Nonnull Set<String> writeLocations,
          @Nonnull Optional<String> refreshEndpoint,
          @Nonnull CredentialVendingContext context) {
        return StorageAccessConfig.builder().supportsCredentialVending(false).build();
      }

      @Override
      public @Nonnull Map<String, Map<PolarisStorageActions, ValidationResult>>
          validateAccessToLocations(
              @Nonnull RealmConfig realmConfig,
              @Nonnull PolarisStorageConfigurationInfo storageConfig,
              @Nonnull Set<PolarisStorageActions> actions,
              @Nonnull Set<String> locations) {
        return Map.of();
      }
    };
  }
}
