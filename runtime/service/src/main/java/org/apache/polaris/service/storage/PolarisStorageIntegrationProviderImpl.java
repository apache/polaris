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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.LocationGrant;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageConfigOverrideResolver;
import org.apache.polaris.core.storage.aws.AwsCredentialsStorageIntegration;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.StsClientProvider;
import org.apache.polaris.core.storage.azure.AzureCredentialsStorageIntegration;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.core.storage.gcp.GcpCredentialsStorageIntegration;
import org.apache.polaris.core.storage.gcp.GcpStorageConfigurationInfo;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Provider that returns a {@link PolarisStorageIntegration} for a resolved entity path. A fresh
 * integration is constructed per call; the per-config state inside each integration is a thin
 * wrapper over shared, application-scoped resources (STS client provider, GCP credentials supplier,
 * Google HTTP transport factory, request-scope realm config).
 */
@ApplicationScoped
public class PolarisStorageIntegrationProviderImpl implements PolarisStorageIntegrationProvider {

  private final PolarisDiagnostics diagnostics;
  private final Function<AwsStorageConfigurationInfo, AwsCredentialsStorageIntegration> awsFactory;
  private final Function<GcpStorageConfigurationInfo, GcpCredentialsStorageIntegration> gcpFactory;
  private final Function<AzureStorageConfigurationInfo, AzureCredentialsStorageIntegration>
      azureFactory;

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
    HttpTransportFactory gcpTransportFactory =
        ServiceOptions.getFromServiceLoader(HttpTransportFactory.class, NetHttpTransport::new);
    this.gcpFactory =
        storageConfig ->
            new GcpCredentialsStorageIntegration(
                gcpCredsProvider.get(), gcpTransportFactory, cache, storageConfig, realmConfig);
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
    this.awsFactory =
        storageConfig ->
            new AwsCredentialsStorageIntegration(
                stsClientProvider, config -> stsCredentials, cache, storageConfig, realmConfig);
    HttpTransportFactory gcpTransportFactory =
        ServiceOptions.getFromServiceLoader(HttpTransportFactory.class, NetHttpTransport::new);
    this.gcpFactory =
        storageConfig ->
            new GcpCredentialsStorageIntegration(
                gcpCredsProvider.get(), gcpTransportFactory, cache, storageConfig, realmConfig);
    this.azureFactory =
        storageConfig -> new AzureCredentialsStorageIntegration(cache, storageConfig, realmConfig);
  }

  @Override
  public @Nullable PolarisStorageIntegration getStorageIntegration(
      @NonNull List<PolarisEntity> resolvedEntityPath) {
    return StorageConfigOverrideResolver.resolveEffectiveConfig(resolvedEntityPath)
        .map(this::createIntegration)
        .orElse(null);
  }

  private PolarisStorageIntegration createIntegration(
      PolarisStorageConfigurationInfo storageConfig) {
    return switch (storageConfig.getStorageType()) {
      case S3 -> awsFactory.apply((AwsStorageConfigurationInfo) storageConfig);
      case GCS -> gcpFactory.apply((GcpStorageConfigurationInfo) storageConfig);
      case AZURE -> azureFactory.apply((AzureStorageConfigurationInfo) storageConfig);
      case FILE -> FILE_INTEGRATION;
    };
  }

  /**
   * Singleton integration for FILE storage. FILE backends do not support credential vending, so
   * there's no per-config state and no caching to worry about.
   */
  private static final PolarisStorageIntegration FILE_INTEGRATION =
      new PolarisStorageIntegration() {
        @Override
        public StorageAccessConfig getStorageAccessConfig(
            @NonNull List<LocationGrant> grants,
            @NonNull Optional<String> refreshEndpoint,
            @NonNull CredentialVendingContext context) {
          return StorageAccessConfig.builder().supportsCredentialVending(false).build();
        }
      };
}
