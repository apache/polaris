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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.aws.AwsCredentialsStorageIntegration;
import org.apache.polaris.core.storage.aws.StsClientProvider;
import org.apache.polaris.core.storage.azure.AzureCredentialsStorageIntegration;
import org.apache.polaris.core.storage.cache.StorageAccessConfigParameters;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.core.storage.gcp.GcpCredentialsStorageIntegration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

@ApplicationScoped
public class PolarisStorageIntegrationProviderImpl implements PolarisStorageIntegrationProvider {

  private final AwsCredentialsStorageIntegration awsIntegration;
  private volatile GcpCredentialsStorageIntegration gcpIntegration;
  private volatile AzureCredentialsStorageIntegration azureIntegration;
  private final PolarisStorageIntegration<?> fileIntegration;
  private final Supplier<GcpCredentialsStorageIntegration> gcpIntegrationSupplier;
  private final Supplier<AzureCredentialsStorageIntegration> azureIntegrationSupplier;

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  public PolarisStorageIntegrationProviderImpl(
      StorageConfiguration storageConfiguration,
      StsClientProvider stsClientProvider,
      RealmConfig realmConfig,
      Clock clock,
      StorageCredentialCache cache) {
    Supplier<RealmConfig> realmConfigSupplier = () -> realmConfig;
    this.awsIntegration =
        new AwsCredentialsStorageIntegration(
            stsClientProvider,
            config -> {
              if (realmConfig.getConfig(FeatureConfiguration.RESOLVE_CREDENTIALS_BY_STORAGE_NAME)) {
                return Optional.of(storageConfiguration.stsCredentials(config.getStorageName()));
              }
              return Optional.of(storageConfiguration.stsCredentials());
            },
            cache,
            realmConfigSupplier);
    Supplier<GoogleCredentials> gcpCredsProvider =
        storageConfiguration.gcpCredentialsSupplier(clock);
    this.gcpIntegrationSupplier =
        () ->
            new GcpCredentialsStorageIntegration(
                gcpCredsProvider.get(),
                ServiceOptions.getFromServiceLoader(
                    HttpTransportFactory.class, NetHttpTransport::new),
                cache,
                realmConfigSupplier);
    this.azureIntegrationSupplier =
        () -> new AzureCredentialsStorageIntegration(cache, realmConfigSupplier);
    this.fileIntegration = createFileIntegration();
  }

  public PolarisStorageIntegrationProviderImpl(
      StsClientProvider stsClientProvider,
      Optional<AwsCredentialsProvider> stsCredentials,
      Supplier<GoogleCredentials> gcpCredsProvider,
      StorageCredentialCache cache,
      Supplier<RealmConfig> realmConfigSupplier) {
    this.awsIntegration =
        new AwsCredentialsStorageIntegration(
            stsClientProvider, config -> stsCredentials, cache, realmConfigSupplier);
    this.gcpIntegrationSupplier =
        () ->
            new GcpCredentialsStorageIntegration(
                gcpCredsProvider.get(),
                ServiceOptions.getFromServiceLoader(
                    HttpTransportFactory.class, NetHttpTransport::new),
                cache,
                realmConfigSupplier);
    this.azureIntegrationSupplier =
        () -> new AzureCredentialsStorageIntegration(cache, realmConfigSupplier);
    this.fileIntegration = createFileIntegration();
  }

  private GcpCredentialsStorageIntegration getGcpIntegration() {
    if (gcpIntegration == null) {
      synchronized (this) {
        if (gcpIntegration == null) {
          gcpIntegration = gcpIntegrationSupplier.get();
        }
      }
    }
    return gcpIntegration;
  }

  private AzureCredentialsStorageIntegration getAzureIntegration() {
    if (azureIntegration == null) {
      synchronized (this) {
        if (azureIntegration == null) {
          azureIntegration = azureIntegrationSupplier.get();
        }
      }
    }
    return azureIntegration;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends PolarisStorageConfigurationInfo>
      @Nullable PolarisStorageIntegration<T> getStorageIntegrationForConfig(
          PolarisStorageConfigurationInfo polarisStorageConfigurationInfo) {
    if (polarisStorageConfigurationInfo == null) {
      return null;
    }
    return (PolarisStorageIntegration<T>)
        switch (polarisStorageConfigurationInfo.getStorageType()) {
          case S3 -> awsIntegration;
          case GCS -> getGcpIntegration();
          case AZURE -> getAzureIntegration();
          case FILE -> fileIntegration;
          default ->
              throw new IllegalArgumentException(
                  "Unknown storage type " + polarisStorageConfigurationInfo.getStorageType());
        };
  }

  private static PolarisStorageIntegration<?> createFileIntegration() {
    return new PolarisStorageIntegration<PolarisStorageConfigurationInfo>("file") {
      @Override
      protected StorageAccessConfigParameters buildCacheKey(
          @Nonnull PolarisEntity entity,
          @Nonnull RealmConfig realmConfig,
          boolean allowList,
          @Nonnull Set<String> readLocations,
          @Nonnull Set<String> writeLocations,
          @Nonnull Optional<String> refreshEndpoint,
          @Nonnull CredentialVendingContext context) {
        return null; // FILE does not support credential vending
      }

      @Override
      public StorageAccessConfig getSubscopedCreds(
          @Nonnull RealmConfig realmConfig,
          @Nonnull PolarisEntity entity,
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
