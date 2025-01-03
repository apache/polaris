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
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.storage.PolarisCredentialProperty;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.aws.AwsCredentialsStorageIntegration;
import org.apache.polaris.core.storage.azure.AzureCredentialsStorageIntegration;
import org.apache.polaris.core.storage.gcp.GcpCredentialsStorageIntegration;
import software.amazon.awssdk.services.sts.StsClient;

@ApplicationScoped
public class PolarisStorageIntegrationProviderImpl implements PolarisStorageIntegrationProvider {

  private final Supplier<StsClient> stsClientSupplier;
  private final Supplier<GoogleCredentials> gcpCredsProvider;

  @Inject
  public PolarisStorageIntegrationProviderImpl(StorageConfiguration storageConfiguration) {
    this(storageConfiguration.stsClientSupplier(), storageConfiguration.gcpCredentialsSupplier());
  }

  public PolarisStorageIntegrationProviderImpl(
      Supplier<StsClient> stsClientSupplier, Supplier<GoogleCredentials> gcpCredsProvider) {
    this.stsClientSupplier = stsClientSupplier;
    this.gcpCredsProvider = gcpCredsProvider;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends PolarisStorageConfigurationInfo> @Nullable
      PolarisStorageIntegration<T> getStorageIntegrationForConfig(
          PolarisStorageConfigurationInfo polarisStorageConfigurationInfo) {
    if (polarisStorageConfigurationInfo == null) {
      return null;
    }
    PolarisStorageIntegration<T> storageIntegration;
    switch (polarisStorageConfigurationInfo.getStorageType()) {
      case S3:
        storageIntegration =
            (PolarisStorageIntegration<T>)
                new AwsCredentialsStorageIntegration(stsClientSupplier.get());
        break;
      case GCS:
        storageIntegration =
            (PolarisStorageIntegration<T>)
                new GcpCredentialsStorageIntegration(
                    gcpCredsProvider.get(),
                    ServiceOptions.getFromServiceLoader(
                        HttpTransportFactory.class, NetHttpTransport::new));
        break;
      case AZURE:
        storageIntegration =
            (PolarisStorageIntegration<T>) new AzureCredentialsStorageIntegration();
        break;
      case FILE:
        storageIntegration =
            new PolarisStorageIntegration<>("file") {
              @Override
              public EnumMap<PolarisCredentialProperty, String> getSubscopedCreds(
                  @Nonnull PolarisDiagnostics diagnostics,
                  @Nonnull T storageConfig,
                  boolean allowListOperation,
                  @Nonnull Set<String> allowedReadLocations,
                  @Nonnull Set<String> allowedWriteLocations) {
                return new EnumMap<>(PolarisCredentialProperty.class);
              }

              @Override
              public @Nonnull Map<String, Map<PolarisStorageActions, ValidationResult>>
                  validateAccessToLocations(
                      @Nonnull T storageConfig,
                      @Nonnull Set<PolarisStorageActions> actions,
                      @Nonnull Set<String> locations) {
                return Map.of();
              }
            };
        break;
      default:
        throw new IllegalArgumentException(
            "Unknown storage type " + polarisStorageConfigurationInfo.getStorageType());
    }
    return storageIntegration;
  }
}
