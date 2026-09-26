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
package org.apache.polaris.core.storage;

import static org.apache.polaris.core.config.RealmConfigurationSource.EMPTY_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.core.storage.cache.StorageCredentialCacheConfig;
import org.apache.polaris.core.storage.cache.StorageCredentialCacheKey;
import org.apache.polaris.core.storage.gcp.GcpStorageConfigurationInfo;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CachingStorageIntegrationTest {

  private static final RealmContext REALM_CONTEXT = () -> "testRealm";
  private static final RealmConfig REALM_CONFIG = new RealmConfigImpl(EMPTY_CONFIG, REALM_CONTEXT);

  @ParameterizedTest
  @MethodSource("storageConfigurationsAndRefreshEndpointProperties")
  void cachesCredentialsIndependentlyOfRefreshEndpoint(
      PolarisStorageConfigurationInfo storageConfig,
      StorageAccessProperty refreshEndpointProperty) {
    StorageCredentialCacheConfig cacheConfig = () -> 10_000;
    AtomicInteger loadCount = new AtomicInteger();
    TestCachingStorageIntegration integration =
        new TestCachingStorageIntegration(
            new StorageCredentialCache(cacheConfig), storageConfig, loadCount);

    StorageAccessConfig serverConfig =
        integration.getStorageAccessConfig(
            List.of(), Optional.empty(), CredentialVendingContext.empty());
    StorageAccessConfig firstClientConfig =
        integration.getStorageAccessConfig(
            List.of(), Optional.of("/v1/first/credentials"), CredentialVendingContext.empty());
    StorageAccessConfig secondClientConfig =
        integration.getStorageAccessConfig(
            List.of(), Optional.of("/v1/second/credentials"), CredentialVendingContext.empty());

    assertThat(loadCount).hasValue(1);
    assertThat(serverConfig.get(refreshEndpointProperty)).isNull();
    assertThat(firstClientConfig.get(refreshEndpointProperty)).isEqualTo("/v1/first/credentials");
    assertThat(secondClientConfig.get(refreshEndpointProperty)).isEqualTo("/v1/second/credentials");
  }

  private static Stream<Arguments> storageConfigurationsAndRefreshEndpointProperties() {
    return Stream.of(
        Arguments.of(
            AwsStorageConfigurationInfo.builder().addAllowedLocation("s3://bucket").build(),
            StorageAccessProperty.AWS_REFRESH_CREDENTIALS_ENDPOINT),
        Arguments.of(
            GcpStorageConfigurationInfo.builder().addAllowedLocation("gs://bucket").build(),
            StorageAccessProperty.GCS_REFRESH_CREDENTIALS_ENDPOINT),
        Arguments.of(
            AzureStorageConfigurationInfo.builder()
                .addAllowedLocation("abfs://container@account.example/")
                .tenantId("tenant-id")
                .build(),
            StorageAccessProperty.AZURE_REFRESH_CREDENTIALS_ENDPOINT));
  }

  private static final class TestCachingStorageIntegration
      extends CachingStorageIntegration<PolarisStorageConfigurationInfo> {

    private final AtomicInteger loadCount;

    private TestCachingStorageIntegration(
        StorageCredentialCache cache,
        PolarisStorageConfigurationInfo storageConfig,
        AtomicInteger loadCount) {
      super(cache, REALM_CONFIG, storageConfig);
      this.loadCount = loadCount;
    }

    @Override
    protected StorageCredentialCacheKey buildCacheKey(
        @NonNull List<LocationGrant> grants,
        @NonNull Optional<String> refreshEndpoint,
        @NonNull CredentialVendingContext context) {
      assertThat(refreshEndpoint).isEmpty();
      return new TestStorageCredentialCacheKey(REALM_CONFIG, loadCount);
    }
  }

  private record TestStorageCredentialCacheKey(RealmConfig realmConfig, AtomicInteger loadCount)
      implements StorageCredentialCacheKey {

    @Override
    public StorageAccessConfig load() {
      loadCount.incrementAndGet();
      return StorageAccessConfig.builder().putCredential("key", "value").build();
    }
  }
}
