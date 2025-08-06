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
package org.apache.polaris.core.storage.cache;

import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.storage.AccessConfig;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.apache.polaris.core.storage.gcp.GcpStorageConfigurationInfo;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class StorageCredentialCacheTest {

  private CallContext callCtx;
  private StorageCredentialCache storageCredentialCache;
  private PolarisStorageIntegrationProvider storageIntegrationProvider;

  @BeforeEach
  public void setup() {
    // to interact with the metastore
    RealmContext realmContext = () -> "testRealm";
    callCtx =
        new CallContext() {
          @Override
          public RealmConfig getRealmConfig() {
            return new RealmConfigImpl(new PolarisConfigurationStore() {}, realmContext);
          }

          @Override
          public RealmContext getRealmContext() {
            return realmContext;
          }

          @Override
          public PolarisCallContext getPolarisCallContext() {
            throw new UnsupportedOperationException();
          }

          @Override
          public CallContext copy() {
            throw new UnsupportedOperationException();
          }
        };

    storageIntegrationProvider = Mockito.mock();
    storageCredentialCache = new StorageCredentialCache(() -> 10_000, storageIntegrationProvider);
  }

  @Test
  public void testBadResult() {
    PolarisStorageIntegration<PolarisStorageConfigurationInfo> integration = mockedIntegration();
    Mockito.when(
            integration.getSubscopedCreds(
                Mockito.any(), Mockito.anyBoolean(), Mockito.anySet(), Mockito.anySet()))
        .thenThrow(new RuntimeException("extra_error_info"));
    Mockito.when(storageIntegrationProvider.getStorageIntegrationForConfig(Mockito.any()))
        .thenReturn(integration);

    Assertions.assertThatThrownBy(
            () ->
                storageCredentialCache.getOrGenerateSubScopeCreds(
                    callCtx,
                    storageConfigs().get(0),
                    true,
                    Set.of("s3://bucket1/path"),
                    Set.of("s3://bucket3/path")))
        .isInstanceOf(UnprocessableEntityException.class)
        .hasMessage("Failed to get subscoped credentials: extra_error_info");
  }

  @Test
  public void testCacheHit() {
    fakedIntegrations(false);

    // add an item to the cache
    storageCredentialCache.getOrGenerateSubScopeCreds(
        callCtx,
        storageConfigs().get(0),
        true,
        Set.of("s3://bucket1/path", "s3://bucket2/path"),
        Set.of("s3://bucket3/path", "s3://bucket4/path"));
    Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(1);

    // subscope for the same entity and same allowed locations, will hit the cache
    storageCredentialCache.getOrGenerateSubScopeCreds(
        callCtx,
        storageConfigs().get(0),
        true,
        Set.of("s3://bucket1/path", "s3://bucket2/path"),
        Set.of("s3://bucket3/path", "s3://bucket4/path"));
    Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(1);
  }

  @RepeatedTest(10)
  public void testCacheEvict() {
    fakedIntegrations(true);

    StorageCredentialCacheKey cacheKey =
        StorageCredentialCacheKey.of(
            callCtx.getRealmContext().getRealmIdentifier(),
            storageConfigs().get(0),
            true,
            Set.of("s3://bucket1/path", "s3://bucket2/path"),
            Set.of("s3://bucket/path"));

    // the entry will be evicted immediately because the token is expired
    storageCredentialCache.getOrGenerateSubScopeCreds(
        callCtx,
        storageConfigs().get(0),
        true,
        Set.of("s3://bucket1/path", "s3://bucket2/path"),
        Set.of("s3://bucket/path"));
    Assertions.assertThat(cacheKey)
        .extracting(storageCredentialCache::isPresent, InstanceOfAssertFactories.BOOLEAN)
        .isFalse();
  }

  @Test
  public void testCacheGenerateNewEntries() {
    fakedIntegrations(false);

    int cacheSize = 0;
    // different catalog will generate new cache entries
    for (var storageConfigStr : storageConfigs()) {
      storageCredentialCache.getOrGenerateSubScopeCreds(
          callCtx,
          storageConfigStr,
          true,
          Set.of("s3://bucket1/path", "s3://bucket2/path"),
          Set.of("s3://bucket/path"));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(++cacheSize);
    }
    // allowedListAction changed to different value FALSE, will generate new entry
    for (var storageConfigStr : storageConfigs()) {
      storageCredentialCache.getOrGenerateSubScopeCreds(
          callCtx,
          /* allowedListAction= */ storageConfigStr,
          false,
          Set.of("s3://bucket1/path", "s3://bucket2/path"),
          Set.of("s3://bucket/path"));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(++cacheSize);
    }
    // different allowedWriteLocations, will generate new entry
    for (var storageConfigStr : storageConfigs()) {
      storageCredentialCache.getOrGenerateSubScopeCreds(
          callCtx,
          /* allowedListAction= */ storageConfigStr,
          false,
          Set.of("s3://bucket1/path", "s3://bucket2/path"),
          Set.of("s3://differentbucket/path"));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(++cacheSize);
    }
    // different allowedReadLocations, will generate new try
    for (var storageConfigStr : storageConfigs()) {
      storageCredentialCache.getOrGenerateSubScopeCreds(
          callCtx,
          /* allowedListAction= */ storageConfigStr,
          false,
          Set.of("s3://differentbucket/path", "s3://bucket2/path"),
          Set.of("s3://bucket/path"));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(++cacheSize);
    }
  }

  @Test
  public void testCacheNotAffectedBy() {
    fakedIntegrations(false);

    List<PolarisStorageConfigurationInfo> configValues = storageConfigs();
    for (var storageConfigStr : configValues) {
      storageCredentialCache.getOrGenerateSubScopeCreds(
          callCtx,
          storageConfigStr,
          true,
          Set.of("s3://bucket1/path", "s3://bucket2/path"),
          Set.of("s3://bucket3/path", "s3://bucket4/path"));
    }
    Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(configValues.size());

    // order of the allowedReadLocations does not affect the cache
    for (var storageConfigStr : configValues) {
      storageCredentialCache.getOrGenerateSubScopeCreds(
          callCtx,
          storageConfigStr,
          true,
          Set.of("s3://bucket2/path", "s3://bucket1/path"),
          Set.of("s3://bucket3/path", "s3://bucket4/path"));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize())
          .isEqualTo(configValues.size());
    }

    // order of the allowedWriteLocations does not affect the cache
    for (var storageConfigStr : configValues) {
      storageCredentialCache.getOrGenerateSubScopeCreds(
          callCtx,
          storageConfigStr,
          true,
          Set.of("s3://bucket2/path", "s3://bucket1/path"),
          Set.of("s3://bucket4/path", "s3://bucket3/path"));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize())
          .isEqualTo(configValues.size());
    }
  }

  private void fakedIntegrations(boolean expireSoon) {
    List<AccessConfig> mockedAccessConfigs = getFakeAccessConfigs(expireSoon);
    var integrations =
        mockedAccessConfigs.stream()
            .map(
                accessConfig -> {
                  var integration = mockedIntegration();
                  Mockito.when(
                          integration.getSubscopedCreds(
                              Mockito.any(),
                              Mockito.anyBoolean(),
                              Mockito.anySet(),
                              Mockito.anySet()))
                      .thenReturn(accessConfig);
                  return integration;
                })
            .collect(Collectors.toList());
    Mockito.when(storageIntegrationProvider.getStorageIntegrationForConfig(Mockito.any()))
        .thenReturn(integrations.get(0))
        .thenReturn(integrations.get(1))
        .thenReturn(integrations.get(2));
  }

  private static List<AccessConfig> getFakeAccessConfigs(boolean expireSoon) {
    // NOTE: The default behavior of the Caffeine cache seems to have a bug; if our
    // expireAfter definition in the StorageCredentialCache constructor doesn't clip
    // the returned time to minimum of 0, and we set the expiration time to more than
    // 1 second in the past, it seems the cache fails to remove the expired entries
    // no matter how long we wait. This is possibly related to the implementation-specific
    // "minimum difference between the scheduled executions" documented in Caffeine.java
    // to be 1 second.
    String expireTime =
        expireSoon
            ? String.valueOf(System.currentTimeMillis() - 100)
            : String.valueOf(Long.MAX_VALUE);
    List<AccessConfig> res = new ArrayList<>();
    res.add(
        AccessConfig.builder()
            .put(StorageAccessProperty.AWS_KEY_ID, "key_id_1")
            .put(StorageAccessProperty.AWS_SECRET_KEY, "key_secret_1")
            .put(StorageAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS, expireTime)
            .put(StorageAccessProperty.EXPIRATION_TIME, expireTime)
            .build());
    res.add(
        AccessConfig.builder()
            .put(StorageAccessProperty.AZURE_SAS_TOKEN, "sas_token_1")
            .put(StorageAccessProperty.EXPIRATION_TIME, expireTime)
            .build());
    res.add(
        AccessConfig.builder()
            .put(StorageAccessProperty.GCS_ACCESS_TOKEN, "gcs_token_1")
            .put(StorageAccessProperty.GCS_ACCESS_TOKEN_EXPIRES_AT, expireTime)
            .build());
    return res;
  }

  @Nonnull
  private static List<PolarisStorageConfigurationInfo> storageConfigs() {
    return List.of(
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocation("s3://foo/bar")
            .roleARN("arn:foo")
            .region("no-where-1")
            .build(),
        GcpStorageConfigurationInfo.builder().addAllowedLocation("gs://foo/bar").build(),
        AzureStorageConfigurationInfo.builder()
            .addAllowedLocation("abfs://foo@baz.foo/bar")
            .tenantId("someone")
            .build());
  }

  @Test
  public void testExtraProperties() {
    var accessConfig =
        AccessConfig.builder()
            .put(StorageAccessProperty.AWS_SECRET_KEY, "super-secret-123")
            .put(StorageAccessProperty.AWS_ENDPOINT, "test-endpoint1")
            .put(StorageAccessProperty.AWS_PATH_STYLE_ACCESS, "true")
            .build();
    var integration = mockedIntegration();
    Mockito.when(
            integration.getSubscopedCreds(
                Mockito.any(), Mockito.anyBoolean(), Mockito.anySet(), Mockito.anySet()))
        .thenReturn(accessConfig);
    Mockito.when(storageIntegrationProvider.getStorageIntegrationForConfig(Mockito.any()))
        .thenReturn(integration);

    List<PolarisStorageConfigurationInfo> configValues = storageConfigs();

    AccessConfig config =
        storageCredentialCache.getOrGenerateSubScopeCreds(
            callCtx,
            configValues.get(0),
            true,
            Set.of("s3://bucket1/path", "s3://bucket2/path"),
            Set.of("s3://bucket3/path", "s3://bucket4/path"));
    Assertions.assertThat(config.credentials())
        .containsExactly(Map.entry("s3.secret-access-key", "super-secret-123"));
    Assertions.assertThat(config.extraProperties())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of("s3.endpoint", "test-endpoint1", "s3.path-style-access", "true"));
  }

  @SuppressWarnings("unchecked")
  private static PolarisStorageIntegration<PolarisStorageConfigurationInfo> mockedIntegration() {
    return Mockito.mock(PolarisStorageIntegration.class);
  }
}
