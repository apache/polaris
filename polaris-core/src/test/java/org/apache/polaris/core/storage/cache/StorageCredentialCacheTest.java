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

import static org.apache.polaris.core.persistence.PrincipalSecretsGenerator.RANDOM_SECRETS;

import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.ScopedCredentialsResult;
import org.apache.polaris.core.persistence.transactional.TransactionalPersistence;
import org.apache.polaris.core.persistence.transactional.TreeMapMetaStore;
import org.apache.polaris.core.persistence.transactional.TreeMapTransactionalPersistenceImpl;
import org.apache.polaris.core.storage.AccessConfig;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
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

  private PolarisCallContext callCtx;
  private PolarisMetaStoreManager metaStoreManager;
  private StorageCredentialCache storageCredentialCache;

  @BeforeEach
  public void setup() {
    // diag services
    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    // the entity store, use treemap implementation
    TreeMapMetaStore store = new TreeMapMetaStore(diagServices);
    // to interact with the metastore
    TransactionalPersistence metaStore =
        new TreeMapTransactionalPersistenceImpl(store, RANDOM_SECRETS);
    callCtx = new PolarisCallContext(() -> "testRealm", metaStore, diagServices);
    metaStoreManager = Mockito.mock(PolarisMetaStoreManager.class);
    storageCredentialCache = new StorageCredentialCache(() -> 10_000);
  }

  @Test
  public void testBadResult() {
    ScopedCredentialsResult badResult =
        new ScopedCredentialsResult(
            BaseResult.ReturnStatus.SUBSCOPE_CREDS_ERROR, "extra_error_info");
    Mockito.when(
            metaStoreManager.getSubscopedCredsForEntity(
                Mockito.any(),
                Mockito.any(),
                Mockito.anyBoolean(),
                Mockito.anySet(),
                Mockito.anySet()))
        .thenReturn(badResult);
    Assertions.assertThatThrownBy(
            () ->
                storageCredentialCache.getOrGenerateSubScopeCreds(
                    metaStoreManager,
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
    List<ScopedCredentialsResult> mockedScopedCreds =
        getFakeScopedCreds(3, /* expireSoon= */ false);
    Mockito.when(
            metaStoreManager.getSubscopedCredsForEntity(
                Mockito.any(),
                Mockito.any(),
                Mockito.anyBoolean(),
                Mockito.anySet(),
                Mockito.anySet()))
        .thenReturn(mockedScopedCreds.get(0))
        .thenReturn(mockedScopedCreds.get(1))
        .thenReturn(mockedScopedCreds.get(1));

    // add an item to the cache
    storageCredentialCache.getOrGenerateSubScopeCreds(
        metaStoreManager,
        callCtx,
        storageConfigs().get(0),
        true,
        Set.of("s3://bucket1/path", "s3://bucket2/path"),
        Set.of("s3://bucket3/path", "s3://bucket4/path"));
    Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(1);

    // subscope for the same entity and same allowed locations, will hit the cache
    storageCredentialCache.getOrGenerateSubScopeCreds(
        metaStoreManager,
        callCtx,
        storageConfigs().get(0),
        true,
        Set.of("s3://bucket1/path", "s3://bucket2/path"),
        Set.of("s3://bucket3/path", "s3://bucket4/path"));
    Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(1);
  }

  @RepeatedTest(10)
  public void testCacheEvict() {
    List<ScopedCredentialsResult> mockedScopedCreds = getFakeScopedCreds(3, /* expireSoon= */ true);

    Mockito.when(
            metaStoreManager.getSubscopedCredsForEntity(
                Mockito.any(),
                Mockito.any(),
                Mockito.anyBoolean(),
                Mockito.anySet(),
                Mockito.anySet()))
        .thenReturn(mockedScopedCreds.get(0))
        .thenReturn(mockedScopedCreds.get(1))
        .thenReturn(mockedScopedCreds.get(2));

    StorageCredentialCacheKey cacheKey =
        StorageCredentialCacheKey.of(
            callCtx.getRealmContext().getRealmIdentifier(),
            storageConfigs().get(0),
            true,
            Set.of("s3://bucket1/path", "s3://bucket2/path"),
            Set.of("s3://bucket/path"));

    // the entry will be evicted immediately because the token is expired
    storageCredentialCache.getOrGenerateSubScopeCreds(
        metaStoreManager,
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
    List<ScopedCredentialsResult> mockedScopedCreds =
        getFakeScopedCreds(3, /* expireSoon= */ false);
    Mockito.when(
            metaStoreManager.getSubscopedCredsForEntity(
                Mockito.any(),
                Mockito.any(),
                Mockito.anyBoolean(),
                Mockito.anySet(),
                Mockito.anySet()))
        .thenReturn(mockedScopedCreds.get(0))
        .thenReturn(mockedScopedCreds.get(1))
        .thenReturn(mockedScopedCreds.get(2));
    int cacheSize = 0;
    // different catalog will generate new cache entries
    for (var storageConfigStr : storageConfigs()) {
      storageCredentialCache.getOrGenerateSubScopeCreds(
          metaStoreManager,
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
          metaStoreManager,
          callCtx,
          storageConfigStr,
          /* allowedListAction= */ false,
          Set.of("s3://bucket1/path", "s3://bucket2/path"),
          Set.of("s3://bucket/path"));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(++cacheSize);
    }
    // different allowedWriteLocations, will generate new entry
    for (var storageConfigStr : storageConfigs()) {
      storageCredentialCache.getOrGenerateSubScopeCreds(
          metaStoreManager,
          callCtx,
          storageConfigStr,
          /* allowedListAction= */ false,
          Set.of("s3://bucket1/path", "s3://bucket2/path"),
          Set.of("s3://differentbucket/path"));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(++cacheSize);
    }
    // different allowedReadLocations, will generate new try
    for (var storageConfigStr : storageConfigs()) {
      storageCredentialCache.getOrGenerateSubScopeCreds(
          metaStoreManager,
          callCtx,
          storageConfigStr,
          /* allowedListAction= */ false,
          Set.of("s3://differentbucket/path", "s3://bucket2/path"),
          Set.of("s3://bucket/path"));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(++cacheSize);
    }
  }

  @Test
  public void testCacheNotAffectedBy() {
    List<ScopedCredentialsResult> mockedScopedCreds =
        getFakeScopedCreds(3, /* expireSoon= */ false);

    Mockito.when(
            metaStoreManager.getSubscopedCredsForEntity(
                Mockito.any(),
                Mockito.any(),
                Mockito.anyBoolean(),
                Mockito.anySet(),
                Mockito.anySet()))
        .thenReturn(mockedScopedCreds.get(0))
        .thenReturn(mockedScopedCreds.get(1))
        .thenReturn(mockedScopedCreds.get(2));
    List<PolarisStorageConfigurationInfo> configValues = storageConfigs();
    for (var storageConfigStr : configValues) {
      storageCredentialCache.getOrGenerateSubScopeCreds(
          metaStoreManager,
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
          metaStoreManager,
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
          metaStoreManager,
          callCtx,
          storageConfigStr,
          true,
          Set.of("s3://bucket2/path", "s3://bucket1/path"),
          Set.of("s3://bucket4/path", "s3://bucket3/path"));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize())
          .isEqualTo(configValues.size());
    }
  }

  private static List<ScopedCredentialsResult> getFakeScopedCreds(int number, boolean expireSoon) {
    List<ScopedCredentialsResult> res = new ArrayList<>();
    for (int i = 1; i <= number; i = i + 3) {
      int finalI = i;
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
      res.add(
          new ScopedCredentialsResult(
              AccessConfig.builder()
                  .put(StorageAccessProperty.AWS_KEY_ID, "key_id_" + finalI)
                  .put(StorageAccessProperty.AWS_SECRET_KEY, "key_secret_" + finalI)
                  .put(StorageAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS, expireTime)
                  .put(StorageAccessProperty.EXPIRATION_TIME, expireTime)
                  .build()));
      if (res.size() == number) return res;
      res.add(
          new ScopedCredentialsResult(
              AccessConfig.builder()
                  .put(StorageAccessProperty.AZURE_SAS_TOKEN, "sas_token_" + finalI)
                  .put(StorageAccessProperty.EXPIRATION_TIME, expireTime)
                  .build()));
      if (res.size() == number) return res;
      res.add(
          new ScopedCredentialsResult(
              AccessConfig.builder()
                  .put(StorageAccessProperty.GCS_ACCESS_TOKEN, "gcs_token_" + finalI)
                  .put(StorageAccessProperty.GCS_ACCESS_TOKEN_EXPIRES_AT, expireTime)
                  .build()));
    }
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
    ScopedCredentialsResult properties =
        new ScopedCredentialsResult(
            AccessConfig.builder()
                .put(StorageAccessProperty.AWS_SECRET_KEY, "super-secret-123")
                .put(StorageAccessProperty.AWS_ENDPOINT, "test-endpoint1")
                .put(StorageAccessProperty.AWS_PATH_STYLE_ACCESS, "true")
                .build());
    Mockito.when(
            metaStoreManager.getSubscopedCredsForEntity(
                Mockito.any(),
                Mockito.any(),
                Mockito.anyBoolean(),
                Mockito.anySet(),
                Mockito.anySet()))
        .thenReturn(properties);
    List<PolarisStorageConfigurationInfo> configValues = storageConfigs();

    AccessConfig config =
        storageCredentialCache.getOrGenerateSubScopeCreds(
            metaStoreManager,
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
}
