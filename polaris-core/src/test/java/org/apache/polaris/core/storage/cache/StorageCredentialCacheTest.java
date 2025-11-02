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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.ScopedCredentialsResult;
import org.apache.polaris.core.persistence.transactional.TransactionalPersistence;
import org.apache.polaris.core.persistence.transactional.TreeMapMetaStore;
import org.apache.polaris.core.persistence.transactional.TreeMapTransactionalPersistenceImpl;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class StorageCredentialCacheTest {
  private final PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
  private final PolarisCallContext callCtx;
  private final StorageCredentialCacheConfig storageCredentialCacheConfig;
  private final PolarisMetaStoreManager metaStoreManager;

  private StorageCredentialCache storageCredentialCache;

  public StorageCredentialCacheTest() {
    // the entity store, use treemap implementation
    TreeMapMetaStore store = new TreeMapMetaStore(diagServices);
    // to interact with the metastore
    TransactionalPersistence metaStore =
        new TreeMapTransactionalPersistenceImpl(
            diagServices, store, Mockito.mock(), RANDOM_SECRETS);
    callCtx = new PolarisCallContext(() -> "testRealm", metaStore);
    storageCredentialCacheConfig = () -> 10_000;
    metaStoreManager = Mockito.mock(PolarisMetaStoreManager.class);
    storageCredentialCache = newStorageCredentialCache();
  }

  private StorageCredentialCache newStorageCredentialCache() {
    return new StorageCredentialCache(diagServices, storageCredentialCacheConfig);
  }

  @Test
  public void testBadResult() {
    storageCredentialCache = newStorageCredentialCache();
    ScopedCredentialsResult badResult =
        new ScopedCredentialsResult(
            BaseResult.ReturnStatus.SUBSCOPE_CREDS_ERROR, "extra_error_info");
    Mockito.when(
            metaStoreManager.getSubscopedCredsForEntity(
                Mockito.any(),
                Mockito.anyLong(),
                Mockito.anyLong(),
                Mockito.any(),
                Mockito.anyBoolean(),
                Mockito.anySet(),
                Mockito.anySet(),
                Mockito.any()))
        .thenReturn(badResult);
    PolarisEntity polarisEntity =
        new PolarisEntity(
            new PolarisBaseEntity(
                1, 2, PolarisEntityType.CATALOG, PolarisEntitySubType.ICEBERG_TABLE, 0, "name"));
    Assertions.assertThatThrownBy(
            () ->
                storageCredentialCache.getOrGenerateSubScopeCreds(
                    metaStoreManager,
                    callCtx,
                    polarisEntity,
                    true,
                    Set.of("s3://bucket1/path"),
                    Set.of("s3://bucket3/path"),
                    Optional.empty()))
        .isInstanceOf(UnprocessableEntityException.class)
        .hasMessage("Failed to get subscoped credentials: extra_error_info");
  }

  @Test
  public void testCacheHit() {
    storageCredentialCache = newStorageCredentialCache();
    List<ScopedCredentialsResult> mockedScopedCreds =
        getFakeScopedCreds(3, /* expireSoon= */ false);
    Mockito.when(
            metaStoreManager.getSubscopedCredsForEntity(
                Mockito.any(),
                Mockito.anyLong(),
                Mockito.anyLong(),
                Mockito.any(),
                Mockito.anyBoolean(),
                Mockito.anySet(),
                Mockito.anySet(),
                Mockito.any()))
        .thenReturn(mockedScopedCreds.get(0))
        .thenReturn(mockedScopedCreds.get(1))
        .thenReturn(mockedScopedCreds.get(1));
    PolarisBaseEntity baseEntity =
        new PolarisBaseEntity(
            1, 2, PolarisEntityType.CATALOG, PolarisEntitySubType.ICEBERG_TABLE, 0, "name");
    PolarisEntity polarisEntity = new PolarisEntity(baseEntity);

    // add an item to the cache
    storageCredentialCache.getOrGenerateSubScopeCreds(
        metaStoreManager,
        callCtx,
        polarisEntity,
        true,
        Set.of("s3://bucket1/path", "s3://bucket2/path"),
        Set.of("s3://bucket3/path", "s3://bucket4/path"),
        Optional.empty());
    Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(1);

    // subscope for the same entity and same allowed locations, will hit the cache
    storageCredentialCache.getOrGenerateSubScopeCreds(
        metaStoreManager,
        callCtx,
        polarisEntity,
        true,
        Set.of("s3://bucket1/path", "s3://bucket2/path"),
        Set.of("s3://bucket3/path", "s3://bucket4/path"),
        Optional.empty());
    Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(1);
  }

  @RepeatedTest(10)
  public void testCacheEvict() throws InterruptedException {
    storageCredentialCache = newStorageCredentialCache();
    List<ScopedCredentialsResult> mockedScopedCreds = getFakeScopedCreds(3, /* expireSoon= */ true);

    Mockito.when(
            metaStoreManager.getSubscopedCredsForEntity(
                Mockito.any(),
                Mockito.anyLong(),
                Mockito.anyLong(),
                Mockito.any(),
                Mockito.anyBoolean(),
                Mockito.anySet(),
                Mockito.anySet(),
                Mockito.any()))
        .thenReturn(mockedScopedCreds.get(0))
        .thenReturn(mockedScopedCreds.get(1))
        .thenReturn(mockedScopedCreds.get(2));
    PolarisBaseEntity baseEntity =
        new PolarisBaseEntity(
            1, 2, PolarisEntityType.CATALOG, PolarisEntitySubType.ICEBERG_TABLE, 0, "name");
    PolarisEntity polarisEntity = new PolarisEntity(baseEntity);
    StorageCredentialCacheKey cacheKey =
        StorageCredentialCacheKey.of(
            callCtx.getRealmContext().getRealmIdentifier(),
            polarisEntity,
            true,
            Set.of("s3://bucket1/path", "s3://bucket2/path"),
            Set.of("s3://bucket/path"),
            Optional.empty());

    // the entry will be evicted immediately because the token is expired
    storageCredentialCache.getOrGenerateSubScopeCreds(
        metaStoreManager,
        callCtx,
        polarisEntity,
        true,
        Set.of("s3://bucket1/path", "s3://bucket2/path"),
        Set.of("s3://bucket/path"),
        Optional.empty());
    Assertions.assertThat(storageCredentialCache.getIfPresent(cacheKey)).isNull();

    storageCredentialCache.getOrGenerateSubScopeCreds(
        metaStoreManager,
        callCtx,
        polarisEntity,
        true,
        Set.of("s3://bucket1/path", "s3://bucket2/path"),
        Set.of("s3://bucket/path"),
        Optional.empty());
    Assertions.assertThat(storageCredentialCache.getIfPresent(cacheKey)).isNull();

    storageCredentialCache.getOrGenerateSubScopeCreds(
        metaStoreManager,
        callCtx,
        polarisEntity,
        true,
        Set.of("s3://bucket1/path", "s3://bucket2/path"),
        Set.of("s3://bucket/path"),
        Optional.empty());
    Assertions.assertThat(storageCredentialCache.getIfPresent(cacheKey)).isNull();
  }

  @Test
  public void testCacheGenerateNewEntries() {
    storageCredentialCache = newStorageCredentialCache();
    List<ScopedCredentialsResult> mockedScopedCreds =
        getFakeScopedCreds(3, /* expireSoon= */ false);
    Mockito.when(
            metaStoreManager.getSubscopedCredsForEntity(
                Mockito.any(),
                Mockito.anyLong(),
                Mockito.anyLong(),
                Mockito.any(),
                Mockito.anyBoolean(),
                Mockito.anySet(),
                Mockito.anySet(),
                Mockito.any()))
        .thenReturn(mockedScopedCreds.get(0))
        .thenReturn(mockedScopedCreds.get(1))
        .thenReturn(mockedScopedCreds.get(2));
    List<PolarisEntity> entityList = getPolarisEntities();
    int cacheSize = 0;
    // different catalog will generate new cache entries
    for (PolarisEntity entity : entityList) {
      storageCredentialCache.getOrGenerateSubScopeCreds(
          metaStoreManager,
          callCtx,
          entity,
          true,
          Set.of("s3://bucket1/path", "s3://bucket2/path"),
          Set.of("s3://bucket/path"),
          Optional.empty());
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(++cacheSize);
    }
    // update the entity's storage config, since StorageConfig changed, cache will generate new
    // entry
    for (PolarisEntity entity : entityList) {
      Map<String, String> internalMap = entity.getPropertiesAsMap();
      internalMap.put(
          PolarisEntityConstants.getStorageConfigInfoPropertyName(), "newStorageConfig");
      PolarisBaseEntity updateEntity =
          new PolarisBaseEntity.Builder(entity).internalPropertiesAsMap(internalMap).build();
      storageCredentialCache.getOrGenerateSubScopeCreds(
          metaStoreManager,
          callCtx,
          PolarisEntity.of(updateEntity),
          /* allowedListAction= */ true,
          Set.of("s3://bucket1/path", "s3://bucket2/path"),
          Set.of("s3://bucket/path"),
          Optional.empty());
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(++cacheSize);
    }
    // allowedListAction changed to different value FALSE, will generate new entry
    for (PolarisEntity entity : entityList) {
      storageCredentialCache.getOrGenerateSubScopeCreds(
          metaStoreManager,
          callCtx,
          entity,
          /* allowedListAction= */ false,
          Set.of("s3://bucket1/path", "s3://bucket2/path"),
          Set.of("s3://bucket/path"),
          Optional.empty());
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(++cacheSize);
    }
    // different allowedWriteLocations, will generate new entry
    for (PolarisEntity entity : entityList) {
      storageCredentialCache.getOrGenerateSubScopeCreds(
          metaStoreManager,
          callCtx,
          entity,
          /* allowedListAction= */ false,
          Set.of("s3://bucket1/path", "s3://bucket2/path"),
          Set.of("s3://differentbucket/path"),
          Optional.empty());
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(++cacheSize);
    }
    // different allowedReadLocations, will generate new try
    for (PolarisEntity entity : entityList) {
      Map<String, String> internalMap = entity.getPropertiesAsMap();
      internalMap.put(
          PolarisEntityConstants.getStorageConfigInfoPropertyName(), "newStorageConfig");
      PolarisBaseEntity updateEntity =
          new PolarisBaseEntity.Builder(entity).internalPropertiesAsMap(internalMap).build();
      storageCredentialCache.getOrGenerateSubScopeCreds(
          metaStoreManager,
          callCtx,
          PolarisEntity.of(updateEntity),
          /* allowedListAction= */ false,
          Set.of("s3://differentbucket/path", "s3://bucket2/path"),
          Set.of("s3://bucket/path"),
          Optional.empty());
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(++cacheSize);
    }
  }

  @Test
  public void testCacheNotAffectedBy() {
    storageCredentialCache = newStorageCredentialCache();
    List<ScopedCredentialsResult> mockedScopedCreds =
        getFakeScopedCreds(3, /* expireSoon= */ false);

    Mockito.when(
            metaStoreManager.getSubscopedCredsForEntity(
                Mockito.any(),
                Mockito.anyLong(),
                Mockito.anyLong(),
                Mockito.any(),
                Mockito.anyBoolean(),
                Mockito.anySet(),
                Mockito.anySet(),
                Mockito.any()))
        .thenReturn(mockedScopedCreds.get(0))
        .thenReturn(mockedScopedCreds.get(1))
        .thenReturn(mockedScopedCreds.get(2));
    List<PolarisEntity> entityList = getPolarisEntities();
    for (PolarisEntity entity : entityList) {
      storageCredentialCache.getOrGenerateSubScopeCreds(
          metaStoreManager,
          callCtx,
          entity,
          true,
          Set.of("s3://bucket1/path", "s3://bucket2/path"),
          Set.of("s3://bucket3/path", "s3://bucket4/path"),
          Optional.empty());
    }
    Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(entityList.size());

    // entity ID does not affect the cache
    for (PolarisEntity entity : entityList) {
      storageCredentialCache.getOrGenerateSubScopeCreds(
          metaStoreManager,
          callCtx,
          new PolarisEntity(new PolarisBaseEntity.Builder(entity).id(1234).build()),
          true,
          Set.of("s3://bucket1/path", "s3://bucket2/path"),
          Set.of("s3://bucket3/path", "s3://bucket4/path"),
          Optional.empty());
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(entityList.size());
    }

    // other property changes does not affect the cache
    for (PolarisEntity entity : entityList) {
      storageCredentialCache.getOrGenerateSubScopeCreds(
          metaStoreManager,
          callCtx,
          new PolarisEntity(new PolarisBaseEntity.Builder(entity).entityVersion(5).build()),
          true,
          Set.of("s3://bucket1/path", "s3://bucket2/path"),
          Set.of("s3://bucket3/path", "s3://bucket4/path"),
          Optional.empty());
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(entityList.size());
    }
    // order of the allowedReadLocations does not affect the cache
    for (PolarisEntity entity : entityList) {
      storageCredentialCache.getOrGenerateSubScopeCreds(
          metaStoreManager,
          callCtx,
          new PolarisEntity(new PolarisBaseEntity.Builder(entity).entityVersion(5).build()),
          true,
          Set.of("s3://bucket2/path", "s3://bucket1/path"),
          Set.of("s3://bucket3/path", "s3://bucket4/path"),
          Optional.empty());
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(entityList.size());
    }

    // order of the allowedWriteLocations does not affect the cache
    for (PolarisEntity entity : entityList) {
      storageCredentialCache.getOrGenerateSubScopeCreds(
          metaStoreManager,
          callCtx,
          new PolarisEntity(new PolarisBaseEntity.Builder(entity).entityVersion(5).build()),
          true,
          Set.of("s3://bucket2/path", "s3://bucket1/path"),
          Set.of("s3://bucket4/path", "s3://bucket3/path"),
          Optional.empty());
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(entityList.size());
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
              StorageAccessConfig.builder()
                  .put(StorageAccessProperty.AWS_KEY_ID, "key_id_" + finalI)
                  .put(StorageAccessProperty.AWS_SECRET_KEY, "key_secret_" + finalI)
                  .put(StorageAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS, expireTime)
                  .put(StorageAccessProperty.EXPIRATION_TIME, expireTime)
                  .build()));
      if (res.size() == number) return res;
      res.add(
          new ScopedCredentialsResult(
              StorageAccessConfig.builder()
                  .put(StorageAccessProperty.AZURE_SAS_TOKEN, "sas_token_" + finalI)
                  .put(StorageAccessProperty.EXPIRATION_TIME, expireTime)
                  .build()));
      if (res.size() == number) return res;
      res.add(
          new ScopedCredentialsResult(
              StorageAccessConfig.builder()
                  .put(StorageAccessProperty.GCS_ACCESS_TOKEN, "gcs_token_" + finalI)
                  .put(StorageAccessProperty.GCS_ACCESS_TOKEN_EXPIRES_AT, expireTime)
                  .build()));
    }
    return res;
  }

  @Nonnull
  private static List<PolarisEntity> getPolarisEntities() {
    PolarisEntity polarisEntity1 =
        new PolarisEntity(
            new PolarisBaseEntity(
                1, 2, PolarisEntityType.CATALOG, PolarisEntitySubType.ICEBERG_TABLE, 0, "name"));
    PolarisEntity polarisEntity2 =
        new PolarisEntity(
            new PolarisBaseEntity(
                2, 2, PolarisEntityType.CATALOG, PolarisEntitySubType.ICEBERG_TABLE, 0, "name"));
    PolarisEntity polarisEntity3 =
        new PolarisEntity(
            new PolarisBaseEntity(
                3, 2, PolarisEntityType.CATALOG, PolarisEntitySubType.ICEBERG_TABLE, 0, "name"));

    return Arrays.asList(polarisEntity1, polarisEntity2, polarisEntity3);
  }

  @Test
  public void testExtraProperties() {
    storageCredentialCache = newStorageCredentialCache();
    ScopedCredentialsResult properties =
        new ScopedCredentialsResult(
            StorageAccessConfig.builder()
                .put(StorageAccessProperty.AWS_SECRET_KEY, "super-secret-123")
                .put(StorageAccessProperty.AWS_ENDPOINT, "test-endpoint1")
                .put(StorageAccessProperty.AWS_PATH_STYLE_ACCESS, "true")
                .build());
    Mockito.when(
            metaStoreManager.getSubscopedCredsForEntity(
                Mockito.any(),
                Mockito.anyLong(),
                Mockito.anyLong(),
                Mockito.any(),
                Mockito.anyBoolean(),
                Mockito.anySet(),
                Mockito.anySet(),
                Mockito.any()))
        .thenReturn(properties);
    List<PolarisEntity> entityList = getPolarisEntities();

    StorageAccessConfig config =
        storageCredentialCache.getOrGenerateSubScopeCreds(
            metaStoreManager,
            callCtx,
            entityList.get(0),
            true,
            Set.of("s3://bucket1/path", "s3://bucket2/path"),
            Set.of("s3://bucket3/path", "s3://bucket4/path"),
            Optional.empty());
    Assertions.assertThat(config.credentials())
        .containsExactly(Map.entry("s3.secret-access-key", "super-secret-123"));
    Assertions.assertThat(config.extraProperties())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of("s3.endpoint", "test-endpoint1", "s3.path-style-access", "true"));
  }
}
