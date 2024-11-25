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

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.BaseResult;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManagerImpl;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;
import org.apache.polaris.core.persistence.PolarisObjectMapperUtil;
import org.apache.polaris.core.persistence.PolarisTreeMapMetaStoreSessionImpl;
import org.apache.polaris.core.persistence.PolarisTreeMapStore;
import org.apache.polaris.core.storage.PolarisCredentialProperty;
import org.apache.polaris.core.storage.PolarisCredentialVendor.ScopedCredentialsResult;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class StorageCredentialCacheTest {

  // polaris call context
  private final PolarisCallContext callCtx;

  // the meta store manager
  private final PolarisMetaStoreManager metaStoreManager;

  StorageCredentialCache storageCredentialCache;

  public StorageCredentialCacheTest() {
    // diag services
    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    // the entity store, use treemap implementation
    PolarisTreeMapStore store = new PolarisTreeMapStore(diagServices);
    // to interact with the metastore
    PolarisMetaStoreSession metaStore =
        new PolarisTreeMapMetaStoreSessionImpl(store, Mockito.mock(), RANDOM_SECRETS);
    callCtx = new PolarisCallContext(metaStore, diagServices);
    metaStoreManager = Mockito.mock(PolarisMetaStoreManagerImpl.class);
    storageCredentialCache = new StorageCredentialCache();
  }

  @Test
  public void testBadResult() {
    storageCredentialCache = new StorageCredentialCache();
    ScopedCredentialsResult badResult =
        new ScopedCredentialsResult(
            BaseResult.ReturnStatus.SUBSCOPE_CREDS_ERROR, "extra_error_info");
    Mockito.when(
            metaStoreManager.getSubscopedCredsForEntity(
                Mockito.any(),
                Mockito.anyLong(),
                Mockito.anyLong(),
                Mockito.anyBoolean(),
                Mockito.anySet(),
                Mockito.anySet()))
        .thenReturn(badResult);
    PolarisEntity polarisEntity =
        new PolarisEntity(
            new PolarisBaseEntity(
                1, 2, PolarisEntityType.CATALOG, PolarisEntitySubType.TABLE, 0, "name"));
    Assertions.assertThatThrownBy(
            () ->
                storageCredentialCache.getOrGenerateSubScopeCreds(
                    metaStoreManager,
                    callCtx,
                    polarisEntity,
                    true,
                    new HashSet<>(Arrays.asList("s3://bucket1/path")),
                    new HashSet<>(Arrays.asList("s3://bucket3/path"))))
        .isInstanceOf(UnprocessableEntityException.class)
        .hasMessage("Failed to get subscoped credentials: extra_error_info");
  }

  @Test
  public void testCacheHit() {
    storageCredentialCache = new StorageCredentialCache();
    List<ScopedCredentialsResult> mockedScopedCreds =
        getFakeScopedCreds(3, /* expireSoon= */ false);
    Mockito.when(
            metaStoreManager.getSubscopedCredsForEntity(
                Mockito.any(),
                Mockito.anyLong(),
                Mockito.anyLong(),
                Mockito.anyBoolean(),
                Mockito.anySet(),
                Mockito.anySet()))
        .thenReturn(mockedScopedCreds.get(0))
        .thenReturn(mockedScopedCreds.get(1))
        .thenReturn(mockedScopedCreds.get(1));
    PolarisBaseEntity baseEntity =
        new PolarisBaseEntity(
            1, 2, PolarisEntityType.CATALOG, PolarisEntitySubType.TABLE, 0, "name");
    PolarisEntity polarisEntity = new PolarisEntity(baseEntity);

    // add an item to the cache
    storageCredentialCache.getOrGenerateSubScopeCreds(
        metaStoreManager,
        callCtx,
        polarisEntity,
        true,
        new HashSet<>(Arrays.asList("s3://bucket1/path", "s3://bucket2/path")),
        new HashSet<>(Arrays.asList("s3://bucket3/path", "s3://bucket4/path")));
    Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(1);

    // subscope for the same entity and same allowed locations, will hit the cache
    storageCredentialCache.getOrGenerateSubScopeCreds(
        metaStoreManager,
        callCtx,
        polarisEntity,
        true,
        new HashSet<>(Arrays.asList("s3://bucket1/path", "s3://bucket2/path")),
        new HashSet<>(Arrays.asList("s3://bucket3/path", "s3://bucket4/path")));
    Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(1);
  }

  @RepeatedTest(10)
  public void testCacheEvict() throws InterruptedException {
    storageCredentialCache = new StorageCredentialCache();
    List<ScopedCredentialsResult> mockedScopedCreds = getFakeScopedCreds(3, /* expireSoon= */ true);

    Mockito.when(
            metaStoreManager.getSubscopedCredsForEntity(
                Mockito.any(),
                Mockito.anyLong(),
                Mockito.anyLong(),
                Mockito.anyBoolean(),
                Mockito.anySet(),
                Mockito.anySet()))
        .thenReturn(mockedScopedCreds.get(0))
        .thenReturn(mockedScopedCreds.get(1))
        .thenReturn(mockedScopedCreds.get(2));
    PolarisBaseEntity baseEntity =
        new PolarisBaseEntity(
            1, 2, PolarisEntityType.CATALOG, PolarisEntitySubType.TABLE, 0, "name");
    PolarisEntity polarisEntity = new PolarisEntity(baseEntity);
    StorageCredentialCacheKey cacheKey =
        new StorageCredentialCacheKey(
            polarisEntity,
            true,
            new HashSet<>(Arrays.asList("s3://bucket1/path", "s3://bucket2/path")),
            new HashSet<>(Arrays.asList("s3://bucket/path")),
            callCtx);

    // the entry will be evicted immediately because the token is expired
    storageCredentialCache.getOrGenerateSubScopeCreds(
        metaStoreManager,
        callCtx,
        polarisEntity,
        true,
        new HashSet<>(Arrays.asList("s3://bucket1/path", "s3://bucket2/path")),
        new HashSet<>(Arrays.asList("s3://bucket/path")));
    Assertions.assertThat(storageCredentialCache.getIfPresent(cacheKey)).isNull();

    storageCredentialCache.getOrGenerateSubScopeCreds(
        metaStoreManager,
        callCtx,
        polarisEntity,
        true,
        new HashSet<>(Arrays.asList("s3://bucket1/path", "s3://bucket2/path")),
        new HashSet<>(Arrays.asList("s3://bucket/path")));
    Assertions.assertThat(storageCredentialCache.getIfPresent(cacheKey)).isNull();

    storageCredentialCache.getOrGenerateSubScopeCreds(
        metaStoreManager,
        callCtx,
        polarisEntity,
        true,
        new HashSet<>(Arrays.asList("s3://bucket1/path", "s3://bucket2/path")),
        new HashSet<>(Arrays.asList("s3://bucket/path")));
    Assertions.assertThat(storageCredentialCache.getIfPresent(cacheKey)).isNull();
  }

  @Test
  public void testCacheGenerateNewEntries() {
    storageCredentialCache = new StorageCredentialCache();
    List<ScopedCredentialsResult> mockedScopedCreds =
        getFakeScopedCreds(3, /* expireSoon= */ false);
    Mockito.when(
            metaStoreManager.getSubscopedCredsForEntity(
                Mockito.any(),
                Mockito.anyLong(),
                Mockito.anyLong(),
                Mockito.anyBoolean(),
                Mockito.anySet(),
                Mockito.anySet()))
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
          new HashSet<>(Arrays.asList("s3://bucket1/path", "s3://bucket2/path")),
          new HashSet<>(Arrays.asList("s3://bucket/path")));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(++cacheSize);
    }
    // update the entity's storage config, since StorageConfig changed, cache will generate new
    // entry
    for (PolarisEntity entity : entityList) {
      Map<String, String> internalMap = entity.getPropertiesAsMap();
      internalMap.put(
          PolarisEntityConstants.getStorageConfigInfoPropertyName(), "newStorageConfig");
      entity.setInternalProperties(
          PolarisObjectMapperUtil.serializeProperties(callCtx, internalMap));
      storageCredentialCache.getOrGenerateSubScopeCreds(
          metaStoreManager,
          callCtx,
          entity,
          /* allowedListAction= */ true,
          new HashSet<>(Arrays.asList("s3://bucket1/path", "s3://bucket2/path")),
          new HashSet<>(Arrays.asList("s3://bucket/path")));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(++cacheSize);
    }
    // allowedListAction changed to different value FALSE, will generate new entry
    for (PolarisEntity entity : entityList) {
      storageCredentialCache.getOrGenerateSubScopeCreds(
          metaStoreManager,
          callCtx,
          entity,
          /* allowedListAction= */ false,
          new HashSet<>(Arrays.asList("s3://bucket1/path", "s3://bucket2/path")),
          new HashSet<>(Arrays.asList("s3://bucket/path")));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(++cacheSize);
    }
    // different allowedWriteLocations, will generate new entry
    for (PolarisEntity entity : entityList) {
      storageCredentialCache.getOrGenerateSubScopeCreds(
          metaStoreManager,
          callCtx,
          entity,
          /* allowedListAction= */ false,
          new HashSet<>(Arrays.asList("s3://bucket1/path", "s3://bucket2/path")),
          new HashSet<>(Arrays.asList("s3://differentbucket/path")));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(++cacheSize);
    }
    // different allowedReadLocations, will generate new try
    for (PolarisEntity entity : entityList) {
      Map<String, String> internalMap = entity.getPropertiesAsMap();
      internalMap.put(
          PolarisEntityConstants.getStorageConfigInfoPropertyName(), "newStorageConfig");
      entity.setInternalProperties(
          PolarisObjectMapperUtil.serializeProperties(callCtx, internalMap));
      storageCredentialCache.getOrGenerateSubScopeCreds(
          metaStoreManager,
          callCtx,
          entity,
          /* allowedListAction= */ false,
          new HashSet<>(Arrays.asList("s3://differentbucket/path", "s3://bucket2/path")),
          new HashSet<>(Arrays.asList("s3://bucket/path")));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(++cacheSize);
    }
  }

  @Test
  public void testCacheNotAffectedBy() {
    storageCredentialCache = new StorageCredentialCache();
    List<ScopedCredentialsResult> mockedScopedCreds =
        getFakeScopedCreds(3, /* expireSoon= */ false);

    Mockito.when(
            metaStoreManager.getSubscopedCredsForEntity(
                Mockito.any(),
                Mockito.anyLong(),
                Mockito.anyLong(),
                Mockito.anyBoolean(),
                Mockito.anySet(),
                Mockito.anySet()))
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
          new HashSet<>(Arrays.asList("s3://bucket1/path", "s3://bucket2/path")),
          new HashSet<>(Arrays.asList("s3://bucket3/path", "s3://bucket4/path")));
    }
    Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(entityList.size());

    // entity ID does not affect the cache
    for (PolarisEntity entity : entityList) {
      entity.setId(1234);
      storageCredentialCache.getOrGenerateSubScopeCreds(
          metaStoreManager,
          callCtx,
          entity,
          true,
          new HashSet<>(Arrays.asList("s3://bucket1/path", "s3://bucket2/path")),
          new HashSet<>(Arrays.asList("s3://bucket3/path", "s3://bucket4/path")));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(entityList.size());
    }

    // other property changes does not affect the cache
    for (PolarisEntity entity : entityList) {
      entity.setEntityVersion(5);
      storageCredentialCache.getOrGenerateSubScopeCreds(
          metaStoreManager,
          callCtx,
          entity,
          true,
          new HashSet<>(Arrays.asList("s3://bucket1/path", "s3://bucket2/path")),
          new HashSet<>(Arrays.asList("s3://bucket3/path", "s3://bucket4/path")));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(entityList.size());
    }
    // order of the allowedReadLocations does not affect the cache
    for (PolarisEntity entity : entityList) {
      entity.setEntityVersion(5);
      storageCredentialCache.getOrGenerateSubScopeCreds(
          metaStoreManager,
          callCtx,
          entity,
          true,
          new HashSet<>(Arrays.asList("s3://bucket2/path", "s3://bucket1/path")),
          new HashSet<>(Arrays.asList("s3://bucket3/path", "s3://bucket4/path")));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(entityList.size());
    }

    // order of the allowedWriteLocations does not affect the cache
    for (PolarisEntity entity : entityList) {
      entity.setEntityVersion(5);
      storageCredentialCache.getOrGenerateSubScopeCreds(
          metaStoreManager,
          callCtx,
          entity,
          true,
          new HashSet<>(Arrays.asList("s3://bucket2/path", "s3://bucket1/path")),
          new HashSet<>(Arrays.asList("s3://bucket4/path", "s3://bucket3/path")));
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
              new EnumMap<>(
                  ImmutableMap.<PolarisCredentialProperty, String>builder()
                      .put(PolarisCredentialProperty.AWS_KEY_ID, "key_id_" + finalI)
                      .put(PolarisCredentialProperty.AWS_SECRET_KEY, "key_secret_" + finalI)
                      .put(PolarisCredentialProperty.EXPIRATION_TIME, expireTime)
                      .buildOrThrow())));
      if (res.size() == number) return res;
      res.add(
          new ScopedCredentialsResult(
              new EnumMap<>(
                  ImmutableMap.<PolarisCredentialProperty, String>builder()
                      .put(PolarisCredentialProperty.AZURE_SAS_TOKEN, "sas_token_" + finalI)
                      .put(PolarisCredentialProperty.AZURE_ACCOUNT_HOST, "account_host")
                      .put(PolarisCredentialProperty.EXPIRATION_TIME, expireTime)
                      .buildOrThrow())));
      if (res.size() == number) return res;
      res.add(
          new ScopedCredentialsResult(
              new EnumMap<>(
                  ImmutableMap.<PolarisCredentialProperty, String>builder()
                      .put(PolarisCredentialProperty.GCS_ACCESS_TOKEN, "gcs_token_" + finalI)
                      .put(PolarisCredentialProperty.GCS_ACCESS_TOKEN_EXPIRES_AT, expireTime)
                      .buildOrThrow())));
    }
    return res;
  }

  @NotNull
  private static List<PolarisEntity> getPolarisEntities() {
    PolarisEntity polarisEntity1 =
        new PolarisEntity(
            new PolarisBaseEntity(
                1, 2, PolarisEntityType.CATALOG, PolarisEntitySubType.TABLE, 0, "name"));
    PolarisEntity polarisEntity2 =
        new PolarisEntity(
            new PolarisBaseEntity(
                2, 2, PolarisEntityType.CATALOG, PolarisEntitySubType.TABLE, 0, "name"));
    PolarisEntity polarisEntity3 =
        new PolarisEntity(
            new PolarisBaseEntity(
                3, 2, PolarisEntityType.CATALOG, PolarisEntitySubType.TABLE, 0, "name"));

    List<PolarisEntity> entityList = Arrays.asList(polarisEntity1, polarisEntity2, polarisEntity3);
    return entityList;
  }
}
