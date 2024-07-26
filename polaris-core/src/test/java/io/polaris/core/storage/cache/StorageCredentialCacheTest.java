package io.polaris.core.storage.cache;

import io.polaris.core.PolarisCallContext;
import io.polaris.core.PolarisDefaultDiagServiceImpl;
import io.polaris.core.PolarisDiagnostics;
import io.polaris.core.entity.PolarisBaseEntity;
import io.polaris.core.entity.PolarisEntity;
import io.polaris.core.entity.PolarisEntityConstants;
import io.polaris.core.entity.PolarisEntitySubType;
import io.polaris.core.entity.PolarisEntityType;
import io.polaris.core.persistence.PolarisMetaStoreManager;
import io.polaris.core.persistence.PolarisMetaStoreManagerImpl;
import io.polaris.core.persistence.PolarisMetaStoreSession;
import io.polaris.core.persistence.PolarisObjectMapperUtil;
import io.polaris.core.persistence.PolarisTreeMapMetaStoreSessionImpl;
import io.polaris.core.persistence.PolarisTreeMapStore;
import io.polaris.core.storage.PolarisCredentialProperty;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
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
        new PolarisTreeMapMetaStoreSessionImpl(store, Mockito.mock());
    callCtx = new PolarisCallContext(metaStore, diagServices);
    metaStoreManager = Mockito.mock(PolarisMetaStoreManagerImpl.class);
    storageCredentialCache = new StorageCredentialCache();
  }

  @Test
  public void testBadResult() {
    storageCredentialCache = new StorageCredentialCache();
    PolarisMetaStoreManager.ScopedCredentialsResult badResult =
        new PolarisMetaStoreManager.ScopedCredentialsResult(
            PolarisMetaStoreManager.ReturnStatus.SUBSCOPE_CREDS_ERROR, "extra_error_info");
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
    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            storageCredentialCache.getOrGenerateSubScopeCreds(
                metaStoreManager,
                callCtx,
                polarisEntity,
                true,
                new HashSet<>(Arrays.asList("s3://bucket1/path")),
                new HashSet<>(Arrays.asList("s3://bucket3/path"))));
  }

  @Test
  public void testCacheHit() {
    storageCredentialCache = new StorageCredentialCache();
    List<PolarisMetaStoreManager.ScopedCredentialsResult> mockedScopedCreds =
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
    Assertions.assertEquals(1, storageCredentialCache.getEstimatedSize());

    // subscope for the same entity and same allowed locations, will hit the cache
    storageCredentialCache.getOrGenerateSubScopeCreds(
        metaStoreManager,
        callCtx,
        polarisEntity,
        true,
        new HashSet<>(Arrays.asList("s3://bucket1/path", "s3://bucket2/path")),
        new HashSet<>(Arrays.asList("s3://bucket3/path", "s3://bucket4/path")));
    Assertions.assertEquals(1, storageCredentialCache.getEstimatedSize());
  }

  @RepeatedTest(10)
  public void testCacheEvict() throws InterruptedException {
    storageCredentialCache = new StorageCredentialCache();
    List<PolarisMetaStoreManager.ScopedCredentialsResult> mockedScopedCreds =
        getFakeScopedCreds(3, /* expireSoon= */ true);

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
    Assertions.assertNull(storageCredentialCache.getIfPresent(cacheKey));

    storageCredentialCache.getOrGenerateSubScopeCreds(
        metaStoreManager,
        callCtx,
        polarisEntity,
        true,
        new HashSet<>(Arrays.asList("s3://bucket1/path", "s3://bucket2/path")),
        new HashSet<>(Arrays.asList("s3://bucket/path")));
    Assertions.assertNull(storageCredentialCache.getIfPresent(cacheKey));

    storageCredentialCache.getOrGenerateSubScopeCreds(
        metaStoreManager,
        callCtx,
        polarisEntity,
        true,
        new HashSet<>(Arrays.asList("s3://bucket1/path", "s3://bucket2/path")),
        new HashSet<>(Arrays.asList("s3://bucket/path")));
    Assertions.assertNull(storageCredentialCache.getIfPresent(cacheKey));
  }

  @Test
  public void testCacheGenerateNewEntries() {
    storageCredentialCache = new StorageCredentialCache();
    List<PolarisMetaStoreManager.ScopedCredentialsResult> mockedScopedCreds =
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
      Map<String, String> res =
          storageCredentialCache.getOrGenerateSubScopeCreds(
              metaStoreManager,
              callCtx,
              entity,
              true,
              new HashSet<>(Arrays.asList("s3://bucket1/path", "s3://bucket2/path")),
              new HashSet<>(Arrays.asList("s3://bucket/path")));
      Assertions.assertEquals(++cacheSize, storageCredentialCache.getEstimatedSize());
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
      Assertions.assertEquals(++cacheSize, storageCredentialCache.getEstimatedSize());
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
      Assertions.assertEquals(++cacheSize, storageCredentialCache.getEstimatedSize());
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
      Assertions.assertEquals(++cacheSize, storageCredentialCache.getEstimatedSize());
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
      Assertions.assertEquals(++cacheSize, storageCredentialCache.getEstimatedSize());
    }
  }

  @Test
  public void testCacheNotAffectedBy() {
    storageCredentialCache = new StorageCredentialCache();
    List<PolarisMetaStoreManager.ScopedCredentialsResult> mockedScopedCreds =
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
    Assertions.assertEquals(entityList.size(), storageCredentialCache.getEstimatedSize());

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
      Assertions.assertEquals(entityList.size(), storageCredentialCache.getEstimatedSize());
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
      Assertions.assertEquals(entityList.size(), storageCredentialCache.getEstimatedSize());
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
      Assertions.assertEquals(entityList.size(), storageCredentialCache.getEstimatedSize());
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
      Assertions.assertEquals(entityList.size(), storageCredentialCache.getEstimatedSize());
    }
  }

  private static List<PolarisMetaStoreManager.ScopedCredentialsResult> getFakeScopedCreds(
      int number, boolean expireSoon) {
    List<PolarisMetaStoreManager.ScopedCredentialsResult> res = new ArrayList<>();
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
          new PolarisMetaStoreManager.ScopedCredentialsResult(
              new EnumMap<>(PolarisCredentialProperty.class) {
                {
                  put(PolarisCredentialProperty.AWS_KEY_ID, "key_id_" + finalI);
                  put(PolarisCredentialProperty.AWS_SECRET_KEY, "key_secret_" + finalI);
                  put(PolarisCredentialProperty.EXPIRATION_TIME, expireTime);
                }
              }));
      if (res.size() == number) return res;
      res.add(
          new PolarisMetaStoreManager.ScopedCredentialsResult(
              new EnumMap<>(PolarisCredentialProperty.class) {
                {
                  put(PolarisCredentialProperty.AZURE_SAS_TOKEN, "sas_token_" + finalI);
                  put(PolarisCredentialProperty.AZURE_ACCOUNT_HOST, "account_host");
                  put(PolarisCredentialProperty.EXPIRATION_TIME, expireTime);
                }
              }));
      if (res.size() == number) return res;
      res.add(
          new PolarisMetaStoreManager.ScopedCredentialsResult(
              new EnumMap<>(PolarisCredentialProperty.class) {
                {
                  put(PolarisCredentialProperty.GCS_ACCESS_TOKEN, "gcs_token_" + finalI);
                  put(PolarisCredentialProperty.GCS_ACCESS_TOKEN_EXPIRES_AT, expireTime);
                }
              }));
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
