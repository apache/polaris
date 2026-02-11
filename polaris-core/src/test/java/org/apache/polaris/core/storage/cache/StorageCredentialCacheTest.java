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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

public class StorageCredentialCacheTest {
  private final RealmContext realmContext = () -> "testRealm";
  private final RealmConfig realmConfig =
      new RealmConfigImpl(new PolarisConfigurationStore() {}, realmContext);
  private StorageCredentialCache storageCredentialCache;

  @BeforeEach
  void beforeEach() {
    StorageCredentialCacheConfig storageCredentialCacheConfig = () -> 10_000;
    storageCredentialCache = new StorageCredentialCache(storageCredentialCacheConfig);
  }

  @Test
  public void testBadResult() {
    StorageCredentialCacheKey key =
        buildKey(
            getPolarisEntities().get(0),
            true,
            Set.of("s3://bucket1/path"),
            Set.of("s3://bucket3/path"),
            Optional.empty(),
            Optional.empty());

    Assertions.assertThatThrownBy(
            () ->
                storageCredentialCache.getOrLoad(
                    key,
                    realmConfig,
                    () -> {
                      throw new UnprocessableEntityException(
                          "Failed to get subscoped credentials: extra_error_info");
                    }))
        .isInstanceOf(UnprocessableEntityException.class)
        .hasMessage("Failed to get subscoped credentials: extra_error_info");
  }

  @Test
  public void testCacheHit() {
    List<StorageAccessConfig> configs = getFakeAccessConfigs(3, /* expireSoon= */ false);
    AtomicInteger loadCount = new AtomicInteger();

    PolarisEntity polarisEntity =
        new PolarisEntity(
            new PolarisBaseEntity(
                1, 2, PolarisEntityType.CATALOG, PolarisEntitySubType.ICEBERG_TABLE, 0, "name"));

    StorageCredentialCacheKey key =
        buildKey(
            polarisEntity,
            true,
            Set.of("s3://bucket1/path", "s3://bucket2/path"),
            Set.of("s3://bucket3/path", "s3://bucket4/path"),
            Optional.empty(),
            Optional.empty());

    // add an item to the cache
    storageCredentialCache.getOrLoad(
        key, realmConfig, () -> configs.get(loadCount.getAndIncrement()));
    Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(1);

    // same key will hit the cache
    storageCredentialCache.getOrLoad(
        key, realmConfig, () -> configs.get(loadCount.getAndIncrement()));
    Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(1);

    // same key again — still a hit
    storageCredentialCache.getOrLoad(
        key, realmConfig, () -> configs.get(loadCount.getAndIncrement()));
    Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(1);

    // loader should only have been called once
    Assertions.assertThat(loadCount.get()).isEqualTo(1);
  }

  @Test
  public void testCacheHitForSameKeyWithoutPrincipal() {
    List<StorageAccessConfig> configs = getFakeAccessConfigs(2, /* expireSoon= */ false);
    AtomicInteger loadCount = new AtomicInteger();

    PolarisEntity polarisEntity =
        new PolarisEntity(
            new PolarisBaseEntity(
                1, 2, PolarisEntityType.CATALOG, PolarisEntitySubType.ICEBERG_TABLE, 0, "name"));

    // Key without principal — same key for different principals
    StorageCredentialCacheKey key =
        buildKey(
            polarisEntity,
            true,
            Set.of("s3://bucket1/path", "s3://bucket2/path"),
            Set.of("s3://bucket3/path", "s3://bucket4/path"),
            Optional.empty(),
            Optional.empty());

    storageCredentialCache.getOrLoad(
        key, realmConfig, () -> configs.get(loadCount.getAndIncrement()));
    Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(1);

    // Same key — cache hit regardless of who the caller is
    storageCredentialCache.getOrLoad(
        key, realmConfig, () -> configs.get(loadCount.getAndIncrement()));
    Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(1);

    Assertions.assertThat(loadCount.get()).isEqualTo(1);
  }

  @Test
  public void testCacheMissForDifferentKeysWithPrincipal() {
    List<StorageAccessConfig> configs = getFakeAccessConfigs(2, /* expireSoon= */ false);
    AtomicInteger loadCount = new AtomicInteger();

    PolarisEntity polarisEntity =
        new PolarisEntity(
            new PolarisBaseEntity(
                1, 2, PolarisEntityType.CATALOG, PolarisEntitySubType.ICEBERG_TABLE, 0, "name"));

    // Key with principal1
    StorageCredentialCacheKey key1 =
        buildKey(
            polarisEntity,
            true,
            Set.of("s3://bucket1/path", "s3://bucket2/path"),
            Set.of("s3://bucket3/path", "s3://bucket4/path"),
            Optional.empty(),
            Optional.of("principal"));

    // Key with principal2
    StorageCredentialCacheKey key2 =
        buildKey(
            polarisEntity,
            true,
            Set.of("s3://bucket1/path", "s3://bucket2/path"),
            Set.of("s3://bucket3/path", "s3://bucket4/path"),
            Optional.empty(),
            Optional.of("anotherPrincipal"));

    storageCredentialCache.getOrLoad(
        key1, realmConfig, () -> configs.get(loadCount.getAndIncrement()));
    Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(1);

    // Different key — cache miss
    storageCredentialCache.getOrLoad(
        key2, realmConfig, () -> configs.get(loadCount.getAndIncrement()));
    Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(2);

    Assertions.assertThat(loadCount.get()).isEqualTo(2);
  }

  @RepeatedTest(10)
  public void testCacheEvict() throws Exception {
    List<StorageAccessConfig> configs = getFakeAccessConfigs(3, /* expireSoon= */ true);
    AtomicInteger loadCount = new AtomicInteger();

    PolarisEntity polarisEntity =
        new PolarisEntity(
            new PolarisBaseEntity(
                1, 2, PolarisEntityType.CATALOG, PolarisEntitySubType.ICEBERG_TABLE, 0, "name"));

    StorageCredentialCacheKey cacheKey =
        buildKey(
            polarisEntity,
            true,
            Set.of("s3://bucket1/path", "s3://bucket2/path"),
            Set.of("s3://bucket/path"),
            Optional.empty(),
            Optional.empty());

    // the entry will be evicted immediately because the token is expired
    storageCredentialCache.getOrLoad(
        cacheKey, realmConfig, () -> configs.get(loadCount.getAndIncrement()));
    Assertions.assertThat(storageCredentialCache.getIfPresent(cacheKey)).isNull();

    storageCredentialCache.getOrLoad(
        cacheKey, realmConfig, () -> configs.get(loadCount.getAndIncrement()));
    Assertions.assertThat(storageCredentialCache.getIfPresent(cacheKey)).isNull();

    storageCredentialCache.getOrLoad(
        cacheKey, realmConfig, () -> configs.get(loadCount.getAndIncrement()));
    Assertions.assertThat(storageCredentialCache.getIfPresent(cacheKey)).isNull();
  }

  @Test
  public void testCacheGenerateNewEntries() {
    List<StorageAccessConfig> configs = getFakeAccessConfigs(30, /* expireSoon= */ false);
    AtomicInteger loadCount = new AtomicInteger();

    List<PolarisEntity> entityList = getPolarisEntities();
    int cacheSize = 0;

    // different catalogs will generate new cache entries
    for (PolarisEntity entity : entityList) {
      StorageCredentialCacheKey key =
          buildKey(
              entity,
              true,
              Set.of("s3://bucket1/path", "s3://bucket2/path"),
              Set.of("s3://bucket/path"),
              Optional.empty(),
              Optional.empty());
      storageCredentialCache.getOrLoad(
          key, realmConfig, () -> configs.get(loadCount.getAndIncrement()));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(++cacheSize);
    }

    // update the entity's storage config — since StorageConfig changed, cache will generate new
    // entry
    for (PolarisEntity entity : entityList) {
      Map<String, String> internalMap = entity.getPropertiesAsMap();
      internalMap.put(
          PolarisEntityConstants.getStorageConfigInfoPropertyName(), "newStorageConfig");
      PolarisBaseEntity updateEntity =
          new PolarisBaseEntity.Builder(entity).internalPropertiesAsMap(internalMap).build();
      StorageCredentialCacheKey key =
          buildKey(
              PolarisEntity.of(updateEntity),
              true,
              Set.of("s3://bucket1/path", "s3://bucket2/path"),
              Set.of("s3://bucket/path"),
              Optional.empty(),
              Optional.empty());
      storageCredentialCache.getOrLoad(
          key, realmConfig, () -> configs.get(loadCount.getAndIncrement()));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(++cacheSize);
    }

    // allowedListAction changed to FALSE — will generate new entry
    for (PolarisEntity entity : entityList) {
      StorageCredentialCacheKey key =
          buildKey(
              entity,
              false,
              Set.of("s3://bucket1/path", "s3://bucket2/path"),
              Set.of("s3://bucket/path"),
              Optional.empty(),
              Optional.empty());
      storageCredentialCache.getOrLoad(
          key, realmConfig, () -> configs.get(loadCount.getAndIncrement()));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(++cacheSize);
    }

    // different allowedWriteLocations — will generate new entry
    for (PolarisEntity entity : entityList) {
      StorageCredentialCacheKey key =
          buildKey(
              entity,
              false,
              Set.of("s3://bucket1/path", "s3://bucket2/path"),
              Set.of("s3://differentbucket/path"),
              Optional.empty(),
              Optional.empty());
      storageCredentialCache.getOrLoad(
          key, realmConfig, () -> configs.get(loadCount.getAndIncrement()));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(++cacheSize);
    }

    // different allowedReadLocations — will generate new entry
    for (PolarisEntity entity : entityList) {
      Map<String, String> internalMap = entity.getPropertiesAsMap();
      internalMap.put(
          PolarisEntityConstants.getStorageConfigInfoPropertyName(), "newStorageConfig");
      PolarisBaseEntity updateEntity =
          new PolarisBaseEntity.Builder(entity).internalPropertiesAsMap(internalMap).build();
      StorageCredentialCacheKey key =
          buildKey(
              PolarisEntity.of(updateEntity),
              false,
              Set.of("s3://differentbucket/path", "s3://bucket2/path"),
              Set.of("s3://bucket/path"),
              Optional.empty(),
              Optional.empty());
      storageCredentialCache.getOrLoad(
          key, realmConfig, () -> configs.get(loadCount.getAndIncrement()));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(++cacheSize);
    }
  }

  @Test
  public void testCacheNotAffectedBy() {
    List<StorageAccessConfig> configs = getFakeAccessConfigs(20, /* expireSoon= */ false);
    AtomicInteger loadCount = new AtomicInteger();

    List<PolarisEntity> entityList = getPolarisEntities();
    for (PolarisEntity entity : entityList) {
      StorageCredentialCacheKey key =
          buildKey(
              entity,
              true,
              Set.of("s3://bucket1/path", "s3://bucket2/path"),
              Set.of("s3://bucket3/path", "s3://bucket4/path"),
              Optional.empty(),
              Optional.empty());
      storageCredentialCache.getOrLoad(
          key, realmConfig, () -> configs.get(loadCount.getAndIncrement()));
    }
    Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(entityList.size());

    // entity ID does not affect the cache
    for (PolarisEntity entity : entityList) {
      StorageCredentialCacheKey key =
          buildKey(
              new PolarisEntity(new PolarisBaseEntity.Builder(entity).id(1234).build()),
              true,
              Set.of("s3://bucket1/path", "s3://bucket2/path"),
              Set.of("s3://bucket3/path", "s3://bucket4/path"),
              Optional.empty(),
              Optional.empty());
      storageCredentialCache.getOrLoad(
          key, realmConfig, () -> configs.get(loadCount.getAndIncrement()));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(entityList.size());
    }

    // other property changes do not affect the cache
    for (PolarisEntity entity : entityList) {
      StorageCredentialCacheKey key =
          buildKey(
              new PolarisEntity(new PolarisBaseEntity.Builder(entity).entityVersion(5).build()),
              true,
              Set.of("s3://bucket1/path", "s3://bucket2/path"),
              Set.of("s3://bucket3/path", "s3://bucket4/path"),
              Optional.empty(),
              Optional.empty());
      storageCredentialCache.getOrLoad(
          key, realmConfig, () -> configs.get(loadCount.getAndIncrement()));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(entityList.size());
    }

    // order of the allowedReadLocations does not affect the cache
    for (PolarisEntity entity : entityList) {
      StorageCredentialCacheKey key =
          buildKey(
              new PolarisEntity(new PolarisBaseEntity.Builder(entity).entityVersion(5).build()),
              true,
              Set.of("s3://bucket2/path", "s3://bucket1/path"),
              Set.of("s3://bucket3/path", "s3://bucket4/path"),
              Optional.empty(),
              Optional.empty());
      storageCredentialCache.getOrLoad(
          key, realmConfig, () -> configs.get(loadCount.getAndIncrement()));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(entityList.size());
    }

    // order of the allowedWriteLocations does not affect the cache
    for (PolarisEntity entity : entityList) {
      StorageCredentialCacheKey key =
          buildKey(
              new PolarisEntity(new PolarisBaseEntity.Builder(entity).entityVersion(5).build()),
              true,
              Set.of("s3://bucket2/path", "s3://bucket1/path"),
              Set.of("s3://bucket4/path", "s3://bucket3/path"),
              Optional.empty(),
              Optional.empty());
      storageCredentialCache.getOrLoad(
          key, realmConfig, () -> configs.get(loadCount.getAndIncrement()));
      Assertions.assertThat(storageCredentialCache.getEstimatedSize()).isEqualTo(entityList.size());
    }
  }

  @Test
  public void testExtraProperties() {
    StorageAccessConfig config =
        StorageAccessConfig.builder()
            .put(StorageAccessProperty.AWS_SECRET_KEY, "super-secret-123")
            .put(StorageAccessProperty.AWS_ENDPOINT, "test-endpoint1")
            .put(StorageAccessProperty.AWS_PATH_STYLE_ACCESS, "true")
            .build();

    List<PolarisEntity> entityList = getPolarisEntities();

    StorageCredentialCacheKey key =
        buildKey(
            entityList.get(0),
            true,
            Set.of("s3://bucket1/path", "s3://bucket2/path"),
            Set.of("s3://bucket3/path", "s3://bucket4/path"),
            Optional.empty(),
            Optional.empty());

    StorageAccessConfig result = storageCredentialCache.getOrLoad(key, realmConfig, () -> config);
    Assertions.assertThat(result.credentials())
        .containsExactly(Map.entry("s3.secret-access-key", "super-secret-123"));
    Assertions.assertThat(result.extraProperties())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of("s3.endpoint", "test-endpoint1", "s3.path-style-access", "true"));
  }

  private StorageCredentialCacheKey buildKey(
      PolarisEntity entity,
      boolean allowListAction,
      Set<String> allowedReadLocations,
      Set<String> allowedWriteLocations,
      Optional<String> refreshCredentialsEndpoint,
      Optional<String> principalName) {
    return StorageCredentialCacheKey.of(
        realmContext.getRealmIdentifier(),
        entity,
        allowListAction,
        allowedReadLocations,
        allowedWriteLocations,
        refreshCredentialsEndpoint,
        principalName.map(
            name -> org.apache.polaris.core.auth.PolarisPrincipal.of(name, Map.of(), Set.of())),
        CredentialVendingContext.empty());
  }

  private static List<StorageAccessConfig> getFakeAccessConfigs(int number, boolean expireSoon) {
    List<StorageAccessConfig> res = new ArrayList<>();
    for (int i = 1; res.size() < number; i = i + 3) {
      int finalI = i;
      String expireTime =
          expireSoon
              ? String.valueOf(System.currentTimeMillis() - 100)
              : String.valueOf(Long.MAX_VALUE);
      res.add(
          StorageAccessConfig.builder()
              .put(StorageAccessProperty.AWS_KEY_ID, "key_id_" + finalI)
              .put(StorageAccessProperty.AWS_SECRET_KEY, "key_secret_" + finalI)
              .put(StorageAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS, expireTime)
              .put(StorageAccessProperty.EXPIRATION_TIME, expireTime)
              .build());
      if (res.size() == number) return res;
      res.add(
          StorageAccessConfig.builder()
              .put(StorageAccessProperty.AZURE_SAS_TOKEN, "sas_token_" + finalI)
              .put(StorageAccessProperty.EXPIRATION_TIME, expireTime)
              .build());
      if (res.size() == number) return res;
      res.add(
          StorageAccessConfig.builder()
              .put(StorageAccessProperty.GCS_ACCESS_TOKEN, "gcs_token_" + finalI)
              .put(StorageAccessProperty.GCS_ACCESS_TOKEN_EXPIRES_AT, expireTime)
              .build());
    }
    return res;
  }

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
}
