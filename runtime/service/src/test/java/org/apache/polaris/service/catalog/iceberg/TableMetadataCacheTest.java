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
package org.apache.polaris.service.catalog.iceberg;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TableMetadataCacheTest {

  private static final String REALM = "realm";
  private static final String LOCATION = "memory://bucket/metadata/00000-abc.metadata.json";
  private static final TableMetadata METADATA =
      TableMetadata.buildFrom(
              TableMetadata.newTableMetadata(
                  new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
                  PartitionSpec.unpartitioned(),
                  "memory://bucket",
                  Map.of()))
          .withMetadataLocation(LOCATION)
          .discardChanges()
          .build();
  private static final String METADATA_JSON = TableMetadataParser.toJson(METADATA);
  private static final PolarisEntityCore TABLE_ENTITY = tableEntity(1, 1);
  private static final StorageAccessConfig STORAGE_ACCESS_CONFIG =
      StorageAccessConfig.builder().put(StorageAccessProperty.CLIENT_REGION, "us-west-2").build();

  private final AtomicInteger fileIOLoads = new AtomicInteger();
  private final AtomicInteger storageReads = new AtomicInteger();

  private FileIO countingFileIO() {
    fileIOLoads.incrementAndGet();
    InMemoryFileIO fileIO =
        new InMemoryFileIO() {
          @Override
          public InputFile newInputFile(String location) {
            storageReads.incrementAndGet();
            return super.newInputFile(location);
          }
        };
    fileIO.addFile(LOCATION, METADATA_JSON.getBytes(StandardCharsets.UTF_8));
    return fileIO;
  }

  private static PolarisEntityCore tableEntity(long id, int entityVersion) {
    return new PolarisEntityCore.Builder<>().id(id).entityVersion(entityVersion).build();
  }

  private static TableMetadataCache newCache() {
    return new TableMetadataCache(TestTableMetadataCacheConfiguration.withMaxBytes(1024 * 1024));
  }

  private String load(TableMetadataCache cache) {
    return load(cache, REALM, TABLE_ENTITY, STORAGE_ACCESS_CONFIG);
  }

  private String load(
      TableMetadataCache cache,
      String realmId,
      PolarisEntityCore tableEntity,
      StorageAccessConfig storageAccessConfig) {
    return cache.getOrLoad(
        realmId, tableEntity, LOCATION, storageAccessConfig, this::countingFileIO);
  }

  @Test
  public void testSecondLoadServedFromCache() {
    TableMetadataCache cache = newCache();
    Assertions.assertThat(cache.isEnabled()).isTrue();
    Assertions.assertThat(load(cache)).isEqualTo(METADATA_JSON);
    Assertions.assertThat(load(cache)).isEqualTo(METADATA_JSON);
    Assertions.assertThat(fileIOLoads).hasValue(1);
    Assertions.assertThat(storageReads).hasValue(1);
  }

  @Test
  public void testEntriesScopedByRealm() {
    TableMetadataCache cache = newCache();
    load(cache, "realm-1", TABLE_ENTITY, STORAGE_ACCESS_CONFIG);
    load(cache, "realm-2", TABLE_ENTITY, STORAGE_ACCESS_CONFIG);
    Assertions.assertThat(storageReads).hasValue(2);
  }

  @Test
  public void testEntriesScopedByTableEntity() {
    TableMetadataCache cache = newCache();
    load(cache, REALM, tableEntity(1, 1), STORAGE_ACCESS_CONFIG);
    load(cache, REALM, tableEntity(2, 1), STORAGE_ACCESS_CONFIG);
    Assertions.assertThat(storageReads).hasValue(2);
  }

  @Test
  public void testEntriesScopedByTableEntityVersion() {
    TableMetadataCache cache = newCache();
    load(cache, REALM, tableEntity(1, 1), STORAGE_ACCESS_CONFIG);
    load(cache, REALM, tableEntity(1, 2), STORAGE_ACCESS_CONFIG);
    Assertions.assertThat(storageReads).hasValue(2);
  }

  @Test
  public void testEntriesScopedByStorageAccessProperties() {
    TableMetadataCache cache = newCache();
    StorageAccessConfig otherRegion =
        StorageAccessConfig.builder().put(StorageAccessProperty.CLIENT_REGION, "eu-west-1").build();
    StorageAccessConfig internalEndpoint =
        StorageAccessConfig.builder()
            .put(StorageAccessProperty.CLIENT_REGION, "us-west-2")
            .putInternalProperty(
                StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), "http://internal:9000")
            .build();
    load(cache, REALM, TABLE_ENTITY, STORAGE_ACCESS_CONFIG);
    load(cache, REALM, TABLE_ENTITY, otherRegion);
    load(cache, REALM, TABLE_ENTITY, internalEndpoint);
    Assertions.assertThat(storageReads).hasValue(3);
  }

  @Test
  public void testCredentialsDoNotScopeEntries() {
    TableMetadataCache cache = newCache();
    StorageAccessConfig vendedAgain =
        StorageAccessConfig.builder()
            .put(StorageAccessProperty.CLIENT_REGION, "us-west-2")
            .put(StorageAccessProperty.AWS_KEY_ID, "key-id")
            .put(StorageAccessProperty.AWS_SECRET_KEY, "secret")
            .put(StorageAccessProperty.AWS_TOKEN, "token")
            .put(StorageAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS, "1000")
            .build();
    load(cache, REALM, TABLE_ENTITY, STORAGE_ACCESS_CONFIG);
    load(cache, REALM, TABLE_ENTITY, vendedAgain);
    Assertions.assertThat(storageReads).hasValue(1);
  }

  @Test
  public void testReadFailureThrowsRuntimeIOException() {
    String gzipLocation = "memory://bucket/metadata/00000-abc.gz.metadata.json";
    InMemoryFileIO fileIO = new InMemoryFileIO();
    // Bytes are not gzip-encoded though the name selects the GZIP codec, so the read fails.
    fileIO.addFile(gzipLocation, METADATA_JSON.getBytes(StandardCharsets.UTF_8));
    TableMetadataCache cache = newCache();
    Assertions.assertThatThrownBy(
            () ->
                cache.getOrLoad(
                    REALM, TABLE_ENTITY, gzipLocation, STORAGE_ACCESS_CONFIG, () -> fileIO))
        .isInstanceOf(RuntimeIOException.class);
  }

  @Test
  public void testGzipDocumentDecompressed() throws IOException {
    String gzipLocation = "memory://bucket/metadata/00000-abc.gz.metadata.json";
    ByteArrayOutputStream compressed = new ByteArrayOutputStream();
    try (OutputStream stream = new GZIPOutputStream(compressed)) {
      stream.write(METADATA_JSON.getBytes(StandardCharsets.UTF_8));
    }
    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(gzipLocation, compressed.toByteArray());
    TableMetadataCache cache = newCache();
    Assertions.assertThat(
            cache.getOrLoad(REALM, TABLE_ENTITY, gzipLocation, STORAGE_ACCESS_CONFIG, () -> fileIO))
        .isEqualTo(METADATA_JSON);
  }

  @Test
  public void testMalformedJsonThrowsRuntimeIOException() {
    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(LOCATION, "{not json".getBytes(StandardCharsets.UTF_8));
    TableMetadataCache cache = newCache();
    Assertions.assertThatThrownBy(
            () ->
                cache.getOrLoadMetadata(
                    REALM, TABLE_ENTITY, LOCATION, STORAGE_ACCESS_CONFIG, () -> fileIO))
        .isInstanceOf(RuntimeIOException.class);
  }

  @Test
  public void testPutSeedsSubsequentLoads() {
    TableMetadataCache cache = newCache();
    cache.put(REALM, TABLE_ENTITY, METADATA, STORAGE_ACCESS_CONFIG);
    Assertions.assertThat(load(cache)).isEqualTo(METADATA_JSON);
    Assertions.assertThat(fileIOLoads).hasValue(0);
  }

  @Test
  public void testDefaultBudgetIsShareOfMaxHeap() {
    Assertions.assertThat(
            TableMetadataCache.maxBytes(
                TestTableMetadataCacheConfiguration.defaults(), 1000L * 1024 * 1024))
        .isEqualTo(50L * 1024 * 1024);
    TableMetadataCache cache =
        new TableMetadataCache(TestTableMetadataCacheConfiguration.defaults());
    Assertions.assertThat(cache.isEnabled()).isTrue();
    load(cache);
    load(cache);
    Assertions.assertThat(storageReads).hasValue(1);
  }

  @Test
  public void testFractionOfMaxHeapSizeSetsBudget() {
    Assertions.assertThat(
            TableMetadataCache.maxBytes(
                TestTableMetadataCacheConfiguration.withFractionOfMaxHeapSize(0.1),
                1000L * 1024 * 1024))
        .isEqualTo(100L * 1024 * 1024);
    Assertions.assertThat(
            new TableMetadataCache(TestTableMetadataCacheConfiguration.withFractionOfMaxHeapSize(0))
                .isEnabled())
        .isFalse();
  }

  @Test
  public void testMaxBytesTakesPrecedenceOverFractionOfMaxHeapSize() {
    Assertions.assertThat(
            TableMetadataCache.maxBytes(
                new TestTableMetadataCacheConfiguration(OptionalLong.of(1024), 0.1, 1024),
                1000L * 1024 * 1024))
        .isEqualTo(1024);
  }

  @Test
  public void testZeroBudgetDisablesCaching() {
    TableMetadataCache cache =
        new TableMetadataCache(TestTableMetadataCacheConfiguration.disabled());
    Assertions.assertThat(cache.isEnabled()).isFalse();
    load(cache);
    load(cache);
    Assertions.assertThat(storageReads).hasValue(2);
  }

  @Test
  public void testDocumentLargerThanContentLengthCapNotCached() {
    TableMetadataCache cache =
        new TableMetadataCache(
            new TestTableMetadataCacheConfiguration(1024 * 1024, METADATA_JSON.length() - 1));
    load(cache);
    load(cache);
    Assertions.assertThat(storageReads).hasValue(2);
    cache.put("other-realm", TABLE_ENTITY, METADATA, STORAGE_ACCESS_CONFIG);
    load(cache, "other-realm", TABLE_ENTITY, STORAGE_ACCESS_CONFIG);
    Assertions.assertThat(storageReads).hasValue(3);
  }

  @Test
  public void testDocumentAtContentLengthCapCached() {
    TableMetadataCache cache =
        new TableMetadataCache(
            new TestTableMetadataCacheConfiguration(1024 * 1024, METADATA_JSON.length()));
    load(cache);
    load(cache);
    Assertions.assertThat(storageReads).hasValue(1);
  }

  @Test
  public void testEntryHeavierThanBudgetNotCached() {
    TableMetadataCache cache =
        new TableMetadataCache(TestTableMetadataCacheConfiguration.withMaxBytes(10));
    Assertions.assertThat(cache.isEnabled()).isTrue();
    load(cache);
    load(cache);
    Assertions.assertThat(storageReads).hasValue(2);
  }
}
