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
  private static final long TABLE_ID = 3;

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

  private static TableMetadataCache newCache() {
    return new TableMetadataCache(TableMetadataCacheTestConfiguration.withMaxBytes(1024 * 1024));
  }

  private String load(TableMetadataCache cache) {
    return load(cache, REALM, TABLE_ID);
  }

  private String load(TableMetadataCache cache, String realmId, long tableEntityId) {
    return cache.getOrLoad(realmId, tableEntityId, LOCATION, this::countingFileIO);
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
    load(cache, "realm-1", TABLE_ID);
    load(cache, "realm-2", TABLE_ID);
    Assertions.assertThat(storageReads).hasValue(2);
  }

  @Test
  public void testEntriesScopedByTableEntity() {
    TableMetadataCache cache = newCache();
    load(cache, REALM, TABLE_ID);
    load(cache, REALM, 4);
    Assertions.assertThat(storageReads).hasValue(2);
  }

  @Test
  public void testReadFailureThrowsRuntimeIOException() {
    String gzipLocation = "memory://bucket/metadata/00000-abc.gz.metadata.json";
    InMemoryFileIO fileIO = new InMemoryFileIO();
    // Bytes are not gzip-encoded though the name selects the GZIP codec, so the read fails.
    fileIO.addFile(gzipLocation, METADATA_JSON.getBytes(StandardCharsets.UTF_8));
    TableMetadataCache cache = newCache();
    Assertions.assertThatThrownBy(
            () -> cache.getOrLoad(REALM, TABLE_ID, gzipLocation, () -> fileIO))
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
    Assertions.assertThat(cache.getOrLoad(REALM, TABLE_ID, gzipLocation, () -> fileIO))
        .isEqualTo(METADATA_JSON);
  }

  @Test
  public void testMalformedJsonThrowsRuntimeIOException() {
    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(LOCATION, "{not json".getBytes(StandardCharsets.UTF_8));
    TableMetadataCache cache = newCache();
    Assertions.assertThatThrownBy(
            () -> cache.getOrLoadMetadata(REALM, TABLE_ID, LOCATION, () -> fileIO))
        .isInstanceOf(RuntimeIOException.class);
  }

  @Test
  public void testPutSeedsSubsequentLoads() {
    TableMetadataCache cache = newCache();
    cache.put(REALM, TABLE_ID, METADATA);
    Assertions.assertThat(load(cache)).isEqualTo(METADATA_JSON);
    Assertions.assertThat(fileIOLoads).hasValue(0);
  }

  @Test
  public void testDefaultBudgetIsShareOfMaxHeap() {
    TableMetadataCacheConfiguration configuration =
        ImmutableTableMetadataCacheTestConfiguration.builder().build();
    Assertions.assertThat(TableMetadataCache.maxBytes(configuration, 1000L * 1024 * 1024))
        .isEqualTo(50L * 1024 * 1024);
    TableMetadataCache cache = new TableMetadataCache(configuration);
    Assertions.assertThat(cache.isEnabled()).isTrue();
    load(cache);
    load(cache);
    Assertions.assertThat(storageReads).hasValue(1);
  }

  @Test
  public void testFractionOfMaxHeapSizeSetsBudget() {
    Assertions.assertThat(
            TableMetadataCache.maxBytes(
                ImmutableTableMetadataCacheTestConfiguration.builder()
                    .fractionOfMaxHeapSize(0.1)
                    .build(),
                1000L * 1024 * 1024))
        .isEqualTo(100L * 1024 * 1024);
    Assertions.assertThat(
            new TableMetadataCache(
                    ImmutableTableMetadataCacheTestConfiguration.builder()
                        .fractionOfMaxHeapSize(0)
                        .build())
                .isEnabled())
        .isFalse();
  }

  @Test
  public void testMaxBytesTakesPrecedenceOverFractionOfMaxHeapSize() {
    Assertions.assertThat(
            TableMetadataCache.maxBytes(
                ImmutableTableMetadataCacheTestConfiguration.builder()
                    .maxBytes(1024)
                    .fractionOfMaxHeapSize(0.1)
                    .build(),
                1000L * 1024 * 1024))
        .isEqualTo(1024);
  }

  @Test
  public void testZeroBudgetDisablesCaching() {
    TableMetadataCache cache =
        new TableMetadataCache(TableMetadataCacheTestConfiguration.disabled());
    Assertions.assertThat(cache.isEnabled()).isFalse();
    load(cache);
    load(cache);
    Assertions.assertThat(storageReads).hasValue(2);
  }

  @Test
  public void testEntryHeavierThanPerEntryCapNotCached() {
    // The estimated entry size exceeds the document's character count.
    TableMetadataCache cache =
        new TableMetadataCache(
            ImmutableTableMetadataCacheTestConfiguration.builder()
                .maxBytes(1024 * 1024)
                .maxBytesPerEntry(METADATA_JSON.length())
                .build());
    load(cache);
    load(cache);
    Assertions.assertThat(storageReads).hasValue(2);
    cache.put("other-realm", TABLE_ID, METADATA);
    load(cache, "other-realm", TABLE_ID);
    Assertions.assertThat(storageReads).hasValue(3);
  }

  @Test
  public void testEntryHeavierThanBudgetNotCached() {
    TableMetadataCache cache =
        new TableMetadataCache(TableMetadataCacheTestConfiguration.withMaxBytes(10));
    Assertions.assertThat(cache.isEnabled()).isTrue();
    load(cache);
    load(cache);
    Assertions.assertThat(storageReads).hasValue(2);
  }
}
