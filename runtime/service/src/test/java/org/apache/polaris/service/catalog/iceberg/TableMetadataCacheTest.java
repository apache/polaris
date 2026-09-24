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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.polaris.core.storage.FileStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TableMetadataCacheTest {

  private static final String LOCATION = "memory://bucket/metadata/00000-abc.metadata.json";
  private static final String METADATA_JSON = "{\"format-version\":2}";
  private static final PolarisStorageConfigurationInfo STORAGE_CONFIGURATION =
      FileStorageConfigurationInfo.builder().addAllowedLocations("file:///bucket").build();

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

  private static TableMetadataCache.Key key(long catalogId) {
    return key(catalogId, 1, STORAGE_CONFIGURATION);
  }

  private static TableMetadataCache.Key key(
      long catalogId, long catalogVersion, PolarisStorageConfigurationInfo storageConfiguration) {
    return new TableMetadataCache.Key(
        "realm", catalogId, catalogVersion, storageConfiguration, LOCATION);
  }

  @Test
  public void testSecondLoadServedFromCache() {
    TableMetadataCache cache =
        new TableMetadataCache(TestTableMetadataCacheConfiguration.withMaxBytes(1024 * 1024));
    Assertions.assertThat(cache.isEnabled()).isTrue();
    Assertions.assertThat(cache.getOrLoad(key(1), this::countingFileIO)).isEqualTo(METADATA_JSON);
    Assertions.assertThat(cache.getOrLoad(key(1), this::countingFileIO)).isEqualTo(METADATA_JSON);
    Assertions.assertThat(fileIOLoads).hasValue(1);
    Assertions.assertThat(storageReads).hasValue(1);
  }

  @Test
  public void testEntriesScopedByCatalog() {
    TableMetadataCache cache =
        new TableMetadataCache(TestTableMetadataCacheConfiguration.withMaxBytes(1024 * 1024));
    cache.getOrLoad(key(1), this::countingFileIO);
    cache.getOrLoad(key(2), this::countingFileIO);
    Assertions.assertThat(storageReads).hasValue(2);
  }

  @Test
  public void testEntriesScopedByCatalogVersion() {
    TableMetadataCache cache =
        new TableMetadataCache(TestTableMetadataCacheConfiguration.withMaxBytes(1024 * 1024));
    cache.getOrLoad(key(1, 1, STORAGE_CONFIGURATION), this::countingFileIO);
    cache.getOrLoad(key(1, 2, STORAGE_CONFIGURATION), this::countingFileIO);
    Assertions.assertThat(storageReads).hasValue(2);
  }

  @Test
  public void testEntriesScopedByStorageConfiguration() {
    TableMetadataCache cache =
        new TableMetadataCache(TestTableMetadataCacheConfiguration.withMaxBytes(1024 * 1024));
    PolarisStorageConfigurationInfo otherStorageConfiguration =
        FileStorageConfigurationInfo.builder().addAllowedLocations("file:///other").build();
    cache.getOrLoad(key(1, 1, STORAGE_CONFIGURATION), this::countingFileIO);
    cache.getOrLoad(key(1, 1, otherStorageConfiguration), this::countingFileIO);
    cache.getOrLoad(key(1, 1, null), this::countingFileIO);
    Assertions.assertThat(storageReads).hasValue(3);
  }

  @Test
  public void testEqualStorageConfigurationsShareEntry() {
    TableMetadataCache cache =
        new TableMetadataCache(TestTableMetadataCacheConfiguration.withMaxBytes(1024 * 1024));
    PolarisStorageConfigurationInfo equalStorageConfiguration =
        PolarisStorageConfigurationInfo.deserialize(STORAGE_CONFIGURATION.serialize());
    cache.getOrLoad(key(1, 1, STORAGE_CONFIGURATION), this::countingFileIO);
    cache.getOrLoad(key(1, 1, equalStorageConfiguration), this::countingFileIO);
    Assertions.assertThat(storageReads).hasValue(1);
  }

  @Test
  public void testReadFailureThrowsRuntimeIOException() {
    String gzipLocation = "memory://bucket/metadata/00000-abc.gz.metadata.json";
    InMemoryFileIO fileIO = new InMemoryFileIO();
    // Bytes are not gzip-encoded though the name selects the GZIP codec, so the read fails.
    fileIO.addFile(gzipLocation, METADATA_JSON.getBytes(StandardCharsets.UTF_8));
    TableMetadataCache cache =
        new TableMetadataCache(TestTableMetadataCacheConfiguration.withMaxBytes(1024 * 1024));
    Assertions.assertThatThrownBy(
            () ->
                cache.getOrLoad(
                    new TableMetadataCache.Key("realm", 1, 1, STORAGE_CONFIGURATION, gzipLocation),
                    () -> fileIO))
        .isInstanceOf(RuntimeIOException.class);
  }

  @Test
  public void testMalformedJsonThrowsRuntimeIOException() {
    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(LOCATION, "{not json".getBytes(StandardCharsets.UTF_8));
    TableMetadataCache cache =
        new TableMetadataCache(TestTableMetadataCacheConfiguration.withMaxBytes(1024 * 1024));
    Assertions.assertThatThrownBy(() -> cache.getOrLoadMetadata(key(1), () -> fileIO))
        .isInstanceOf(RuntimeIOException.class);
  }

  @Test
  public void testPutSeedsSubsequentLoads() {
    TableMetadataCache cache =
        new TableMetadataCache(TestTableMetadataCacheConfiguration.withMaxBytes(1024 * 1024));
    cache.put(key(1), METADATA_JSON);
    Assertions.assertThat(cache.getOrLoad(key(1), this::countingFileIO)).isEqualTo(METADATA_JSON);
    Assertions.assertThat(fileIOLoads).hasValue(0);
  }

  @Test
  public void testZeroBudgetDisablesCaching() {
    TableMetadataCache cache =
        new TableMetadataCache(TestTableMetadataCacheConfiguration.disabled());
    Assertions.assertThat(cache.isEnabled()).isFalse();
    cache.getOrLoad(key(1), this::countingFileIO);
    cache.getOrLoad(key(1), this::countingFileIO);
    Assertions.assertThat(storageReads).hasValue(2);
  }

  @Test
  public void testDocumentLargerThanContentLengthCapNotCached() {
    TableMetadataCache cache =
        new TableMetadataCache(
            new TestTableMetadataCacheConfiguration(1024 * 1024, METADATA_JSON.length() - 1));
    cache.getOrLoad(key(1), this::countingFileIO);
    cache.getOrLoad(key(1), this::countingFileIO);
    Assertions.assertThat(storageReads).hasValue(2);
    cache.put(key(2), METADATA_JSON);
    cache.getOrLoad(key(2), this::countingFileIO);
    Assertions.assertThat(storageReads).hasValue(3);
  }

  @Test
  public void testDocumentAtContentLengthCapCached() {
    TableMetadataCache cache =
        new TableMetadataCache(
            new TestTableMetadataCacheConfiguration(1024 * 1024, METADATA_JSON.length()));
    cache.getOrLoad(key(1), this::countingFileIO);
    cache.getOrLoad(key(1), this::countingFileIO);
    Assertions.assertThat(storageReads).hasValue(1);
  }

  @Test
  public void testEntryHeavierThanBudgetNotCached() {
    TableMetadataCache cache =
        new TableMetadataCache(TestTableMetadataCacheConfiguration.withMaxBytes(10));
    Assertions.assertThat(cache.isEnabled()).isTrue();
    cache.getOrLoad(key(1), this::countingFileIO);
    cache.getOrLoad(key(1), this::countingFileIO);
    Assertions.assertThat(storageReads).hasValue(2);
  }
}
