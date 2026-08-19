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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TableMetadataCacheTest {

  private static final String LOCATION = "memory://bucket/metadata/00000-abc.metadata.json";
  private static final String METADATA_JSON = "{\"format-version\":2}";

  private final AtomicInteger storageReads = new AtomicInteger();

  private FileIO countingFileIO() {
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
    return new TableMetadataCache.Key("realm", catalogId, 1, LOCATION);
  }

  private static TableMetadataCache.Key key(long catalogId, long catalogVersion) {
    return new TableMetadataCache.Key("realm", catalogId, catalogVersion, LOCATION);
  }

  @Test
  public void testSecondLoadServedFromCache() {
    TableMetadataCache cache = new TableMetadataCache(() -> 1024 * 1024);
    Assertions.assertThat(cache.isEnabled()).isTrue();
    Assertions.assertThat(cache.getOrLoad(key(1), countingFileIO())).isEqualTo(METADATA_JSON);
    Assertions.assertThat(cache.getOrLoad(key(1), countingFileIO())).isEqualTo(METADATA_JSON);
    Assertions.assertThat(storageReads).hasValue(1);
  }

  @Test
  public void testEntriesScopedByCatalog() {
    TableMetadataCache cache = new TableMetadataCache(() -> 1024 * 1024);
    cache.getOrLoad(key(1), countingFileIO());
    cache.getOrLoad(key(2), countingFileIO());
    Assertions.assertThat(storageReads).hasValue(2);
  }

  @Test
  public void testEntriesScopedByCatalogVersion() {
    TableMetadataCache cache = new TableMetadataCache(() -> 1024 * 1024);
    cache.getOrLoad(key(1, 1), countingFileIO());
    cache.getOrLoad(key(1, 2), countingFileIO());
    Assertions.assertThat(storageReads).hasValue(2);
  }

  @Test
  public void testReadFailureThrowsRuntimeIOException() {
    String gzipLocation = "memory://bucket/metadata/00000-abc.gz.metadata.json";
    InMemoryFileIO fileIO = new InMemoryFileIO();
    // Bytes are not gzip-encoded though the name selects the GZIP codec, so the read fails.
    fileIO.addFile(gzipLocation, METADATA_JSON.getBytes(StandardCharsets.UTF_8));
    TableMetadataCache cache = new TableMetadataCache(() -> 1024 * 1024);
    Assertions.assertThatThrownBy(
            () -> cache.getOrLoad(new TableMetadataCache.Key("realm", 1, 1, gzipLocation), fileIO))
        .isInstanceOf(RuntimeIOException.class);
  }

  @Test
  public void testPutSeedsSubsequentLoads() {
    TableMetadataCache cache = new TableMetadataCache(() -> 1024 * 1024);
    cache.put(key(1), METADATA_JSON);
    Assertions.assertThat(cache.getOrLoad(key(1), countingFileIO())).isEqualTo(METADATA_JSON);
    Assertions.assertThat(storageReads).hasValue(0);
  }

  @Test
  public void testZeroBudgetDisablesCaching() {
    TableMetadataCache cache = new TableMetadataCache(() -> 0);
    Assertions.assertThat(cache.isEnabled()).isFalse();
    cache.getOrLoad(key(1), countingFileIO());
    cache.getOrLoad(key(1), countingFileIO());
    Assertions.assertThat(storageReads).hasValue(2);
  }

  @Test
  public void testEntryHeavierThanBudgetNotCached() {
    TableMetadataCache cache = new TableMetadataCache(() -> 10);
    Assertions.assertThat(cache.isEnabled()).isTrue();
    cache.getOrLoad(key(1), countingFileIO());
    cache.getOrLoad(key(1), countingFileIO());
    Assertions.assertThat(storageReads).hasValue(2);
  }
}
