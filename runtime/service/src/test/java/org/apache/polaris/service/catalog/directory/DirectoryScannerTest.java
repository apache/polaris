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
package org.apache.polaris.service.catalog.directory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.parquet.Parquet;
import org.apache.polaris.service.catalog.io.ExceptionMappingFileIO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

class DirectoryScannerTest {
  private static final String BASE = "memory:/bucket/images/";

  private PrefixFileIO sourceIO;
  private Table table;

  @BeforeEach
  void setUp() {
    sourceIO = new PrefixFileIO();
    for (String name : List.of("a.jpg", "b.png", "thumbs/a.jpg", "doc.txt")) {
      sourceIO.addFile(BASE + name, new byte[] {1, 2, 3});
    }
    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize("test", Map.of());
    catalog.createNamespace(Namespace.of("ns"));
    table =
        catalog.createTable(
            TableIdentifier.of("ns", "images"), DirectoryCatalogHandler.DIRECTORY_TABLE_SCHEMA);
  }

  static Stream<Arguments> filters() {
    return Stream.of(
        Arguments.of(null, null, List.of("a.jpg", "b.png", "thumbs/a.jpg", "doc.txt")),
        Arguments.of(
            List.of(".*\\.jpg$", ".*\\.png$"), null, List.of("a.jpg", "b.png", "thumbs/a.jpg")),
        Arguments.of(
            List.of(".*\\.jpg$", ".*\\.png$"), List.of(".*thumbs/.*"), List.of("a.jpg", "b.png")),
        Arguments.of(null, List.of(".*"), List.of()));
  }

  @ParameterizedTest
  @MethodSource("filters")
  void scanAppliesFilters(List<String> include, List<String> exclude, List<String> expected)
      throws IOException {
    long count = DirectoryScanner.scan(table, sourceIO, BASE, include, exclude);

    assertThat(count).isEqualTo(expected.size());
    assertThat(readUris())
        .containsExactlyInAnyOrderElementsOf(expected.stream().map(n -> BASE + n).toList());
  }

  @Test
  void defaultScanServiceScansTheLocation() throws IOException {
    long count =
        new DefaultDirectoryScanService()
            .scan(
                TableIdentifier.of("ns", "images"),
                table,
                sourceIO,
                BASE,
                List.of(".*\\.png$"),
                null);

    assertThat(count).isEqualTo(1);
    assertThat(readUris()).containsExactly(BASE + "b.png");
  }

  @Test
  void rescanReplacesPreviousContent() throws IOException {
    DirectoryScanner.scan(table, sourceIO, BASE, null, null);
    sourceIO.deleteFile(BASE + "doc.txt");
    sourceIO.addFile(BASE + "new.jpg", new byte[] {1});

    long count = DirectoryScanner.scan(table, sourceIO, BASE, null, null);

    assertThat(count).isEqualTo(4);
    assertThat(readUris())
        .containsExactlyInAnyOrder(
            BASE + "a.jpg", BASE + "b.png", BASE + "thumbs/a.jpg", BASE + "new.jpg");
  }

  @Test
  void scanRecordsObjectMetadata() throws IOException {
    DirectoryScanner.scan(table, sourceIO, BASE, List.of(".*a\\.jpg$"), List.of(".*thumbs/.*"));

    assertThat(readRecords())
        .singleElement()
        .satisfies(
            r -> {
              assertThat(r.getField("file_uri")).isEqualTo(BASE + "a.jpg");
              assertThat(r.getField("content_type")).isEqualTo("image/jpeg");
              assertThat(r.getField("size")).isEqualTo(3L);
              assertThat(r.getField("last_modified")).isNotNull();
            });
  }

  @Test
  void scanSupportsFileIOWrappedByTheFileIOFactory() throws IOException {
    long count =
        DirectoryScanner.scan(table, ExceptionMappingFileIO.wrap(sourceIO), BASE, null, null);

    assertThat(count).isEqualTo(4);
    assertThat(readUris()).hasSize(4);
  }

  @Test
  void scanFailsWithoutPrefixOperationsSupport() {
    FileIO plainIO = Mockito.mock(FileIO.class);
    assertThatThrownBy(() -> DirectoryScanner.scan(table, plainIO, BASE, null, null))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  private List<String> readUris() throws IOException {
    return readRecords().stream().map(r -> (String) r.getField("file_uri")).toList();
  }

  /** Reads the data files directly, to not depend on Iceberg's ORC support. */
  private List<Record> readRecords() throws IOException {
    List<Record> records = new ArrayList<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        try (CloseableIterable<Record> rows =
            Parquet.read(table.io().newInputFile(task.file().location()))
                .project(table.schema())
                .createReaderFunc(
                    fileSchema -> GenericParquetReaders.buildReader(table.schema(), fileSchema))
                .build()) {
          rows.forEach(records::add);
        }
      }
    }
    return records;
  }

  /** In-memory {@link FileIO} that, unlike {@link InMemoryFileIO}, can list a prefix. */
  private static class PrefixFileIO extends InMemoryFileIO implements SupportsPrefixOperations {
    private final Map<String, FileInfo> files = new LinkedHashMap<>();

    @Override
    public void addFile(String location, byte[] contents) {
      super.addFile(location, contents);
      files.put(location, new FileInfo(location, contents.length, 1_700_000_000_000L));
    }

    @Override
    public void deleteFile(String location) {
      super.deleteFile(location);
      files.remove(location);
    }

    @Override
    public Iterable<FileInfo> listPrefix(String prefix) {
      return files.values().stream().filter(f -> f.location().startsWith(prefix)).toList();
    }

    @Override
    public void deletePrefix(String prefix) {
      listPrefix(prefix).forEach(f -> deleteFile(f.location()));
    }
  }
}
