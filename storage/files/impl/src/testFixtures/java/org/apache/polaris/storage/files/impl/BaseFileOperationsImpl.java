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

package org.apache.polaris.storage.files.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.apache.polaris.storage.files.api.FileFilter;
import org.apache.polaris.storage.files.api.FileSpec;
import org.apache.polaris.storage.files.api.FileType;
import org.apache.polaris.storage.files.api.PurgeSpec;
import org.apache.polaris.storage.files.api.PurgeStats;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
public abstract class BaseFileOperationsImpl {
  @InjectSoftAssertions protected SoftAssertions soft;

  protected void icebergIntegration(FileIO fileIO, Map<String, String> properties)
      throws Exception {
    var tables = new ConcurrentHashMap<TableIdentifier, String>();

    var prefix = prefix() + "ns/some_table/";

    var ops = new TableOps(tables, fileIO, TableIdentifier.of(Namespace.of("ns"), "some_table"));
    var schema = new Schema(Types.NestedField.required(3, "id", Types.IntegerType.get()));
    var metadata =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), SortOrder.unsorted(), prefix, properties);
    ops.commit(null, metadata);

    var table = new BaseTable(ops, "some_table", x -> {});
    var dataFile1 = dataFile(fileIO, prefix + "data-file-1.parquet");
    table.newFastAppend().appendFile(dataFile1).commit();
    var dataFile2a = dataFile(fileIO, prefix + "data-file-2a.parquet");
    var dataFile2b = dataFile(fileIO, prefix + "data-file-2b.parquet");
    table.newFastAppend().appendFile(dataFile2a).appendFile(dataFile2b).commit();
    var dataFile3a = dataFile(fileIO, prefix + "data-file-3a.parquet");
    var dataFile3b = dataFile(fileIO, prefix + "data-file-3b.parquet");
    var dataFile3c = dataFile(fileIO, prefix + "data-file-3c.parquet");
    table
        .newFastAppend()
        .appendFile(dataFile3a)
        .appendFile(dataFile3b)
        .appendFile(dataFile3c)
        .commit();

    var metadataLocation = table.operations().current().metadataFileLocation();

    var fileOps = new FileOperationsImpl(fileIO);

    var identifiedFiles = fileOps.identifyIcebergTableFiles(metadataLocation, true).toList();
    // 4 table-metadata in total (identify can only yield the given metadata and all mentioned in
    //   the metadata-log)
    // 3 manifest lists
    // 3 manifest files
    // 6 data files
    soft.assertThat(identifiedFiles)
        .hasSize(16)
        .extracting(FileSpec::location)
        .contains(
            dataFile1.location(),
            dataFile2a.location(),
            dataFile2b.location(),
            dataFile3a.location(),
            dataFile3b.location(),
            dataFile3c.location(),
            metadataLocation);
    soft.assertThat(
            identifiedFiles.stream()
                .collect(
                    Collectors.groupingBy(
                        fileSpec -> fileSpec.fileType().orElse(FileType.UNKNOWN),
                        Collectors.counting())))
        .containsAllEntriesOf(
            Map.of(
                FileType.ICEBERG_METADATA,
                4L,
                FileType.ICEBERG_MANIFEST_LIST,
                3L,
                FileType.ICEBERG_MANIFEST_FILE,
                3L,
                FileType.ICEBERG_DATA_FILE,
                6L));

    var foundFiles = fileOps.findFiles(prefix, FileFilter.alwaysTrue()).toList();
    soft.assertThat(foundFiles)
        .extracting(FileSpec::location)
        .containsExactlyInAnyOrderElementsOf(
            identifiedFiles.stream().map(FileSpec::location).collect(Collectors.toList()));
    soft.assertThat(
            foundFiles.stream()
                .collect(
                    Collectors.groupingBy(
                        BaseFileOperationsImpl::guessTypeFromName, Collectors.counting())))
        .containsAllEntriesOf(
            Map.of(
                FileType.ICEBERG_METADATA,
                4L,
                FileType.ICEBERG_MANIFEST_LIST,
                3L,
                FileType.ICEBERG_MANIFEST_FILE,
                3L,
                FileType.UNKNOWN,
                6L));

    soft.assertThat(fileOps.purgeIcebergTable(metadataLocation, PurgeSpec.DEFAULT_INSTANCE))
        .extracting(PurgeStats::purgedFiles)
        .isEqualTo(16L);
    soft.assertThat(fileOps.findFiles(prefix, FileFilter.alwaysTrue())).isEmpty();
  }

  /** Guesses the given file's type from its name, not guaranteed to be accurate. */
  static FileType guessTypeFromName(FileSpec fileSpec) {
    var location = fileSpec.location();
    var lastSlash = location.lastIndexOf('/');
    var fileName = lastSlash > 0 ? location.substring(lastSlash + 1) : location;

    if (fileName.contains(".metadata.json")) {
      return FileType.ICEBERG_METADATA;
    } else if (fileName.startsWith("snap-")) {
      return FileType.ICEBERG_MANIFEST_LIST;
    } else if (fileName.contains("-m")) {
      return FileType.ICEBERG_MANIFEST_FILE;
    }
    return FileType.UNKNOWN;
  }

  DataFile dataFile(FileIO fileIO, String path) throws Exception {
    var dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(path)
            .withFileSizeInBytes(10L)
            .withRecordCount(2L)
            .build();
    write(fileIO, path, new byte[(int) dataFile.fileSizeInBytes()]);
    return dataFile;
  }

  protected void write(FileIO fileIO, String name, byte[] bytes) throws Exception {
    var outFile = fileIO.newOutputFile(name);
    try (var out = outFile.create()) {
      out.write(bytes);
    }
  }

  protected abstract String prefix();
}
