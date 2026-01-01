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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.stream.IntStream;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.storage.files.api.PurgeSpec;
import org.apache.polaris.storage.files.api.PurgeStats;
import org.junit.jupiter.api.Test;

public abstract class BaseITFileOperationsImpl extends BaseFileOperationsImpl {

  /** Verify that batch-deletions do not fail in case some files do not exist. */
  @Test
  public void batchDeleteNonExistentFiles() throws Exception {
    try (var fileIO = initializedFileIO()) {
      var prefix = prefix() + "batchDeleteNonExistentFiles/";

      write(fileIO, prefix + "1", new byte[1]);
      write(fileIO, prefix + "3", new byte[1]);
      write(fileIO, prefix + "5", new byte[1]);

      var fileOps = new FileOperationsImpl(fileIO);
      var result =
          fileOps.purgeFiles(
              IntStream.range(0, 10).mapToObj(i -> prefix + i), PurgeSpec.DEFAULT_INSTANCE);
      soft.assertThat(result)
          .extracting(PurgeStats::purgedFiles, PurgeStats::failedPurges)
          // Iceberg does not yield the correct number of purged files, 3/7 in this test (via
          // `BulkDeletionFailureException`) in case those do not exist.
          .containsExactly(10L, 0L);

      result =
          fileOps.purgeFiles(
              IntStream.range(20, 40).mapToObj(i -> prefix + i), PurgeSpec.DEFAULT_INSTANCE);
      soft.assertThat(result)
          .extracting(PurgeStats::purgedFiles, PurgeStats::failedPurges)
          // Iceberg does not yield the correct number of purged files, 0/20 in this test (via
          // `BulkDeletionFailureException`) in case those do not exist.
          .containsExactly(20L, 0L);

      result =
          fileOps.purgeFiles(
              IntStream.range(40, 60).mapToObj(i -> prefix + "40"), PurgeSpec.DEFAULT_INSTANCE);
      soft.assertThat(result)
          .extracting(PurgeStats::purgedFiles, PurgeStats::failedPurges)
          // Iceberg does not yield the correct number of purged files, 0/1 in this test (via
          // `BulkDeletionFailureException`) in case those do not exist.
          .containsExactly(1L, 0L);
    }
  }

  @Test
  public void purgeIcebergTable() throws Exception {
    try (var fileIO = initializedFileIO()) {
      var prefix = prefix() + "purgeIcebergTable/";
      var fixtures = new IcebergFixtures(prefix, 3, 3, 3);
      var tableMetadataPath = fixtures.prefix + "foo.metadata.json";

      write(fileIO, tableMetadataPath, fixtures.tableMetadataBytes);
      for (var snapshotId = 1; snapshotId <= fixtures.numSnapshots; snapshotId++) {
        write(
            fileIO,
            fixtures.manifestListPath(snapshotId),
            fixtures.serializedManifestList(snapshotId));

        for (int mf = 0; mf < fixtures.numManifestFiles; mf++) {
          var manifestFilePath = fixtures.manifestFilePath(snapshotId, mf);
          write(
              fileIO,
              manifestFilePath,
              fixtures.serializedManifestFile(snapshotId, mf, manifestFilePath));
        }
      }

      var fileOps = new FileOperationsImpl(fileIO);

      var purgeStats = fileOps.purgeIcebergTable(tableMetadataPath, PurgeSpec.DEFAULT_INSTANCE);

      assertThat(purgeStats.purgedFiles())
          // 1st "1" --> metadata-json
          // 2nd "1" --> manifest-list
          // 3rd "1" --> manifest-file
          .isEqualTo(
              1
                  + fixtures.numSnapshots
                      * (1 + (long) fixtures.numManifestFiles * (1 + fixtures.numDataFiles)));
    }
  }

  @Test
  public void icebergIntegration() throws Exception {
    try (var fileIO = initializedFileIO()) {
      icebergIntegration(fileIO, icebergProperties());
    }
  }

  public FileIO initializedFileIO() {
    var fileIO = createFileIO();
    fileIO.initialize(icebergProperties());
    return fileIO;
  }

  protected abstract FileIO createFileIO();

  protected abstract Map<String, String> icebergProperties();
}
