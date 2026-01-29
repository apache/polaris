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

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.objectstoragemock.HeapStorageBucket.newHeapStorageBucket;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.storage.files.api.FileFilter;
import org.apache.polaris.storage.files.api.FileSpec;
import org.apache.polaris.storage.files.api.PurgeSpec;
import org.apache.polaris.storage.files.api.PurgeStats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.objectstoragemock.Bucket;
import org.projectnessie.objectstoragemock.InterceptingBucket;
import org.projectnessie.objectstoragemock.MockObject;
import org.projectnessie.objectstoragemock.ObjectStorageMock;

public abstract class BaseTestFileOperationsImpl extends BaseFileOperationsImpl {
  @ParameterizedTest
  @MethodSource
  public void purgeIcebergTable(
      int numSnapshots, int numManifestFiles, int numDataFiles, int deleteBatchSize)
      throws Exception {

    var fixtures = new IcebergFixtures(prefix(), numSnapshots, numManifestFiles, numDataFiles);

    var baseBucket = Bucket.builder().build();
    var interceptingBucket = new InterceptingBucket(baseBucket);
    interceptingBucket.setRetriever(
        path -> {
          if ("foo.metadata.json".equals(path)) {
            return mockObjectForBytes(fixtures.tableMetadataBytes);
          }
          // manifest list example: 00000/snap-0.avro
          if (path.endsWith(".avro")) {
            var firstSlash = path.indexOf('/');
            var snapshot = Integer.parseInt(path.substring(0, firstSlash));
            if (path.contains("/snap-")) {
              var manifestListBytes = fixtures.serializedManifestList(snapshot);
              return mockObjectForBytes(manifestListBytes);
            } else {
              var nextSlash = path.indexOf('/', firstSlash + 1);
              var manifest = Integer.parseInt(path.substring(firstSlash + 1, nextSlash));
              var manifestBytes = fixtures.serializedManifestFile(snapshot, manifest, path);
              return mockObjectForBytes(manifestBytes);
            }
          }
          if (path.endsWith(".parquet")) {
            // Need some data file here
            return Optional.of(
                MockObject.builder()
                    .contentType("application/parquet")
                    .contentLength(1)
                    .lastModified(0L)
                    .build());
          }
          return Optional.empty();
        });
    var deletes = ConcurrentHashMap.<String>newKeySet();
    interceptingBucket.setDeleter(path -> Optional.of(deletes.add(path)));

    var objectStorageMock =
        ObjectStorageMock.builder().putBuckets(bucket(), interceptingBucket).build();

    try (var server = objectStorageMock.start();
        var fileIO = initializedFileIO(server)) {
      fileIO.initialize(icebergProperties(server));

      var prefix = prefix();

      var fileOps = new FileOperationsImpl(fileIO);

      var purgeStats =
          fileOps.purgeIcebergTable(
              prefix + "foo.metadata.json",
              PurgeSpec.DEFAULT_INSTANCE.withDeleteBatchSize(deleteBatchSize));

      assertThat(purgeStats.purgeFileRequests())
          // 1st "1" --> metadata-json
          // 2nd "1" --> manifest-list
          // 3rd "1" --> manifest-file
          .isEqualTo(
              1
                  + fixtures.numSnapshots
                      * (1 + (long) fixtures.numManifestFiles * (1 + fixtures.numDataFiles)))
          .isEqualTo(deletes.size());
    }
  }

  static Stream<Arguments> purgeIcebergTable() {
    return Stream.of(
        // one snapshot, three manifest files, 5 data files per manifest file
        Arguments.of(1, 3, 5, 10),
        // five snapshot, 7 manifest files per snapshot, 13 data files per manifest file
        Arguments.of(5, 7, 13, 10),
        // five snapshot, 7 manifest files per snapshot, 1000 data files per manifest file,
        // batch delete size 500
        Arguments.of(5, 7, 1_000, 500));
  }

  static Optional<MockObject> mockObjectForBytes(byte[] bytes) {
    return Optional.of(
        MockObject.builder()
            .contentType("application/json")
            .lastModified(System.currentTimeMillis())
            .contentLength(bytes.length)
            .writer(
                (range, output) -> {
                  var off = range != null ? (int) range.start() : 0;
                  var end =
                      range == null || range.end() == Long.MAX_VALUE
                          ? bytes.length
                          : (int) range.end() + 1;
                  var len = Math.min(end - off, bytes.length - off);
                  output.write(bytes, off, len);
                })
            .build());
  }

  @ParameterizedTest
  @ValueSource(ints = {500})
  public void someFiles(int numFiles) throws Exception {
    Set<String> keys =
        IntStream.range(0, numFiles)
            .mapToObj(i -> format("path/%d/%d", i % 100, i))
            .collect(Collectors.toCollection(ConcurrentHashMap::newKeySet));

    var mockObject = MockObject.builder().build();
    var objectStorageMock =
        ObjectStorageMock.builder()
            .putBuckets(
                bucket(),
                Bucket.builder()
                    .lister(
                        (String prefix, String offset) ->
                            keys.stream()
                                .map(
                                    key ->
                                        new Bucket.ListElement() {
                                          @Override
                                          public String key() {
                                            return key;
                                          }

                                          @Override
                                          public MockObject object() {
                                            return mockObject;
                                          }
                                        }))
                    .object(key -> keys.contains(key) ? mockObject : null)
                    .deleter(keys::remove)
                    .build())
            .build();

    try (var server = objectStorageMock.start();
        var fileIO = initializedFileIO(server)) {

      var prefix = prefix();

      var fileOps = new FileOperationsImpl(fileIO);

      try (Stream<FileSpec> files = fileOps.findFiles(prefix, FileFilter.alwaysTrue())) {
        assertThat(files).hasSize(numFiles);
      }

      int deletes = numFiles / 10;
      assertThat(
              fileOps.purge(
                  IntStream.range(0, deletes)
                      .mapToObj(i -> format(prefix + "path/%d/%d", i % 100, i))
                      .map(BaseFileOperationsImpl::fileSpecFromLocation),
                  PurgeSpec.DEFAULT_INSTANCE))
          .extracting(PurgeStats::purgeFileRequests)
          .isEqualTo((long) deletes);

      try (Stream<FileSpec> files = fileOps.findFiles(prefix, FileFilter.alwaysTrue())) {
        assertThat(files).hasSize(numFiles - deletes);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(
      ints = {50_000
        // 150_000 (disabled for unit test runtime reasons)
        // 50_000_000 would work, too, but takes way too long for a unit test
      })
  public void manyFiles(int numFiles) throws Exception {
    var mockObject = MockObject.builder().build();
    var pathPrefix = "x".repeat(1000) + "/";
    var objectStorageMock =
        ObjectStorageMock.builder()
            .putBuckets(
                bucket(),
                Bucket.builder()
                    .lister(
                        (String prefix, String offset) -> {
                          var off =
                              offset != null
                                  ? Integer.parseInt(offset.substring(pathPrefix.length()))
                                  : 0;
                          return IntStream.range(off, numFiles)
                              .mapToObj(
                                  i ->
                                      new Bucket.ListElement() {
                                        @Override
                                        public String key() {
                                          return pathPrefix + format("%010d", i);
                                        }

                                        @Override
                                        public MockObject object() {
                                          return mockObject;
                                        }
                                      });
                        })
                    .object(key -> mockObject)
                    .build())
            .build();

    try (var server = objectStorageMock.start();
        var fileIO = initializedFileIO(server)) {

      var prefix = prefix();

      var fileOps = new FileOperationsImpl(fileIO);

      try (Stream<FileSpec> files = fileOps.findFiles(prefix, FileFilter.alwaysTrue())) {
        assertThat(files.count()).isEqualTo(numFiles);
      }
    }
  }

  @Test
  public void icebergIntegration() throws Exception {
    var objectStorageMock =
        ObjectStorageMock.builder().putBuckets(bucket(), newHeapStorageBucket().bucket()).build();

    try (var server = objectStorageMock.start();
        var fileIO = initializedFileIO(server)) {
      var properties = icebergProperties(server);

      icebergIntegration(fileIO, properties);
    }
  }

  public FileIO initializedFileIO(ObjectStorageMock.MockServer server) {
    var fileIO = createFileIO();
    fileIO.initialize(icebergProperties(server));
    return fileIO;
  }

  protected abstract FileIO createFileIO();

  protected abstract String bucket();

  protected abstract Map<String, String> icebergProperties(ObjectStorageMock.MockServer server);
}
