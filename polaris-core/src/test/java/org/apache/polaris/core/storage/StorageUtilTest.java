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
package org.apache.polaris.core.storage;

import java.util.Map;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class StorageUtilTest {

  @Test
  public void testEmptyString() {
    Assertions.assertThat(StorageUtil.getBucket("")).isNull();
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3", "s3a", "gcs", "abfs", "wasb", "file"})
  public void testAbsolutePaths(String scheme) {
    Assertions.assertThat(StorageUtil.getBucket(scheme + "://bucket/path/file.txt"))
        .isEqualTo("bucket");
    Assertions.assertThat(StorageUtil.getBucket(scheme + "://bucket:with:colon/path/file.txt"))
        .isEqualTo("bucket:with:colon");
    Assertions.assertThat(StorageUtil.getBucket(scheme + "://bucket_with_underscore/path/file.txt"))
        .isEqualTo("bucket_with_underscore");
    Assertions.assertThat(StorageUtil.getBucket(scheme + "://bucket_with_ユニコード/path/file.txt"))
        .isEqualTo("bucket_with_ユニコード");
  }

  @Test
  public void testRelativePaths() {
    Assertions.assertThat(StorageUtil.getBucket("bucket/path/file.txt")).isNull();
    Assertions.assertThat(StorageUtil.getBucket("path/file.txt")).isNull();
  }

  @Test
  public void testAbsolutePathWithoutScheme() {
    Assertions.assertThat(StorageUtil.getBucket("/bucket/path/file.txt")).isNull();
  }

  @Test
  public void testInvalidURI() {
    Assertions.assertThatThrownBy(
            () -> StorageUtil.getBucket("s3://bucket with space/path/file.txt"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testAuthorityWithPort() {
    Assertions.assertThat(StorageUtil.getBucket("s3://bucket:8080/path/file.txt"))
        .isEqualTo("bucket:8080");
  }

  @Test
  public void getLocationsUsedByTable() {
    Assertions.assertThat(StorageUtil.getLocationsUsedByTable(null, Map.of())).isEmpty();
    Assertions.assertThat(StorageUtil.getLocationsUsedByTable("", Map.of())).isNotEmpty();
    Assertions.assertThat(StorageUtil.getLocationsUsedByTable("/foo/", Map.of())).contains("/foo/");
    Assertions.assertThat(
            StorageUtil.getLocationsUsedByTable(
                "/foo/",
                Map.of(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY, "/foo/")))
        .contains("/foo/");
    Assertions.assertThat(
            StorageUtil.getLocationsUsedByTable(
                "/foo/",
                Map.of(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY, "/bar/")))
        .contains("/foo/", "/bar/");
    Assertions.assertThat(
            StorageUtil.getLocationsUsedByTable(
                "/foo/",
                Map.of(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY, "/foo/bar/")))
        .contains("/foo/");
    Assertions.assertThat(
            StorageUtil.getLocationsUsedByTable(
                "/foo/bar/",
                Map.of(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY, "/foo/")))
        .contains("/foo/");
    Assertions.assertThat(
            StorageUtil.getLocationsUsedByTable(
                "/foo/bar/",
                Map.of(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY, "/foo/")))
        .contains("/foo/");
    Assertions.assertThat(
            StorageUtil.getLocationsUsedByTable(
                "/1/",
                Map.of(
                    IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY, "/2/",
                    IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY, "/3/")))
        .contains("/1/", "/2/", "/3/");
  }

  @Test
  public void getLocationsUsedByTable_trailingSlashOnlyMismatchIsDeduped() {
    // baseLocation and write.data.path are the same path, differing only by a trailing slash.
    // They are distinct raw strings, so they do NOT collapse in the HashSet, but they are
    // mutually isChildOf each other once isChildOf's trailing-slash normalization is applied.
    // removeRedundantLocations must treat that as equivalence and keep exactly one, not drop both.
    Assertions.assertThat(
            StorageUtil.getLocationsUsedByTable(
                "s3://bucket/a/b/c",
                Map.of(
                    IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY,
                    "s3://bucket/a/b/c/")))
        .hasSize(1);
  }

  @Test
  public void getLocationsUsedByTable_schemeOnlyMismatchIsDeduped() {
    // Same container/account/file path, but "abfss" (secure) vs "abfs" (non-secure) schemes.
    // AzureLocation.isChildOf never inspects scheme, so this hits the same bug class as the
    // trailing-slash case above, with no trailing slash involved at all -- confirming the defect
    // is in the general mutual-containment handling in removeRedundantLocations, not specific to
    // trailing slashes.
    Assertions.assertThat(
            StorageUtil.getLocationsUsedByTable(
                "abfss://container@account.dfs.core.windows.net/a/b/c",
                Map.of(
                    IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY,
                    "abfs://container@account.dfs.core.windows.net/a/b/c")))
        .hasSize(1);
  }
}
