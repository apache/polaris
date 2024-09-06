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
  @ValueSource(strings = {"s3", "gcs", "abfs", "file"})
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
}
