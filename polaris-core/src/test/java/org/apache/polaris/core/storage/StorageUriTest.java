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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class StorageUriTest {

  @ParameterizedTest
  @MethodSource("validStorageLocations")
  void testParsePreservesRawPath(
      String location, String expectedAuthority, String expectedRawPath) {
    StorageUri uri = StorageUri.parse(location);

    assertThat(uri.authority()).isEqualTo(expectedAuthority);
    assertThat(uri.rawPath()).isEqualTo(expectedRawPath);
  }

  private static Stream<Arguments> validStorageLocations() {
    return Stream.of(
        Arguments.of(
            "s3://bucket/path/with?question#and-fragment",
            "bucket",
            "/path/with?question#and-fragment"),
        Arguments.of("s3://bucket/path/ns*%3F$/tb$%3F*", "bucket", "/path/ns*%3F$/tb$%3F*"),
        Arguments.of("gs://bucket", "bucket", ""),
        Arguments.of("abfs://container", "container", ""));
  }

  @ParameterizedTest
  @MethodSource("invalidStorageLocations")
  void testParseRejects(String location) {
    assertThatThrownBy(() -> StorageUri.parse(location))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Invalid storage location uri");
  }

  private static Stream<String> invalidStorageLocations() {
    return Stream.of(
        "s3://buck et/path",
        "s3://buck\tet/path",
        "s3://buck\net/path",
        "s3://buck\ret/path",
        "s3://buck\fet/path",
        "s3://buck\u00A0et/path",
        "s3://buck\u2003et/path",
        "s3://buck\u2028et/path",
        "s3://buck\u3000et/path",
        "s3://bucket /path",
        null,
        "://",
        "/foo/bar");
  }
}
