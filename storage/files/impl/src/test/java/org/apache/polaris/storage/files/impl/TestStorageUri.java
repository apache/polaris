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

import java.net.URI;
import java.net.URISyntaxException;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(SoftAssertionsExtension.class)
class TestStorageUri {

  @InjectSoftAssertions protected SoftAssertions soft;

  private static URI normalizedURI(String value) {
    // URI remembers the original string values and only makes normalized components available via
    // getters
    var u = URI.create(value).normalize();
    try {
      if (u.isOpaque()) {
        return new URI(u.getScheme(), u.getSchemeSpecificPart(), u.getFragment());
      } else {
        return new URI(
            u.getScheme(),
            u.getUserInfo(),
            u.getHost(),
            u.getPort(),
            u.getPath(),
            u.getQuery(),
            u.getFragment());
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testPathWithoutLeadingTrailingSlash() {
    soft.assertThat(StorageUri.of("s3://foo/bar/baz"))
        .extracting(StorageUri::path, StorageUri::pathWithoutLeadingTrailingSlash)
        .containsExactly("/bar/baz", "bar/baz");
    soft.assertThat(StorageUri.of("s3://foo/bar/baz/"))
        .extracting(StorageUri::path, StorageUri::pathWithoutLeadingTrailingSlash)
        .containsExactly("/bar/baz/", "bar/baz");
    soft.assertThat(StorageUri.of("s3://foo/"))
        .extracting(StorageUri::path, StorageUri::pathWithoutLeadingTrailingSlash)
        .containsExactly("/", "");
  }

  @Test
  void testScheme() {
    soft.assertThat(StorageUri.of("/path/to/file").scheme()).isNull();
    soft.assertThat(StorageUri.of("relative/to/file").scheme()).isNull();
    soft.assertThat(StorageUri.of("file:/path/to/file").scheme()).isEqualTo("file");
    soft.assertThat(StorageUri.of(URI.create("file:/path/to/file")).scheme()).isEqualTo("file");
  }

  @ParameterizedTest
  @CsvSource({
    "/absolute/path,/absolute/path",
    "/absolute/path,/absolute/path/",
    "//extra/leading,//extra/leading",
    "//extra/leading,//extra/leading/",
    "relative/path,relative/path",
    "relative/path,relative/path/",
    "file:/path,file:///path",
    "file:///path,file:///path/",
    "file://////path,file:///path",
    "file:/path,/path",
    "file:/path,path",
    "s3://b/,s3://b",
    "s3://b/,s3://b/",
    "s3://b/path,s3://b/path",
    "s3://b/path,s3://b/path/",
    "s3://b/path,s3://b/path//",
    "s3://b/path,/path",
  })
  void testCompare(String v1, String v2) {
    var s1 = StorageUri.of(v1);
    var s2 = StorageUri.of(v2);
    var u1 = normalizedURI(v1);
    var u2 = normalizedURI(v2);
    soft.assertThat(s1.equals(s2)).isEqualTo(u1.equals(u2));
    soft.assertThat(s2.equals(s1)).isEqualTo(u2.equals(u1));
    soft.assertThat(Integer.signum(s2.compareTo(s1))).isEqualTo(Integer.signum(u2.compareTo(u1)));
    soft.assertThat(Integer.signum(s1.compareTo(s2))).isEqualTo(Integer.signum(u1.compareTo(u2)));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "/path/file",
        "relative/file",
        "file:/",
        "file:/path",
        "file:/path/file",
        "file:/path/file//",
        "file:///path/file/",
        "file:///path/file",
        "file://///path/file",
        "file:///path/file//",
        "file:///path////file//",
        "file:///C:/path/",
        "file:///C:/path",
        "file:///C:/path/file",
        "file:///C:/path/file/",
        "file:/C:/path/file/",
        "s3:/path",
        "s3://bucket",
        "s3://bucket/",
      })
  void testLocation(String input) {
    var s = StorageUri.of(input);
    var u = normalizedURI(input);
    soft.assertThat(s.location()).isEqualTo(u.toString());
    soft.assertThat(s.toString()).isEqualTo(u.toString());
  }

  @Test
  void testUriComponents() {
    var s = StorageUri.of("scheme://user@host:123/path");
    soft.assertThat(s.scheme()).isEqualTo("scheme");
    soft.assertThat(s.authority()).isEqualTo("user@host:123");
    soft.assertThat(s.path()).isEqualTo("/path");

    s = StorageUri.of("scheme:opaque/file");
    soft.assertThat(s.scheme()).isEqualTo("scheme");
    soft.assertThat(s.authority()).isNull();
    // RFC3986 calls this "path", but java.lang.URI will say path is null in this case
    soft.assertThat(s.path()).isEqualTo("opaque/file");

    s = StorageUri.of("file:///path/file");
    soft.assertThat(s.scheme()).isEqualTo("file");
    soft.assertThat(s.authority()).isNull();
    soft.assertThat(s.path()).isEqualTo("/path/file");
    s = StorageUri.of("file:/path/file");
    soft.assertThat(s.scheme()).isEqualTo("file");
    soft.assertThat(s.authority()).isNull();
    soft.assertThat(s.path()).isEqualTo("/path/file");
  }

  @ParameterizedTest
  @CsvSource({
    "s3://bucket/\"file,s3://bucket/\"file",
    "s3://bucket/\000file,s3://bucket/\000file",
  })
  void testLocationSpecialChars(String input, String expected) {
    soft.assertThat(StorageUri.of(input).location()).isEqualTo(expected);
    soft.assertThat(StorageUri.of(input).toString()).isEqualTo(expected);
  }

  @ParameterizedTest
  @CsvSource({
    "/base,/base/file",
    "/base/,/base/file",
    "/base/,/base//file",
    "/base,/base////file",
    "/ba,/base////file",
    "file:///base/,file:/base/file",
    "file:///base,file:/base/file",
    "file:/ba,file:/base/file",
    "file:///C:/base,file:/C:/base/file",
    "file:opaque/a,path",
    "file:opaque/a,file:path",
    "file:opaque,file:/path",
    "file:opaque/a,file:/path",
    "s3://b/path,s3://b/path/file",
    "s3://b/path/,s3://b/path/file",
    "s3://b/path/,s3://b///path/file",
    "s3://b/,s3://b/path/file",
    "s3://b,s3://b/path/file",
    "s3://b,/file/a/",
    "s3://b/foo,/foo/a/",
    "s3://b/foo,/path/a/",
    "s3://b/base/,s3://c/base/file",
    "s3://b/base/,file://b/base/file",
    "s3://b/base/,file://c/base/file",
  })
  void testRelativize(String base, String other) {
    String expected = normalizedURI(base).relativize(normalizedURI(other)).toString();
    soft.assertThat(StorageUri.of(base).relativize(StorageUri.of(other)).location())
        .isEqualTo(expected);
    soft.assertThat(StorageUri.of(base).relativize(other).location()).isEqualTo(expected);
  }

  @ParameterizedTest
  @CsvSource({
    "/base,file",
    "/base/,file",
    "/base/a,file",
    "/base,/absolute/a",
    "file:/base,file",
    "file:///base,file",
    "file:/base,/file",
    "file:///C:/base,/file",
    "file:///C:/base/,/file",
    "file:/C:/base,/file",
    "file:/C:/base,file",
    "file:/C:/base/,file",
    "file:/C:/,file",
    "file:/C:,file",
    "file:opaque,path",
    "file:opaque/a,path",
    "file:opaque,file:path",
    "file:opaque/,file:path",
    "file:opaque/a,file:path",
    "file:opaque,file:/path",
    "file:opaque/a,file:/path",
    "s3://b/base,file",
    "s3://b/base/,file",
    "s3://b/base/a,file",
    "s3://b/base/a/,file",
    "s3://b/base/a,/absolute",
    "s3://b/base/a/,/absolute",
    "s3://b/base/a/,////absolute",
    "s3://b/,file",
    "s3://b,file/a",
    "s3://b,file/a/",
    "s3://b/,////absolute",
    "s3://b////,////absolute",
    "s3://b/base/,s3://b/file",
  })
  void testResolve(String base, String rel) {
    var expected = normalizedURI(base).resolve(normalizedURI(rel)).toString();
    soft.assertThat(StorageUri.of(base).resolve(StorageUri.of(rel)).location()).isEqualTo(expected);
    soft.assertThat(StorageUri.of(base).resolve(rel).location()).isEqualTo(expected);
  }

  @Test
  void testResolveInvalid() {
    var base = StorageUri.of("/base");
    soft.assertThatThrownBy(() -> base.resolve("../parent"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Parent and self-references are not supported: ../parent");
    soft.assertThatThrownBy(() -> base.resolve("./self"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Parent and self-references are not supported: ./self");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "file",
        "file/",
        "/file",
        "/file/",
        "file:///",
        "file:///path",
        "file:///path/a",
        "file:///path/a/",
        "s3://b/path",
      })
  void testWithTrailingSeparator(String input) {
    var uri = StorageUri.of(input).withTrailingSeparator();
    soft.assertThat(uri.path()).endsWith("/");
    soft.assertThat(uri.path()).doesNotEndWith("//");
    soft.assertThat(uri.location()).endsWith("/");
    soft.assertThat(uri.location()).doesNotEndWith("//");
  }
}
