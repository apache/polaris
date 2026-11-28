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

import java.net.URISyntaxException;
import org.apache.polaris.core.storage.azure.AzureLocation;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

public class StorageLocationTest {

  @Test
  public void testOfDifferentPrefixes() {
    StorageLocation standardLocation = StorageLocation.of("file:///path/to/file");
    StorageLocation slashLeadingLocation = StorageLocation.of("/path/to/file");
    StorageLocation manySlashLeadingLocation = StorageLocation.of("////////path/to/file");
    StorageLocation fileSingleSlashLocation = StorageLocation.of("file:/path/to/file");
    StorageLocation fileTooManySlashesLocation = StorageLocation.of("file://///////path/to/file");
    Assertions.assertThat(slashLeadingLocation.equals(standardLocation)).isTrue();
    Assertions.assertThat(manySlashLeadingLocation.equals(standardLocation)).isTrue();
    Assertions.assertThat(fileSingleSlashLocation.equals(standardLocation)).isTrue();
    Assertions.assertThat(fileTooManySlashesLocation.equals(standardLocation)).isTrue();
    Assertions.assertThat(standardLocation).isExactlyInstanceOf(StorageLocation.class);
    Assertions.assertThat(slashLeadingLocation).isExactlyInstanceOf(StorageLocation.class);
    Assertions.assertThat(manySlashLeadingLocation).isExactlyInstanceOf(StorageLocation.class);
    Assertions.assertThat(fileSingleSlashLocation).isExactlyInstanceOf(StorageLocation.class);
    Assertions.assertThat(fileTooManySlashesLocation).isExactlyInstanceOf(StorageLocation.class);
  }

  @Test
  public void testBlobStorageLocations() {
    StorageLocation azureLocation =
        StorageLocation.of("wasb://container@storageaccount.blob.core.windows.net/myfile");
    Assertions.assertThat(azureLocation instanceof AzureLocation).isTrue();
    azureLocation =
        StorageLocation.of("abfss://container@storageaccount.blob.core.windows.net/myfile");
    Assertions.assertThat(azureLocation instanceof AzureLocation).isTrue();

    String s3LocationStr = "s3://test-bucket/mydirectory";
    StorageLocation s3Location = StorageLocation.of(s3LocationStr);
    Assertions.assertThat(s3Location instanceof AzureLocation).isFalse();

    Assertions.assertThat(s3Location.toString()).isEqualTo(s3LocationStr);
  }

  @Test
  public void testIsSameLocationIgnoresTrailingSlashAndPrefixDifferences() {
    StorageLocation standard = StorageLocation.of("file:///path/to/ns");
    StorageLocation trailingSlash = StorageLocation.of("file:///path/to/ns/");
    StorageLocation singleSlashPrefix = StorageLocation.of("file:/path/to/ns");

    // Equal locations, including trailing-slash and prefix-normalization differences, are the same.
    Assertions.assertThat(standard.isSameLocation(standard)).isTrue();
    Assertions.assertThat(standard.isSameLocation(trailingSlash)).isTrue();
    Assertions.assertThat(trailingSlash.isSameLocation(standard)).isTrue();
    Assertions.assertThat(standard.isSameLocation(singleSlashPrefix)).isTrue();

    // A parent and its strict child are not the same location.
    StorageLocation child = StorageLocation.of("file:///path/to/ns/table");
    Assertions.assertThat(standard.isSameLocation(child)).isFalse();
    Assertions.assertThat(child.isSameLocation(standard)).isFalse();

    // Unrelated locations are not the same.
    StorageLocation sibling = StorageLocation.of("file:///path/to/other");
    Assertions.assertThat(standard.isSameLocation(sibling)).isFalse();

    // The same holds for scheme-based locations such as S3.
    Assertions.assertThat(
            StorageLocation.of("s3://bucket/ns")
                .isSameLocation(StorageLocation.of("s3://bucket/ns/")))
        .isTrue();
    Assertions.assertThat(
            StorageLocation.of("s3://bucket/ns")
                .isSameLocation(StorageLocation.of("s3://bucket/ns/table")))
        .isFalse();
  }

  @Test
  public void testSpecialCharacters() {
    // Blob Storage does not have validations
    String specialCharsBlobStorage = "s3://test-bucket/quote'/equals=/period/../myfile.parquet";
    StorageLocation s3LocationSpecialCharacters = StorageLocation.of(specialCharsBlobStorage);

    Assertions.assertThat(s3LocationSpecialCharacters.toString())
        .isEqualTo(specialCharsBlobStorage);

    // But local filesystems do
    String specialCharsLocalStorage = "file:///var/tmp\"/myfile.parquet";
    Assertions.assertThatThrownBy(
            () -> {
              StorageLocation.of(specialCharsLocalStorage);
            })
        .hasCauseInstanceOf(URISyntaxException.class);
  }

  @ParameterizedTest
  @CsvSource({
    "s3://bucket/a/b/file.avro, s3://bucket/a/b/",
    "s3://bucket/a/b/, s3://bucket/a/",
    "s3://bucket/file.avro, s3://bucket/",
    // the last path segment equals the bucket name
    "s3://b/b, s3://b/",
    "s3a://bucket/a/file.avro, s3a://bucket/a/",
    // S3 keys that are not valid URIs
    "'s3://bucket/my dir/file{1}.avro', 's3://bucket/my dir/'",
    "s3://bucket/a%b/file.avro, s3://bucket/a%b/",
    // empty path segments are kept; only one trailing slash is ignored
    "s3://bucket/a//file.avro, s3://bucket/a//",
    "s3://bucket/a//, s3://bucket/a/",
    "s3://bucket//, s3://bucket/",
    "gs://bucket/a/file.avro, gs://bucket/a/",
    "abfss://container@account.dfs.core.windows.net/a/file.avro, abfss://container@account.dfs.core.windows.net/a/",
    "abfss://container@account.dfs.core.windows.net/file.avro, abfss://container@account.dfs.core.windows.net/",
    "wasbs://container@account.blob.core.windows.net/a/b/, wasbs://container@account.blob.core.windows.net/a/",
    "file:///tmp/a/file.avro, file:///tmp/a/",
    "file:///tmp, file:///",
    "/tmp/a/file.avro, file:///tmp/a/",
  })
  public void testParent(String location, String expectedParent) {
    StorageLocation child = StorageLocation.of(location);
    StorageLocation parent = child.parent().orElseThrow();

    Assertions.assertThat(parent.toString()).isEqualTo(expectedParent);
    Assertions.assertThat(parent).isExactlyInstanceOf(child.getClass());
    Assertions.assertThat(child.isChildOf(parent)).isTrue();
    Assertions.assertThat(parent.isSameLocation(child)).isFalse();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "s3://bucket",
        "s3://bucket/",
        "gs://bucket/",
        "abfss://container@account.dfs.core.windows.net",
        "abfss://container@account.dfs.core.windows.net/",
        "file:///",
        "no-scheme/a/b",
      })
  public void testParentOfRootIsEmpty(String location) {
    Assertions.assertThat(StorageLocation.of(location).parent()).isEmpty();
  }
}
