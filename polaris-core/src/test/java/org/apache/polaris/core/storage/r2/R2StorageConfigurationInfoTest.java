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
package org.apache.polaris.core.storage.r2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.util.List;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class R2StorageConfigurationInfoTest {

  private static final String ACCOUNT = "0123456789abcdef0123456789abcdef";

  @Test
  void derivesDefaultEndpointHostFromAccountId() {
    R2StorageConfigurationInfo info =
        R2StorageConfigurationInfo.builder()
            .accountId(ACCOUNT)
            .addAllowedLocations("s3://bucket/prefix/")
            .build();
    assertThat(info.getStorageType()).isEqualTo(PolarisStorageConfigurationInfo.StorageType.R2);
    assertThat(info.getEndpointHost()).isEqualTo(ACCOUNT + ".r2.cloudflarestorage.com");
    assertThat(info.getEndpointUri())
        .isEqualTo(URI.create("https://" + ACCOUNT + ".r2.cloudflarestorage.com"));
    assertThat(info.getFileIoImplClassName()).isEqualTo("org.apache.iceberg.aws.s3.S3FileIO");
    assertThat(info.getJurisdiction()).isNull();
  }

  static List<String> knownJurisdictions() {
    return R2StorageConfigurationInfo.KNOWN_JURISDICTIONS;
  }

  @ParameterizedTest
  @MethodSource("knownJurisdictions")
  void derivesJurisdictionEndpointHost(String jurisdiction) {
    R2StorageConfigurationInfo info =
        R2StorageConfigurationInfo.builder()
            .accountId(ACCOUNT)
            .jurisdiction(jurisdiction)
            .addAllowedLocations("s3a://bucket/")
            .build();
    assertThat(info.getEndpointHost())
        .isEqualTo(ACCOUNT + "." + jurisdiction + ".r2.cloudflarestorage.com");
    assertThat(info.getEndpointUri())
        .isEqualTo(
            URI.create("https://" + ACCOUNT + "." + jurisdiction + ".r2.cloudflarestorage.com"));
  }

  @Test
  void rejectsMissingAccountId() {
    assertThatThrownBy(
            () -> R2StorageConfigurationInfo.builder().addAllowedLocations("s3://b/").build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("accountId");
  }

  @Test
  void rejectsMalformedAccountId() {
    assertThatThrownBy(
            () ->
                R2StorageConfigurationInfo.builder()
                    .accountId("not-hex")
                    .addAllowedLocations("s3://b/")
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("accountId");
  }

  @Test
  void rejectsUnknownJurisdiction() {
    assertThatThrownBy(
            () ->
                R2StorageConfigurationInfo.builder()
                    .accountId(ACCOUNT)
                    .jurisdiction("mars")
                    .addAllowedLocations("s3://b/")
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("jurisdiction")
        .hasMessageContaining("eu");
  }

  @Test
  void rejectsNonS3LocationPrefix() {
    assertThatThrownBy(
            () ->
                R2StorageConfigurationInfo.builder()
                    .accountId(ACCOUNT)
                    .addAllowedLocations("gs://bucket/")
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Location prefix not allowed");
  }

  @Test
  void serializesWithoutDerivedFields() {
    R2StorageConfigurationInfo info =
        R2StorageConfigurationInfo.builder()
            .accountId(ACCOUNT)
            .addAllowedLocations("s3://bucket/prefix/")
            .build();
    String json = info.serialize();
    assertThat(json).contains("\"@type\":\"R2StorageConfigurationInfo\"");
    assertThat(json).contains("\"accountId\":\"" + ACCOUNT + "\"");
    assertThat(json).doesNotContain("endpointHost").doesNotContain("endpointUri");
    assertThat(PolarisStorageConfigurationInfo.deserialize(json)).isEqualTo(info);
  }
}
