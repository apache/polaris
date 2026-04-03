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
package org.apache.polaris.service.storage.aws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.AwsS3TablesStorageConfigurationInfo;
import org.junit.jupiter.api.Test;

class AwsS3TablesStorageConfigurationInfoTest {

  private static final String TABLE_BUCKET_ARN =
      "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket";
  private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/polaris-s3tables-role";

  @Test
  void testSerializationRoundTrip() {
    AwsS3TablesStorageConfigurationInfo original =
        AwsS3TablesStorageConfigurationInfo.builder()
            .allowedLocations(Set.of(TABLE_BUCKET_ARN))
            .roleARN(ROLE_ARN)
            .region("us-east-1")
            .externalId("ext-123")
            .build();

    String json = original.serialize();
    PolarisStorageConfigurationInfo deserialized =
        PolarisStorageConfigurationInfo.deserialize(json);

    assertThat(deserialized).isInstanceOf(AwsS3TablesStorageConfigurationInfo.class);
    AwsS3TablesStorageConfigurationInfo result = (AwsS3TablesStorageConfigurationInfo) deserialized;

    assertThat(result.getStorageType())
        .isEqualTo(PolarisStorageConfigurationInfo.StorageType.S3_TABLES);
    assertThat(result.getRoleARN()).isEqualTo(ROLE_ARN);
    assertThat(result.getRegion()).isEqualTo("us-east-1");
    assertThat(result.getExternalId()).isEqualTo("ext-123");
    assertThat(result.getAllowedLocations()).containsExactly(TABLE_BUCKET_ARN);
    assertThat(result.getFileIoImplClassName()).isEqualTo("org.apache.iceberg.aws.s3.S3FileIO");
  }

  @Test
  void testArnPrefixedLocationsAccepted() {
    AwsS3TablesStorageConfigurationInfo config =
        AwsS3TablesStorageConfigurationInfo.builder()
            .allowedLocations(Set.of(TABLE_BUCKET_ARN))
            .roleARN(ROLE_ARN)
            .build();

    assertThat(config.getStorageType())
        .isEqualTo(PolarisStorageConfigurationInfo.StorageType.S3_TABLES);
  }

  @Test
  void testNonArnLocationRejected() {
    assertThatThrownBy(
            () ->
                AwsS3TablesStorageConfigurationInfo.builder()
                    .allowedLocations(Set.of("s3://my-bucket/prefix"))
                    .roleARN(ROLE_ARN)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Location prefix not allowed");
  }

  @Test
  void testNullFieldsAccepted() {
    AwsS3TablesStorageConfigurationInfo config =
        AwsS3TablesStorageConfigurationInfo.builder()
            .allowedLocations(Set.of(TABLE_BUCKET_ARN))
            .build();

    assertThat(config.getRoleARN()).isNull();
    assertThat(config.getRegion()).isNull();
    assertThat(config.getExternalId()).isNull();
  }
}
