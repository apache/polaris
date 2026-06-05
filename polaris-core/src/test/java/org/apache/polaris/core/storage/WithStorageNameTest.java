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

import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.apache.polaris.core.storage.gcp.GcpStorageConfigurationInfo;
import org.junit.jupiter.api.Test;

class WithStorageNameTest {

  @Test
  void awsConfigPreservesAllFieldsAndSwapsStorageName() {
    AwsStorageConfigurationInfo base =
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocations("s3://foo/bar", "s3://no/where")
            .roleARN("arn:aws:iam::123456789012:role/polaris-test")
            .region("no-where-1")
            .externalId("external-id")
            .storageName("default")
            .build();

    PolarisStorageConfigurationInfo result =
        PolarisStorageConfigurationInfo.withStorageName(base, "team-a");

    assertThat(result).isInstanceOf(AwsStorageConfigurationInfo.class);
    AwsStorageConfigurationInfo aws = (AwsStorageConfigurationInfo) result;
    assertThat(aws.getStorageName()).isEqualTo("team-a");
    assertThat(aws.getRoleARN()).isEqualTo(base.getRoleARN());
    assertThat(aws.getRegion()).isEqualTo(base.getRegion());
    assertThat(aws.getExternalId()).isEqualTo(base.getExternalId());
    assertThat(aws.getAllowedLocations()).containsExactlyElementsOf(base.getAllowedLocations());
    assertThat(aws).isNotEqualTo(base);
    assertThat(base.getStorageName()).isEqualTo("default"); // base unchanged
  }

  @Test
  void awsConfigAcceptsNullClearingStorageName() {
    AwsStorageConfigurationInfo base =
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocations("s3://foo/bar")
            .roleARN("arn:aws:iam::123456789012:role/polaris-test")
            .region("us-east-1")
            .storageName("default")
            .build();

    AwsStorageConfigurationInfo result =
        (AwsStorageConfigurationInfo) PolarisStorageConfigurationInfo.withStorageName(base, null);

    assertThat(result.getStorageName()).isNull();
    assertThat(result.getRoleARN()).isEqualTo(base.getRoleARN());
  }

  @Test
  void azureConfigPreservesFieldsAndSwapsStorageName() {
    AzureStorageConfigurationInfo base =
        AzureStorageConfigurationInfo.builder()
            .addAllowedLocations("abfs://foo@bar.baz/", "abfss://boo@meep.buzz/")
            .tenantId("tenant-id")
            .storageName("default")
            .build();

    AzureStorageConfigurationInfo result =
        (AzureStorageConfigurationInfo)
            PolarisStorageConfigurationInfo.withStorageName(base, "team-azure");

    assertThat(result.getStorageName()).isEqualTo("team-azure");
    assertThat(result.getTenantId()).isEqualTo("tenant-id");
    assertThat(result.getAllowedLocations()).containsExactlyElementsOf(base.getAllowedLocations());
  }

  @Test
  void gcpConfigPreservesFieldsAndSwapsStorageName() {
    GcpStorageConfigurationInfo base =
        GcpStorageConfigurationInfo.builder()
            .addAllowedLocations("gs://foo/bar")
            .gcpServiceAccount("svc@project.iam.gserviceaccount.com")
            .storageName("default")
            .build();

    GcpStorageConfigurationInfo result =
        (GcpStorageConfigurationInfo)
            PolarisStorageConfigurationInfo.withStorageName(base, "team-gcp");

    assertThat(result.getStorageName()).isEqualTo("team-gcp");
    assertThat(result.getGcpServiceAccount()).isEqualTo(base.getGcpServiceAccount());
    assertThat(result.getAllowedLocations()).containsExactlyElementsOf(base.getAllowedLocations());
  }

  @Test
  void fileConfigPreservesFieldsAndSwapsStorageName() {
    FileStorageConfigurationInfo base =
        FileStorageConfigurationInfo.builder()
            .addAllowedLocations("file:///tmp/bar")
            .storageName("default")
            .build();

    FileStorageConfigurationInfo result =
        (FileStorageConfigurationInfo)
            PolarisStorageConfigurationInfo.withStorageName(base, "team-file");

    assertThat(result.getStorageName()).isEqualTo("team-file");
    assertThat(result.getAllowedLocations()).containsExactlyElementsOf(base.getAllowedLocations());
  }

  @Test
  void unsupportedSubtypeThrows() {
    PolarisStorageConfigurationInfo bogus =
        new PolarisStorageConfigurationInfo() {
          @Override
          public java.util.List<String> getAllowedLocations() {
            return java.util.List.of();
          }

          @Override
          public String getStorageName() {
            return null;
          }

          @Override
          public StorageType getStorageType() {
            return StorageType.S3;
          }

          @Override
          public String getFileIoImplClassName() {
            return "x";
          }
        };

    assertThatThrownBy(() -> PolarisStorageConfigurationInfo.withStorageName(bogus, "any"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported");
  }
}
