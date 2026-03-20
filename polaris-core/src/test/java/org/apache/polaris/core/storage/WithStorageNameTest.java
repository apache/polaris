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

import java.util.List;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.apache.polaris.core.storage.gcp.GcpStorageConfigurationInfo;
import org.junit.jupiter.api.Test;

class WithStorageNameTest {

  @Test
  void awsFieldsPreserved() {
    AwsStorageConfigurationInfo base =
        AwsStorageConfigurationInfo.builder()
            .allowedLocations(List.of("s3://bucket/path"))
            .storageName("original")
            .roleARN("arn:aws:iam::123456789012:role/test-role")
            .externalId("ext-id-123")
            .region("us-west-2")
            .endpoint("https://s3.example.com")
            .endpointInternal("https://s3-internal.example.com")
            .stsEndpoint("https://sts.example.com")
            .pathStyleAccess(true)
            .stsUnavailable(true)
            .kmsUnavailable(false)
            .currentKmsKey("arn:aws:kms:us-west-2:123456789012:key/test")
            .allowedKmsKeys(List.of("arn:aws:kms:us-west-2:123456789012:key/test"))
            .build();

    PolarisStorageConfigurationInfo result =
        PolarisStorageConfigurationInfo.withStorageName(base, "new-storage");

    assertThat(result).isInstanceOf(AwsStorageConfigurationInfo.class);
    AwsStorageConfigurationInfo awsResult = (AwsStorageConfigurationInfo) result;
    assertThat(awsResult.getStorageName()).isEqualTo("new-storage");
    assertThat(awsResult.getAllowedLocations()).containsExactly("s3://bucket/path");
    assertThat(awsResult.getRoleARN()).isEqualTo("arn:aws:iam::123456789012:role/test-role");
    assertThat(awsResult.getExternalId()).isEqualTo("ext-id-123");
    assertThat(awsResult.getRegion()).isEqualTo("us-west-2");
    assertThat(awsResult.getEndpoint()).isEqualTo("https://s3.example.com");
    assertThat(awsResult.getEndpointInternal()).isEqualTo("https://s3-internal.example.com");
    assertThat(awsResult.getStsEndpoint()).isEqualTo("https://sts.example.com");
    assertThat(awsResult.getPathStyleAccess()).isTrue();
    assertThat(awsResult.getStsUnavailable()).isTrue();
    assertThat(awsResult.getKmsUnavailable()).isFalse();
    assertThat(awsResult.getCurrentKmsKey())
        .isEqualTo("arn:aws:kms:us-west-2:123456789012:key/test");
    assertThat(awsResult.getAllowedKmsKeys())
        .containsExactly("arn:aws:kms:us-west-2:123456789012:key/test");
  }

  @Test
  void awsNullStorageNameClearsOverride() {
    AwsStorageConfigurationInfo base =
        AwsStorageConfigurationInfo.builder()
            .allowedLocations(List.of("s3://bucket/path"))
            .storageName("original")
            .roleARN("arn:aws:iam::123456789012:role/test-role")
            .build();

    PolarisStorageConfigurationInfo result =
        PolarisStorageConfigurationInfo.withStorageName(base, null);

    assertThat(result.getStorageName()).isNull();
    assertThat(result.getAllowedLocations()).containsExactly("s3://bucket/path");
  }

  @Test
  void azureFieldsPreserved() {
    AzureStorageConfigurationInfo base =
        AzureStorageConfigurationInfo.builder()
            .allowedLocations(List.of("abfss://container@account.dfs.core.windows.net/path"))
            .storageName("original")
            .tenantId("tenant-123")
            .multiTenantAppName("my-app")
            .consentUrl("https://consent.example.com")
            .hierarchical(true)
            .build();

    PolarisStorageConfigurationInfo result =
        PolarisStorageConfigurationInfo.withStorageName(base, "azure-storage");

    assertThat(result).isInstanceOf(AzureStorageConfigurationInfo.class);
    AzureStorageConfigurationInfo azureResult = (AzureStorageConfigurationInfo) result;
    assertThat(azureResult.getStorageName()).isEqualTo("azure-storage");
    assertThat(azureResult.getAllowedLocations())
        .containsExactly("abfss://container@account.dfs.core.windows.net/path");
    assertThat(azureResult.getTenantId()).isEqualTo("tenant-123");
    assertThat(azureResult.getMultiTenantAppName()).isEqualTo("my-app");
    assertThat(azureResult.getConsentUrl()).isEqualTo("https://consent.example.com");
    assertThat(azureResult.isHierarchical()).isTrue();
  }

  @Test
  void gcpFieldsPreserved() {
    GcpStorageConfigurationInfo base =
        GcpStorageConfigurationInfo.builder()
            .allowedLocations(List.of("gs://bucket/path"))
            .storageName("original")
            .gcpServiceAccount("sa@project.iam.gserviceaccount.com")
            .build();

    PolarisStorageConfigurationInfo result =
        PolarisStorageConfigurationInfo.withStorageName(base, "gcp-storage");

    assertThat(result).isInstanceOf(GcpStorageConfigurationInfo.class);
    GcpStorageConfigurationInfo gcpResult = (GcpStorageConfigurationInfo) result;
    assertThat(gcpResult.getStorageName()).isEqualTo("gcp-storage");
    assertThat(gcpResult.getAllowedLocations()).containsExactly("gs://bucket/path");
    assertThat(gcpResult.getGcpServiceAccount()).isEqualTo("sa@project.iam.gserviceaccount.com");
  }

  @Test
  void fileFieldsPreserved() {
    FileStorageConfigurationInfo base =
        FileStorageConfigurationInfo.builder()
            .allowedLocations(List.of("file:///tmp/warehouse"))
            .storageName("original")
            .build();

    PolarisStorageConfigurationInfo result =
        PolarisStorageConfigurationInfo.withStorageName(base, "file-storage");

    assertThat(result).isInstanceOf(FileStorageConfigurationInfo.class);
    assertThat(result.getStorageName()).isEqualTo("file-storage");
    assertThat(result.getAllowedLocations()).containsExactly("file:///tmp/warehouse");
  }

  @Test
  void serializationRoundTrip() {
    AwsStorageConfigurationInfo base =
        AwsStorageConfigurationInfo.builder()
            .allowedLocations(List.of("s3://bucket/path"))
            .storageName("original")
            .roleARN("arn:aws:iam::123456789012:role/test-role")
            .region("us-west-2")
            .build();

    PolarisStorageConfigurationInfo overridden =
        PolarisStorageConfigurationInfo.withStorageName(base, "new-name");
    String serialized = overridden.serialize();
    PolarisStorageConfigurationInfo deserialized =
        PolarisStorageConfigurationInfo.deserialize(serialized);

    assertThat(deserialized.getStorageName()).isEqualTo("new-name");
    assertThat(deserialized).isInstanceOf(AwsStorageConfigurationInfo.class);
    AwsStorageConfigurationInfo awsDeserialized = (AwsStorageConfigurationInfo) deserialized;
    assertThat(awsDeserialized.getRoleARN()).isEqualTo("arn:aws:iam::123456789012:role/test-role");
    assertThat(awsDeserialized.getRegion()).isEqualTo("us-west-2");
  }
}
