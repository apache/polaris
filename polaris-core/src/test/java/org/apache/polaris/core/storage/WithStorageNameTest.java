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
    String allowedLocation = "s3://bucket/path";
    String roleArn = "arn:aws:iam::123456789012:role/test-role";
    String externalId = "ext-id-123";
    String region = "us-west-2";
    String endpoint = "https://s3.example.com";
    String endpointInternal = "https://s3-internal.example.com";
    String stsEndpoint = "https://sts.example.com";
    String kmsKey = "arn:aws:kms:us-west-2:123456789012:key/test";

    AwsStorageConfigurationInfo base =
        AwsStorageConfigurationInfo.builder()
            .allowedLocations(List.of(allowedLocation))
            .storageName("original")
            .roleARN(roleArn)
            .externalId(externalId)
            .region(region)
            .endpoint(endpoint)
            .endpointInternal(endpointInternal)
            .stsEndpoint(stsEndpoint)
            .pathStyleAccess(true)
            .stsUnavailable(true)
            .kmsUnavailable(false)
            .currentKmsKey(kmsKey)
            .allowedKmsKeys(List.of(kmsKey))
            .build();

    PolarisStorageConfigurationInfo result =
        PolarisStorageConfigurationInfo.withStorageName(base, "new-storage");

    assertThat(result).isInstanceOf(AwsStorageConfigurationInfo.class);
    AwsStorageConfigurationInfo awsResult = (AwsStorageConfigurationInfo) result;
    assertThat(awsResult.getStorageName()).isEqualTo("new-storage");
    assertThat(awsResult.getAllowedLocations()).containsExactly(allowedLocation);
    assertThat(awsResult.getRoleARN()).isEqualTo(roleArn);
    assertThat(awsResult.getExternalId()).isEqualTo(externalId);
    assertThat(awsResult.getRegion()).isEqualTo(region);
    assertThat(awsResult.getEndpoint()).isEqualTo(endpoint);
    assertThat(awsResult.getEndpointInternal()).isEqualTo(endpointInternal);
    assertThat(awsResult.getStsEndpoint()).isEqualTo(stsEndpoint);
    assertThat(awsResult.getPathStyleAccess()).isTrue();
    assertThat(awsResult.getStsUnavailable()).isTrue();
    assertThat(awsResult.getKmsUnavailable()).isFalse();
    assertThat(awsResult.getCurrentKmsKey()).isEqualTo(kmsKey);
    assertThat(awsResult.getAllowedKmsKeys()).containsExactly(kmsKey);
  }

  @Test
  void awsNullStorageNameClearsOverride() {
    String allowedLocation = "s3://bucket/path";
    String roleArn = "arn:aws:iam::123456789012:role/test-role";

    AwsStorageConfigurationInfo base =
        AwsStorageConfigurationInfo.builder()
            .allowedLocations(List.of(allowedLocation))
            .storageName("original")
            .roleARN(roleArn)
            .build();

    PolarisStorageConfigurationInfo result =
        PolarisStorageConfigurationInfo.withStorageName(base, null);

    assertThat(result.getStorageName()).isNull();
    assertThat(result.getAllowedLocations()).containsExactly(allowedLocation);
  }

  @Test
  void azureFieldsPreserved() {
    String allowedLocation = "abfss://container@account.dfs.core.windows.net/path";
    String tenantId = "tenant-123";
    String multiTenantAppName = "my-app";
    String consentUrl = "https://consent.example.com";
    boolean hierarchical = true;

    AzureStorageConfigurationInfo base =
        AzureStorageConfigurationInfo.builder()
            .allowedLocations(List.of(allowedLocation))
            .storageName("original")
            .tenantId(tenantId)
            .multiTenantAppName(multiTenantAppName)
            .consentUrl(consentUrl)
            .hierarchical(hierarchical)
            .build();

    PolarisStorageConfigurationInfo result =
        PolarisStorageConfigurationInfo.withStorageName(base, "azure-storage");

    assertThat(result).isInstanceOf(AzureStorageConfigurationInfo.class);
    AzureStorageConfigurationInfo azureResult = (AzureStorageConfigurationInfo) result;
    assertThat(azureResult.getStorageName()).isEqualTo("azure-storage");
    assertThat(azureResult.getAllowedLocations()).containsExactly(allowedLocation);
    assertThat(azureResult.getTenantId()).isEqualTo(tenantId);
    assertThat(azureResult.getMultiTenantAppName()).isEqualTo(multiTenantAppName);
    assertThat(azureResult.getConsentUrl()).isEqualTo(consentUrl);
    assertThat(azureResult.isHierarchical()).isEqualTo(hierarchical);
  }

  @Test
  void gcpFieldsPreserved() {
    String allowedLocation = "gs://bucket/path";
    String gcpServiceAccount = "sa@project.iam.gserviceaccount.com";

    GcpStorageConfigurationInfo base =
        GcpStorageConfigurationInfo.builder()
            .allowedLocations(List.of(allowedLocation))
            .storageName("original")
            .gcpServiceAccount(gcpServiceAccount)
            .build();

    PolarisStorageConfigurationInfo result =
        PolarisStorageConfigurationInfo.withStorageName(base, "gcp-storage");

    assertThat(result).isInstanceOf(GcpStorageConfigurationInfo.class);
    GcpStorageConfigurationInfo gcpResult = (GcpStorageConfigurationInfo) result;
    assertThat(gcpResult.getStorageName()).isEqualTo("gcp-storage");
    assertThat(gcpResult.getAllowedLocations()).containsExactly(allowedLocation);
    assertThat(gcpResult.getGcpServiceAccount()).isEqualTo(gcpServiceAccount);
  }

  @Test
  void fileFieldsPreserved() {
    String allowedLocation = "file:///tmp/warehouse";

    FileStorageConfigurationInfo base =
        FileStorageConfigurationInfo.builder()
            .allowedLocations(List.of(allowedLocation))
            .storageName("original")
            .build();

    PolarisStorageConfigurationInfo result =
        PolarisStorageConfigurationInfo.withStorageName(base, "file-storage");

    assertThat(result).isInstanceOf(FileStorageConfigurationInfo.class);
    assertThat(result.getStorageName()).isEqualTo("file-storage");
    assertThat(result.getAllowedLocations()).containsExactly(allowedLocation);
  }
}
