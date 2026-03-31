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

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.aws.AwsS3TablesCredentialsStorageIntegration;
import org.apache.polaris.core.storage.aws.AwsS3TablesStorageConfigurationInfo;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

class AwsS3TablesCredentialsStorageIntegrationTest {

  private static final Instant EXPIRE_TIME = Instant.now().plusMillis(3600_000);
  private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/polaris-s3tables-role";
  private static final String TABLE_ARN =
      "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket/table/abc123";
  private static final PolarisPrincipal PRINCIPAL =
      PolarisPrincipal.of("test-principal", Map.of(), Set.of());

  private static final AssumeRoleResponse ASSUME_ROLE_RESPONSE =
      AssumeRoleResponse.builder()
          .credentials(
              Credentials.builder()
                  .accessKeyId("accessKey")
                  .secretAccessKey("secretKey")
                  .sessionToken("sess")
                  .expiration(EXPIRE_TIME)
                  .build())
          .build();

  private static final RealmConfig REALM_CONFIG =
      new RealmConfigImpl((rc, name) -> null, () -> "realm");

  private AwsS3TablesCredentialsStorageIntegration createIntegration(StsClient stsClient) {
    AwsS3TablesStorageConfigurationInfo config =
        AwsS3TablesStorageConfigurationInfo.builder()
            .allowedLocations(Set.of(TABLE_ARN))
            .roleARN(ROLE_ARN)
            .region("us-east-1")
            .build();
    return new AwsS3TablesCredentialsStorageIntegration(
        config, (destination) -> stsClient, Optional.empty());
  }

  @Test
  void testS3TablesReadOnlyPolicy() {
    StsClient stsClient = Mockito.mock(StsClient.class);
    Mockito.when(stsClient.assumeRole(Mockito.any(AssumeRoleRequest.class)))
        .thenReturn(ASSUME_ROLE_RESPONSE);

    AwsS3TablesCredentialsStorageIntegration integration = createIntegration(stsClient);

    integration.getSubscopedCreds(
        REALM_CONFIG,
        false,
        Set.of(TABLE_ARN),
        Set.of(),
        PRINCIPAL,
        Optional.empty(),
        CredentialVendingContext.empty());

    ArgumentCaptor<AssumeRoleRequest> captor = ArgumentCaptor.forClass(AssumeRoleRequest.class);
    Mockito.verify(stsClient).assumeRole(captor.capture());
    String policy = captor.getValue().policy();

    assertThat(policy).contains("s3tables:GetTableData");
    assertThat(policy).contains("s3tables:GetTableMetadataLocation");
    assertThat(policy).doesNotContain("s3tables:UpdateTableMetadataLocation");
    assertThat(policy).doesNotContain("s3tables:PutTableData");
    assertThat(policy).contains(TABLE_ARN);
  }

  @Test
  void testS3TablesReadWritePolicy() {
    StsClient stsClient = Mockito.mock(StsClient.class);
    Mockito.when(stsClient.assumeRole(Mockito.any(AssumeRoleRequest.class)))
        .thenReturn(ASSUME_ROLE_RESPONSE);

    AwsS3TablesCredentialsStorageIntegration integration = createIntegration(stsClient);

    integration.getSubscopedCreds(
        REALM_CONFIG,
        false,
        Set.of(TABLE_ARN),
        Set.of(TABLE_ARN),
        PRINCIPAL,
        Optional.empty(),
        CredentialVendingContext.empty());

    ArgumentCaptor<AssumeRoleRequest> captor = ArgumentCaptor.forClass(AssumeRoleRequest.class);
    Mockito.verify(stsClient).assumeRole(captor.capture());
    String policy = captor.getValue().policy();

    assertThat(policy).contains("s3tables:GetTableData");
    assertThat(policy).contains("s3tables:GetTableMetadataLocation");
    assertThat(policy).contains("s3tables:UpdateTableMetadataLocation");
    assertThat(policy).contains("s3tables:PutTableData");
    assertThat(policy).contains(TABLE_ARN);
  }

  @Test
  void testNoS3ActionsInPolicy() {
    StsClient stsClient = Mockito.mock(StsClient.class);
    Mockito.when(stsClient.assumeRole(Mockito.any(AssumeRoleRequest.class)))
        .thenReturn(ASSUME_ROLE_RESPONSE);

    AwsS3TablesCredentialsStorageIntegration integration = createIntegration(stsClient);

    integration.getSubscopedCreds(
        REALM_CONFIG,
        true,
        Set.of(TABLE_ARN),
        Set.of(TABLE_ARN),
        PRINCIPAL,
        Optional.empty(),
        CredentialVendingContext.empty());

    ArgumentCaptor<AssumeRoleRequest> captor = ArgumentCaptor.forClass(AssumeRoleRequest.class);
    Mockito.verify(stsClient).assumeRole(captor.capture());
    String policy = captor.getValue().policy();

    // Must not contain any s3: actions
    assertThat(policy).doesNotContain("\"s3:");
    assertThat(policy).doesNotContain("s3:GetObject");
    assertThat(policy).doesNotContain("s3:PutObject");
    assertThat(policy).doesNotContain("s3:ListBucket");
    assertThat(policy).doesNotContain("s3:GetBucketLocation");
    assertThat(policy).doesNotContain("kms:");
  }

  @Test
  void testAllowListOperationIgnored() {
    StsClient stsClient = Mockito.mock(StsClient.class);
    Mockito.when(stsClient.assumeRole(Mockito.any(AssumeRoleRequest.class)))
        .thenReturn(ASSUME_ROLE_RESPONSE);

    AwsS3TablesCredentialsStorageIntegration integration = createIntegration(stsClient);

    // Call with allowListOperation=true
    integration.getSubscopedCreds(
        REALM_CONFIG,
        true,
        Set.of(TABLE_ARN),
        Set.of(),
        PRINCIPAL,
        Optional.empty(),
        CredentialVendingContext.empty());

    ArgumentCaptor<AssumeRoleRequest> captorWithList =
        ArgumentCaptor.forClass(AssumeRoleRequest.class);
    Mockito.verify(stsClient).assumeRole(captorWithList.capture());
    String policyWithList = captorWithList.getValue().policy();

    // Reset and call with allowListOperation=false
    Mockito.reset(stsClient);
    Mockito.when(stsClient.assumeRole(Mockito.any(AssumeRoleRequest.class)))
        .thenReturn(ASSUME_ROLE_RESPONSE);

    integration.getSubscopedCreds(
        REALM_CONFIG,
        false,
        Set.of(TABLE_ARN),
        Set.of(),
        PRINCIPAL,
        Optional.empty(),
        CredentialVendingContext.empty());

    ArgumentCaptor<AssumeRoleRequest> captorWithoutList =
        ArgumentCaptor.forClass(AssumeRoleRequest.class);
    Mockito.verify(stsClient).assumeRole(captorWithoutList.capture());
    String policyWithoutList = captorWithoutList.getValue().policy();

    // Policies should be identical regardless of allowListOperation
    assertThat(policyWithList).isEqualTo(policyWithoutList);
  }

  @Test
  void testCredentialsReturned() {
    StsClient stsClient = Mockito.mock(StsClient.class);
    Mockito.when(stsClient.assumeRole(Mockito.any(AssumeRoleRequest.class)))
        .thenReturn(ASSUME_ROLE_RESPONSE);

    AwsS3TablesCredentialsStorageIntegration integration = createIntegration(stsClient);

    StorageAccessConfig result =
        integration.getSubscopedCreds(
            REALM_CONFIG,
            false,
            Set.of(TABLE_ARN),
            Set.of(),
            PRINCIPAL,
            Optional.empty(),
            CredentialVendingContext.empty());

    assertThat(result.get(StorageAccessProperty.AWS_KEY_ID)).isEqualTo("accessKey");
    assertThat(result.get(StorageAccessProperty.AWS_SECRET_KEY)).isEqualTo("secretKey");
    assertThat(result.get(StorageAccessProperty.AWS_TOKEN)).isEqualTo("sess");
    assertThat(result.get(StorageAccessProperty.EXPIRATION_TIME))
        .isEqualTo(String.valueOf(EXPIRE_TIME.toEpochMilli()));
  }

  @Test
  void testAssumeRoleCalledWithCorrectParams() {
    StsClient stsClient = Mockito.mock(StsClient.class);
    Mockito.when(stsClient.assumeRole(Mockito.any(AssumeRoleRequest.class)))
        .thenReturn(ASSUME_ROLE_RESPONSE);

    AwsS3TablesCredentialsStorageIntegration integration = createIntegration(stsClient);

    integration.getSubscopedCreds(
        REALM_CONFIG,
        false,
        Set.of(TABLE_ARN),
        Set.of(),
        PRINCIPAL,
        Optional.empty(),
        CredentialVendingContext.empty());

    ArgumentCaptor<AssumeRoleRequest> captor = ArgumentCaptor.forClass(AssumeRoleRequest.class);
    Mockito.verify(stsClient).assumeRole(captor.capture());

    AssumeRoleRequest request = captor.getValue();
    assertThat(request.roleArn()).isEqualTo(ROLE_ARN);
    assertThat(request.policy()).isNotEmpty();
  }
}
