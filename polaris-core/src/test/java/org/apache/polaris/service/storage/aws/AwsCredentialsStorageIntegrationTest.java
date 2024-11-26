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

import java.util.EnumMap;
import java.util.List;
import java.util.Set;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.storage.PolarisCredentialProperty;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.AwsCredentialsStorageIntegration;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import software.amazon.awssdk.policybuilder.iam.IamAction;
import software.amazon.awssdk.policybuilder.iam.IamCondition;
import software.amazon.awssdk.policybuilder.iam.IamConditionOperator;
import software.amazon.awssdk.policybuilder.iam.IamEffect;
import software.amazon.awssdk.policybuilder.iam.IamPolicy;
import software.amazon.awssdk.policybuilder.iam.IamResource;
import software.amazon.awssdk.policybuilder.iam.IamStatement;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

class AwsCredentialsStorageIntegrationTest {

  public static final AssumeRoleResponse ASSUME_ROLE_RESPONSE =
      AssumeRoleResponse.builder()
          .credentials(
              Credentials.builder()
                  .accessKeyId("accessKey")
                  .secretAccessKey("secretKey")
                  .sessionToken("sess")
                  .build())
          .build();
  public static final String AWS_PARTITION = "aws";

  @Test
  public void testGetSubscopedCreds() {
    StsClient stsClient = Mockito.mock(StsClient.class);
    String roleARN = "arn:aws:iam::012345678901:role/jdoe";
    String externalId = "externalId";
    Mockito.when(stsClient.assumeRole(Mockito.isA(AssumeRoleRequest.class)))
        .thenAnswer(
            invocation -> {
              assertThat(invocation.getArguments()[0])
                  .isInstanceOf(AssumeRoleRequest.class)
                  .asInstanceOf(InstanceOfAssertFactories.type(AssumeRoleRequest.class))
                  .returns(externalId, AssumeRoleRequest::externalId)
                  .returns(roleARN, AssumeRoleRequest::roleArn);
              return ASSUME_ROLE_RESPONSE;
            });
    String warehouseDir = "s3://bucket/path/to/warehouse";
    EnumMap<PolarisCredentialProperty, String> credentials =
        new AwsCredentialsStorageIntegration(stsClient)
            .getSubscopedCreds(
                Mockito.mock(PolarisDiagnostics.class),
                new AwsStorageConfigurationInfo(
                    PolarisStorageConfigurationInfo.StorageType.S3,
                    List.of(warehouseDir),
                    roleARN,
                    externalId,
                    null),
                true,
                Set.of(warehouseDir + "/namespace/table"),
                Set.of(warehouseDir + "/namespace/table"));
    assertThat(credentials)
        .isNotEmpty()
        .containsEntry(PolarisCredentialProperty.AWS_TOKEN, "sess")
        .containsEntry(PolarisCredentialProperty.AWS_KEY_ID, "accessKey")
        .containsEntry(PolarisCredentialProperty.AWS_SECRET_KEY, "secretKey");
  }

  @ParameterizedTest
  @ValueSource(strings = {AWS_PARTITION, "aws-cn", "aws-us-gov"})
  public void testGetSubscopedCredsInlinePolicy(String awsPartition) {
    PolarisStorageConfigurationInfo.StorageType storageType =
        PolarisStorageConfigurationInfo.StorageType.S3;
    String roleARN;
    switch (awsPartition) {
      case AWS_PARTITION:
        roleARN = "arn:aws:iam::012345678901:role/jdoe";
        break;
      case "aws-cn":
        roleARN = "arn:aws-cn:iam::012345678901:role/jdoe";
        break;
      case "aws-us-gov":
        roleARN = "arn:aws-us-gov:iam::012345678901:role/jdoe";
        break;
      default:
        throw new IllegalArgumentException("Unknown aws partition: " + awsPartition);
    }
    ;
    StsClient stsClient = Mockito.mock(StsClient.class);
    String externalId = "externalId";
    String bucket = "bucket";
    String warehouseKeyPrefix = "path/to/warehouse";
    String firstPath = warehouseKeyPrefix + "/namespace/table";
    String secondPath = warehouseKeyPrefix + "/oldnamespace/table";
    Mockito.when(stsClient.assumeRole(Mockito.isA(AssumeRoleRequest.class)))
        .thenAnswer(
            invocation -> {
              assertThat(invocation.getArguments()[0])
                  .isInstanceOf(AssumeRoleRequest.class)
                  .asInstanceOf(InstanceOfAssertFactories.type(AssumeRoleRequest.class))
                  .extracting(AssumeRoleRequest::policy)
                  .extracting(IamPolicy::fromJson)
                  .satisfies(
                      policy -> {
                        assertThat(policy)
                            .extracting(IamPolicy::statements)
                            .asInstanceOf(InstanceOfAssertFactories.list(IamStatement.class))
                            .hasSize(4)
                            .satisfiesExactly(
                                statement ->
                                    assertThat(statement)
                                        .returns(IamEffect.ALLOW, IamStatement::effect)
                                        .returns(
                                            List.of(
                                                IamResource.create(
                                                    s3Arn(awsPartition, bucket, firstPath))),
                                            IamStatement::resources)
                                        .returns(
                                            List.of(
                                                IamAction.create("s3:PutObject"),
                                                IamAction.create("s3:DeleteObject")),
                                            IamStatement::actions),
                                statement ->
                                    assertThat(statement)
                                        .returns(IamEffect.ALLOW, IamStatement::effect)
                                        .returns(
                                            List.of(
                                                IamResource.create(
                                                    s3Arn(awsPartition, bucket, null))),
                                            IamStatement::resources)
                                        .returns(
                                            List.of(IamAction.create("s3:ListBucket")),
                                            IamStatement::actions)
                                        .returns(
                                            List.of(
                                                IamResource.create(
                                                    s3Arn(awsPartition, bucket, null))),
                                            IamStatement::resources)
                                        .satisfies(
                                            st ->
                                                assertThat(st.conditions())
                                                    .containsExactlyInAnyOrder(
                                                        IamCondition.builder()
                                                            .operator(
                                                                IamConditionOperator.STRING_LIKE)
                                                            .key("s3:prefix")
                                                            .value(secondPath + "/*")
                                                            .build(),
                                                        IamCondition.builder()
                                                            .operator(
                                                                IamConditionOperator.STRING_LIKE)
                                                            .key("s3:prefix")
                                                            .value(firstPath + "/*")
                                                            .build())),
                                statement ->
                                    assertThat(statement)
                                        .returns(IamEffect.ALLOW, IamStatement::effect)
                                        .satisfies(
                                            st ->
                                                assertThat(st.resources())
                                                    .contains(
                                                        IamResource.create(
                                                            s3Arn(awsPartition, bucket, null))))
                                        .returns(
                                            List.of(IamAction.create("s3:GetBucketLocation")),
                                            IamStatement::actions),
                                statement ->
                                    assertThat(statement)
                                        .returns(IamEffect.ALLOW, IamStatement::effect)
                                        .satisfies(
                                            st ->
                                                assertThat(st.resources())
                                                    .containsExactlyInAnyOrder(
                                                        IamResource.create(
                                                            s3Arn(awsPartition, bucket, firstPath)),
                                                        IamResource.create(
                                                            s3Arn(
                                                                awsPartition, bucket, secondPath))))
                                        .returns(
                                            List.of(
                                                IamAction.create("s3:GetObject"),
                                                IamAction.create("s3:GetObjectVersion")),
                                            IamStatement::actions));
                      });
              return ASSUME_ROLE_RESPONSE;
            });
    EnumMap<PolarisCredentialProperty, String> credentials =
        new AwsCredentialsStorageIntegration(stsClient)
            .getSubscopedCreds(
                Mockito.mock(PolarisDiagnostics.class),
                new AwsStorageConfigurationInfo(
                    storageType,
                    List.of(s3Path(bucket, warehouseKeyPrefix)),
                    roleARN,
                    externalId,
                    "us-east-2"),
                true,
                Set.of(s3Path(bucket, firstPath), s3Path(bucket, secondPath)),
                Set.of(s3Path(bucket, firstPath)));
    assertThat(credentials)
        .isNotEmpty()
        .containsEntry(PolarisCredentialProperty.AWS_TOKEN, "sess")
        .containsEntry(PolarisCredentialProperty.AWS_KEY_ID, "accessKey")
        .containsEntry(PolarisCredentialProperty.AWS_SECRET_KEY, "secretKey");
  }

  @Test
  public void testGetSubscopedCredsInlinePolicyWithoutList() {
    StsClient stsClient = Mockito.mock(StsClient.class);
    String roleARN = "arn:aws:iam::012345678901:role/jdoe";
    String externalId = "externalId";
    String bucket = "bucket";
    String warehouseKeyPrefix = "path/to/warehouse";
    String firstPath = warehouseKeyPrefix + "/namespace/table";
    String secondPath = warehouseKeyPrefix + "/oldnamespace/table";
    Mockito.when(stsClient.assumeRole(Mockito.isA(AssumeRoleRequest.class)))
        .thenAnswer(
            invocation -> {
              assertThat(invocation.getArguments()[0])
                  .isInstanceOf(AssumeRoleRequest.class)
                  .asInstanceOf(InstanceOfAssertFactories.type(AssumeRoleRequest.class))
                  .extracting(AssumeRoleRequest::policy)
                  .extracting(IamPolicy::fromJson)
                  .satisfies(
                      policy -> {
                        assertThat(policy)
                            .extracting(IamPolicy::statements)
                            .asInstanceOf(InstanceOfAssertFactories.list(IamStatement.class))
                            .hasSize(3)
                            .satisfiesExactly(
                                statement ->
                                    assertThat(statement)
                                        .returns(IamEffect.ALLOW, IamStatement::effect)
                                        .returns(
                                            List.of(
                                                IamResource.create(
                                                    s3Arn(AWS_PARTITION, bucket, firstPath))),
                                            IamStatement::resources)
                                        .returns(
                                            List.of(
                                                IamAction.create("s3:PutObject"),
                                                IamAction.create("s3:DeleteObject")),
                                            IamStatement::actions),
                                statement ->
                                    assertThat(statement)
                                        .returns(IamEffect.ALLOW, IamStatement::effect)
                                        .satisfies(
                                            st ->
                                                assertThat(st.resources())
                                                    .contains(
                                                        IamResource.create(
                                                            s3Arn(AWS_PARTITION, bucket, null))))
                                        .returns(
                                            List.of(IamAction.create("s3:GetBucketLocation")),
                                            IamStatement::actions),
                                statement ->
                                    assertThat(statement)
                                        .returns(IamEffect.ALLOW, IamStatement::effect)
                                        .satisfies(
                                            st ->
                                                assertThat(st.resources())
                                                    .containsExactlyInAnyOrder(
                                                        IamResource.create(
                                                            s3Arn(
                                                                AWS_PARTITION, bucket, firstPath)),
                                                        IamResource.create(
                                                            s3Arn(
                                                                AWS_PARTITION,
                                                                bucket,
                                                                secondPath))))
                                        .returns(
                                            List.of(
                                                IamAction.create("s3:GetObject"),
                                                IamAction.create("s3:GetObjectVersion")),
                                            IamStatement::actions));
                      });
              return ASSUME_ROLE_RESPONSE;
            });
    PolarisStorageConfigurationInfo.StorageType storageType =
        PolarisStorageConfigurationInfo.StorageType.S3;
    EnumMap<PolarisCredentialProperty, String> credentials =
        new AwsCredentialsStorageIntegration(stsClient)
            .getSubscopedCreds(
                Mockito.mock(PolarisDiagnostics.class),
                new AwsStorageConfigurationInfo(
                    PolarisStorageConfigurationInfo.StorageType.S3,
                    List.of(s3Path(bucket, warehouseKeyPrefix)),
                    roleARN,
                    externalId,
                    "us-east-2"),
                false, /* allowList = false*/
                Set.of(s3Path(bucket, firstPath), s3Path(bucket, secondPath)),
                Set.of(s3Path(bucket, firstPath)));
    assertThat(credentials)
        .isNotEmpty()
        .containsEntry(PolarisCredentialProperty.AWS_TOKEN, "sess")
        .containsEntry(PolarisCredentialProperty.AWS_KEY_ID, "accessKey")
        .containsEntry(PolarisCredentialProperty.AWS_SECRET_KEY, "secretKey");
  }

  @Test
  public void testGetSubscopedCredsInlinePolicyWithoutWrites() {
    StsClient stsClient = Mockito.mock(StsClient.class);
    String roleARN = "arn:aws:iam::012345678901:role/jdoe";
    String externalId = "externalId";
    String bucket = "bucket";
    String warehouseKeyPrefix = "path/to/warehouse";
    String firstPath = warehouseKeyPrefix + "/namespace/table";
    String secondPath = warehouseKeyPrefix + "/oldnamespace/table";
    Mockito.when(stsClient.assumeRole(Mockito.isA(AssumeRoleRequest.class)))
        .thenAnswer(
            invocation -> {
              assertThat(invocation.getArguments()[0])
                  .isInstanceOf(AssumeRoleRequest.class)
                  .asInstanceOf(InstanceOfAssertFactories.type(AssumeRoleRequest.class))
                  .extracting(AssumeRoleRequest::policy)
                  .extracting(IamPolicy::fromJson)
                  .satisfies(
                      policy -> {
                        assertThat(policy)
                            .extracting(IamPolicy::statements)
                            .asInstanceOf(InstanceOfAssertFactories.list(IamStatement.class))
                            .hasSize(3)
                            .satisfiesExactly(
                                statement ->
                                    assertThat(statement)
                                        .returns(IamEffect.ALLOW, IamStatement::effect)
                                        .returns(
                                            List.of(
                                                IamResource.create(
                                                    s3Arn(AWS_PARTITION, bucket, null))),
                                            IamStatement::resources)
                                        .returns(
                                            List.of(IamAction.create("s3:ListBucket")),
                                            IamStatement::actions),
                                statement ->
                                    assertThat(statement)
                                        .returns(IamEffect.ALLOW, IamStatement::effect)
                                        .satisfies(
                                            st ->
                                                assertThat(st.resources())
                                                    .contains(
                                                        IamResource.create(
                                                            s3Arn(AWS_PARTITION, bucket, null))))
                                        .returns(
                                            List.of(IamAction.create("s3:GetBucketLocation")),
                                            IamStatement::actions),
                                statement ->
                                    assertThat(statement)
                                        .returns(IamEffect.ALLOW, IamStatement::effect)
                                        .satisfies(
                                            st ->
                                                assertThat(st.resources())
                                                    .containsExactlyInAnyOrder(
                                                        IamResource.create(
                                                            s3Arn(
                                                                AWS_PARTITION, bucket, firstPath)),
                                                        IamResource.create(
                                                            s3Arn(
                                                                AWS_PARTITION,
                                                                bucket,
                                                                secondPath))))
                                        .returns(
                                            List.of(
                                                IamAction.create("s3:GetObject"),
                                                IamAction.create("s3:GetObjectVersion")),
                                            IamStatement::actions));
                      });
              return ASSUME_ROLE_RESPONSE;
            });
    PolarisStorageConfigurationInfo.StorageType storageType =
        PolarisStorageConfigurationInfo.StorageType.S3;
    EnumMap<PolarisCredentialProperty, String> credentials =
        new AwsCredentialsStorageIntegration(stsClient)
            .getSubscopedCreds(
                Mockito.mock(PolarisDiagnostics.class),
                new AwsStorageConfigurationInfo(
                    storageType,
                    List.of(s3Path(bucket, warehouseKeyPrefix)),
                    roleARN,
                    externalId,
                    "us-east-2"),
                true, /* allowList = true */
                Set.of(s3Path(bucket, firstPath), s3Path(bucket, secondPath)),
                Set.of());
    assertThat(credentials)
        .isNotEmpty()
        .containsEntry(PolarisCredentialProperty.AWS_TOKEN, "sess")
        .containsEntry(PolarisCredentialProperty.AWS_KEY_ID, "accessKey")
        .containsEntry(PolarisCredentialProperty.AWS_SECRET_KEY, "secretKey");
  }

  @Test
  public void testGetSubscopedCredsInlinePolicyWithEmptyReadAndWrite() {
    StsClient stsClient = Mockito.mock(StsClient.class);
    String roleARN = "arn:aws:iam::012345678901:role/jdoe";
    String externalId = "externalId";
    String bucket = "bucket";
    String warehouseKeyPrefix = "path/to/warehouse";
    Mockito.when(stsClient.assumeRole(Mockito.isA(AssumeRoleRequest.class)))
        .thenAnswer(
            invocation -> {
              assertThat(invocation.getArguments()[0])
                  .isInstanceOf(AssumeRoleRequest.class)
                  .asInstanceOf(InstanceOfAssertFactories.type(AssumeRoleRequest.class))
                  .extracting(AssumeRoleRequest::policy)
                  .extracting(IamPolicy::fromJson)
                  .satisfies(
                      policy -> {
                        assertThat(policy)
                            .extracting(IamPolicy::statements)
                            .asInstanceOf(InstanceOfAssertFactories.list(IamStatement.class))
                            .hasSize(2)
                            .satisfiesExactly(
                                statement ->
                                    assertThat(statement)
                                        .returns(IamEffect.ALLOW, IamStatement::effect)
                                        .returns(List.of(), IamStatement::resources)
                                        .returns(
                                            List.of(IamAction.create("s3:ListBucket")),
                                            IamStatement::actions),
                                statement ->
                                    assertThat(statement)
                                        .returns(IamEffect.ALLOW, IamStatement::effect)
                                        .satisfies(
                                            st -> assertThat(st.resources()).containsExactly())
                                        .returns(
                                            List.of(
                                                IamAction.create("s3:GetObject"),
                                                IamAction.create("s3:GetObjectVersion")),
                                            IamStatement::actions));
                      });
              return ASSUME_ROLE_RESPONSE;
            });
    EnumMap<PolarisCredentialProperty, String> credentials =
        new AwsCredentialsStorageIntegration(stsClient)
            .getSubscopedCreds(
                Mockito.mock(PolarisDiagnostics.class),
                new AwsStorageConfigurationInfo(
                    PolarisStorageConfigurationInfo.StorageType.S3,
                    List.of(s3Path(bucket, warehouseKeyPrefix)),
                    roleARN,
                    externalId,
                    "us-east-2"),
                true, /* allowList = true */
                Set.of(),
                Set.of());
    assertThat(credentials)
        .isNotEmpty()
        .containsEntry(PolarisCredentialProperty.AWS_TOKEN, "sess")
        .containsEntry(PolarisCredentialProperty.AWS_KEY_ID, "accessKey")
        .containsEntry(PolarisCredentialProperty.AWS_SECRET_KEY, "secretKey");
  }

  @Test
  public void testClientRegion() {
    StsClient stsClient = Mockito.mock(StsClient.class);
    String roleARN = "arn:aws:iam::012345678901:role/jdoe";
    String externalId = "externalId";
    String bucket = "bucket";
    String warehouseKeyPrefix = "path/to/warehouse";
    String clientRegion = "test-region";
    Mockito.when(stsClient.assumeRole(Mockito.isA(AssumeRoleRequest.class)))
        .thenAnswer(
            invocation -> {
              return ASSUME_ROLE_RESPONSE;
            });
    EnumMap<PolarisCredentialProperty, String> credentials =
        new AwsCredentialsStorageIntegration(stsClient)
            .getSubscopedCreds(
                Mockito.mock(PolarisDiagnostics.class),
                new AwsStorageConfigurationInfo(
                    PolarisStorageConfigurationInfo.StorageType.S3,
                    List.of(s3Path(bucket, warehouseKeyPrefix)),
                    roleARN,
                    externalId,
                    clientRegion),
                true, /* allowList = true */
                Set.of(),
                Set.of());
    assertThat(credentials)
        .isNotEmpty()
        .containsEntry(PolarisCredentialProperty.CLIENT_REGION, clientRegion);
  }

  @Test
  public void testNoClientRegion() {
    StsClient stsClient = Mockito.mock(StsClient.class);
    String roleARN = "arn:aws:iam::012345678901:role/jdoe";
    String externalId = "externalId";
    String bucket = "bucket";
    String warehouseKeyPrefix = "path/to/warehouse";
    Mockito.when(stsClient.assumeRole(Mockito.isA(AssumeRoleRequest.class)))
        .thenAnswer(
            invocation -> {
              return ASSUME_ROLE_RESPONSE;
            });
    EnumMap<PolarisCredentialProperty, String> credentials =
        new AwsCredentialsStorageIntegration(stsClient)
            .getSubscopedCreds(
                Mockito.mock(PolarisDiagnostics.class),
                new AwsStorageConfigurationInfo(
                    PolarisStorageConfigurationInfo.StorageType.S3,
                    List.of(s3Path(bucket, warehouseKeyPrefix)),
                    roleARN,
                    externalId,
                    null),
                true, /* allowList = true */
                Set.of(),
                Set.of());
    assertThat(credentials).isNotEmpty().doesNotContainKey(PolarisCredentialProperty.CLIENT_REGION);
  }

  private static @NotNull String s3Arn(String partition, String bucket, String keyPrefix) {
    String bucketArn = "arn:" + partition + ":s3:::" + bucket;
    if (keyPrefix == null) {
      return bucketArn;
    }
    return bucketArn + "/" + keyPrefix + "/*";
  }

  private static @NotNull String s3Path(String bucket, String keyPrefix) {
    return "s3://" + bucket + "/" + keyPrefix;
  }
}
