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

import jakarta.annotation.Nonnull;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.storage.BaseStorageIntegrationTest;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.aws.AwsCredentialsStorageIntegration;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
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
import software.amazon.awssdk.services.sts.model.StsException;

class AwsCredentialsStorageIntegrationTest extends BaseStorageIntegrationTest {

  public static final Instant EXPIRE_TIME = Instant.now().plusMillis(3600_000);

  public static final RealmConfig PRINCIPAL_INCLUDER_REALM_CONFIG =
      new RealmConfigImpl(
          new PolarisConfigurationStore() {
            @SuppressWarnings("unchecked")
            @Override
            public String getConfiguration(@Nonnull RealmContext ctx, String configName) {
              if (configName.equals(
                  FeatureConfiguration.INCLUDE_PRINCIPAL_NAME_IN_SUBSCOPED_CREDENTIAL.key())) {
                return "true";
              }
              return null;
            }
          },
          () -> "realm");

  public static final AssumeRoleResponse ASSUME_ROLE_RESPONSE =
      AssumeRoleResponse.builder()
          .credentials(
              Credentials.builder()
                  .accessKeyId("accessKey")
                  .secretAccessKey("secretKey")
                  .sessionToken("sess")
                  .expiration(EXPIRE_TIME)
                  .build())
          .build();
  public static final String AWS_PARTITION = "aws";
  public static final PolarisPrincipal POLARIS_PRINCIPAL =
      PolarisPrincipal.of("test-principal", Map.of(), Set.of());

  @ParameterizedTest
  @ValueSource(strings = {"s3a", "s3"})
  public void testGetSubscopedCreds(String scheme) {
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
                  .returns(roleARN, AssumeRoleRequest::roleArn)
                  .returns(
                      "PolarisAwsCredentialsStorageIntegration", AssumeRoleRequest::roleSessionName)
                  // ensure that the policy content does not refer to S3A
                  .extracting(AssumeRoleRequest::policy)
                  .doesNotMatch(s -> s.contains("s3a"));
              return ASSUME_ROLE_RESPONSE;
            });
    String warehouseDir = scheme + "://bucket/path/to/warehouse";
    StorageAccessConfig storageAccessConfig =
        new AwsCredentialsStorageIntegration(
                AwsStorageConfigurationInfo.builder()
                    .addAllowedLocation(warehouseDir)
                    .roleARN(roleARN)
                    .externalId(externalId)
                    .build(),
                stsClient)
            .getSubscopedCreds(
                EMPTY_REALM_CONFIG,
                true,
                Set.of(warehouseDir + "/namespace/table"),
                Set.of(warehouseDir + "/namespace/table"),
                POLARIS_PRINCIPAL,
                Optional.of("/namespace/table/credentials"),
                CredentialVendingContext.empty());
    assertThat(storageAccessConfig.credentials())
        .isNotEmpty()
        .containsEntry(StorageAccessProperty.AWS_TOKEN.getPropertyName(), "sess")
        .containsEntry(StorageAccessProperty.AWS_KEY_ID.getPropertyName(), "accessKey")
        .containsEntry(StorageAccessProperty.AWS_SECRET_KEY.getPropertyName(), "secretKey")
        .containsEntry(
            StorageAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS.getPropertyName(),
            String.valueOf(EXPIRE_TIME.toEpochMilli()));
    assertThat(storageAccessConfig.extraProperties())
        .containsEntry(
            StorageAccessProperty.AWS_REFRESH_CREDENTIALS_ENDPOINT.getPropertyName(),
            "/namespace/table/credentials");
  }

  // uses different realm config with INCLUDE_PRINCIPAL_NAME_IN_SUBSCOPED_CREDENTIAL set to true
  // tests that the resulting role session name includes principal name
  @Test
  public void testGetSubscopedCredsRoleSessionNameWithPrincipalIncluded() {
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
                  .returns(roleARN, AssumeRoleRequest::roleArn)
                  .returns("polaris-test-principal", AssumeRoleRequest::roleSessionName);
              return ASSUME_ROLE_RESPONSE;
            });
    String warehouseDir = "s3://bucket/path/to/warehouse";
    StorageAccessConfig storageAccessConfig =
        new AwsCredentialsStorageIntegration(
                AwsStorageConfigurationInfo.builder()
                    .addAllowedLocation(warehouseDir)
                    .roleARN(roleARN)
                    .externalId(externalId)
                    .build(),
                stsClient)
            .getSubscopedCreds(
                PRINCIPAL_INCLUDER_REALM_CONFIG,
                true,
                Set.of(warehouseDir + "/namespace/table"),
                Set.of(warehouseDir + "/namespace/table"),
                POLARIS_PRINCIPAL,
                Optional.of("/namespace/table/credentials"),
                CredentialVendingContext.empty());
  }

  @ParameterizedTest
  @ValueSource(strings = {AWS_PARTITION, "aws-cn", "aws-us-gov"})
  public void testGetSubscopedCredsInlinePolicy(String awsPartition) {
    String roleARN;
    String region;
    switch (awsPartition) {
      case AWS_PARTITION:
        roleARN = "arn:aws:iam::012345678901:role/jdoe";
        region = "us-east-1";
        break;
      case "aws-cn":
        roleARN = "arn:aws-cn:iam::012345678901:role/jdoe";
        region = "Beijing";
        break;
      case "aws-us-gov":
        roleARN = "arn:aws-us-gov:iam::012345678901:role/jdoe";
        region = "us-gov-west-1";
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
    switch (awsPartition) {
      case "aws-cn":
      case AWS_PARTITION:
      case "aws-us-gov":
        StorageAccessConfig storageAccessConfig =
            new AwsCredentialsStorageIntegration(
                    AwsStorageConfigurationInfo.builder()
                        .addAllowedLocation(s3Path(bucket, warehouseKeyPrefix))
                        .roleARN(roleARN)
                        .externalId(externalId)
                        .region(region)
                        .build(),
                    stsClient)
                .getSubscopedCreds(
                    EMPTY_REALM_CONFIG,
                    true,
                    Set.of(s3Path(bucket, firstPath), s3Path(bucket, secondPath)),
                    Set.of(s3Path(bucket, firstPath)),
                    POLARIS_PRINCIPAL,
                    Optional.empty(),
                    CredentialVendingContext.empty());
        assertThat(storageAccessConfig.credentials())
            .isNotEmpty()
            .containsEntry(StorageAccessProperty.AWS_TOKEN.getPropertyName(), "sess")
            .containsEntry(StorageAccessProperty.AWS_KEY_ID.getPropertyName(), "accessKey")
            .containsEntry(StorageAccessProperty.AWS_SECRET_KEY.getPropertyName(), "secretKey")
            .containsEntry(
                StorageAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS.getPropertyName(),
                String.valueOf(EXPIRE_TIME.toEpochMilli()));
        break;
      default:
        throw new IllegalArgumentException("Unknown aws partition: " + awsPartition);
    }
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
    StorageAccessConfig storageAccessConfig =
        new AwsCredentialsStorageIntegration(
                AwsStorageConfigurationInfo.builder()
                    .addAllowedLocation(s3Path(bucket, warehouseKeyPrefix))
                    .roleARN(roleARN)
                    .externalId(externalId)
                    .region("us-east-2")
                    .build(),
                stsClient)
            .getSubscopedCreds(
                EMPTY_REALM_CONFIG,
                false, /* allowList = false*/
                Set.of(s3Path(bucket, firstPath), s3Path(bucket, secondPath)),
                Set.of(s3Path(bucket, firstPath)),
                POLARIS_PRINCIPAL,
                Optional.empty(),
                CredentialVendingContext.empty());
    assertThat(storageAccessConfig.credentials())
        .isNotEmpty()
        .containsEntry(StorageAccessProperty.AWS_TOKEN.getPropertyName(), "sess")
        .containsEntry(StorageAccessProperty.AWS_KEY_ID.getPropertyName(), "accessKey")
        .containsEntry(StorageAccessProperty.AWS_SECRET_KEY.getPropertyName(), "secretKey")
        .containsEntry(
            StorageAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS.getPropertyName(),
            String.valueOf(EXPIRE_TIME.toEpochMilli()));
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
    String region = "us-east-2";
    String accountId = "012345678901";
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
                                                IamAction.create(
                                                    "kms:GenerateDataKeyWithoutPlaintext"),
                                                IamAction.create("kms:DescribeKey"),
                                                IamAction.create("kms:Decrypt"),
                                                IamAction.create("kms:GenerateDataKey")),
                                            IamStatement::actions)
                                        .returns(
                                            List.of(
                                                IamResource.create(
                                                    String.format(
                                                        "arn:aws:kms:%s:%s:key/*",
                                                        region, accountId))),
                                            IamStatement::resources),
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
    StorageAccessConfig storageAccessConfig =
        new AwsCredentialsStorageIntegration(
                AwsStorageConfigurationInfo.builder()
                    .addAllowedLocation(s3Path(bucket, warehouseKeyPrefix))
                    .roleARN(roleARN)
                    .externalId(externalId)
                    .region(region)
                    .build(),
                stsClient)
            .getSubscopedCreds(
                EMPTY_REALM_CONFIG,
                true, /* allowList = true */
                Set.of(s3Path(bucket, firstPath), s3Path(bucket, secondPath)),
                Set.of(),
                POLARIS_PRINCIPAL,
                Optional.empty(),
                CredentialVendingContext.empty());
    assertThat(storageAccessConfig.credentials())
        .isNotEmpty()
        .containsEntry(StorageAccessProperty.AWS_TOKEN.getPropertyName(), "sess")
        .containsEntry(StorageAccessProperty.AWS_KEY_ID.getPropertyName(), "accessKey")
        .containsEntry(StorageAccessProperty.AWS_SECRET_KEY.getPropertyName(), "secretKey")
        .containsEntry(
            StorageAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS.getPropertyName(),
            String.valueOf(EXPIRE_TIME.toEpochMilli()));
  }

  @Test
  public void testGetSubscopedCredsInlinePolicyWithEmptyReadAndWrite() {
    StsClient stsClient = Mockito.mock(StsClient.class);
    String roleARN = "arn:aws:iam::012345678901:role/jdoe";
    String externalId = "externalId";
    String bucket = "bucket";
    String warehouseKeyPrefix = "path/to/warehouse";
    String region = "us-east-2";
    String accountId = "012345678901";
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
                            .satisfiesExactlyInAnyOrder(
                                statement ->
                                    assertThat(statement)
                                        .returns(IamEffect.ALLOW, IamStatement::effect)
                                        .returns(
                                            List.of(
                                                IamAction.create(
                                                    "kms:GenerateDataKeyWithoutPlaintext"),
                                                IamAction.create("kms:DescribeKey"),
                                                IamAction.create("kms:Decrypt"),
                                                IamAction.create("kms:GenerateDataKey")),
                                            IamStatement::actions)
                                        .returns(
                                            List.of(
                                                IamResource.create(
                                                    String.format(
                                                        "arn:aws:kms:%s:%s:key/*",
                                                        region, accountId))),
                                            IamStatement::resources),
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
    StorageAccessConfig storageAccessConfig =
        new AwsCredentialsStorageIntegration(
                AwsStorageConfigurationInfo.builder()
                    .addAllowedLocation(s3Path(bucket, warehouseKeyPrefix))
                    .roleARN(roleARN)
                    .externalId(externalId)
                    .region(region)
                    .build(),
                stsClient)
            .getSubscopedCreds(
                EMPTY_REALM_CONFIG,
                true, /* allowList = true */
                Set.of(),
                Set.of(),
                POLARIS_PRINCIPAL,
                Optional.empty(),
                CredentialVendingContext.empty());
    assertThat(storageAccessConfig.credentials())
        .isNotEmpty()
        .containsEntry(StorageAccessProperty.AWS_TOKEN.getPropertyName(), "sess")
        .containsEntry(StorageAccessProperty.AWS_KEY_ID.getPropertyName(), "accessKey")
        .containsEntry(StorageAccessProperty.AWS_SECRET_KEY.getPropertyName(), "secretKey")
        .containsEntry(
            StorageAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS.getPropertyName(),
            String.valueOf(EXPIRE_TIME.toEpochMilli()));
  }

  @ParameterizedTest
  @ValueSource(strings = {AWS_PARTITION, "aws-cn", "aws-us-gov"})
  public void testClientRegion(String awsPartition) {
    StsClient stsClient = Mockito.mock(StsClient.class);
    String roleARN = "arn:aws:iam::012345678901:role/jdoe".replaceFirst("aws", awsPartition);
    String externalId = "externalId";
    String bucket = "bucket";
    String warehouseKeyPrefix = "path/to/warehouse";
    String clientRegion = "test-region";
    Mockito.when(stsClient.assumeRole(Mockito.isA(AssumeRoleRequest.class)))
        .thenAnswer(
            invocation -> {
              return ASSUME_ROLE_RESPONSE;
            });
    switch (awsPartition) {
      case "aws-cn":
      case AWS_PARTITION:
      case "aws-us-gov":
        StorageAccessConfig storageAccessConfig =
            new AwsCredentialsStorageIntegration(
                    AwsStorageConfigurationInfo.builder()
                        .addAllowedLocation(s3Path(bucket, warehouseKeyPrefix))
                        .roleARN(roleARN)
                        .externalId(externalId)
                        .region(clientRegion)
                        .build(),
                    stsClient)
                .getSubscopedCreds(
                    EMPTY_REALM_CONFIG,
                    true, /* allowList = true */
                    Set.of(),
                    Set.of(),
                    POLARIS_PRINCIPAL,
                    Optional.empty(),
                    CredentialVendingContext.empty());
        assertThat(storageAccessConfig.credentials())
            .containsEntry(StorageAccessProperty.AWS_TOKEN.getPropertyName(), "sess")
            .containsEntry(StorageAccessProperty.AWS_KEY_ID.getPropertyName(), "accessKey")
            .containsEntry(StorageAccessProperty.AWS_SECRET_KEY.getPropertyName(), "secretKey")
            .doesNotContainKey(StorageAccessProperty.CLIENT_REGION.getPropertyName());
        break;
      default:
        throw new IllegalArgumentException("Unknown aws partition: " + awsPartition);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {AWS_PARTITION, "aws-cn", "aws-us-gov"})
  public void testNoClientRegion(String awsPartition) {
    StsClient stsClient = Mockito.mock(StsClient.class);
    String roleARN = "arn:aws:iam::012345678901:role/jdoe".replaceFirst("aws", awsPartition);
    String externalId = "externalId";
    String bucket = "bucket";
    String warehouseKeyPrefix = "path/to/warehouse";
    Mockito.when(stsClient.assumeRole(Mockito.isA(AssumeRoleRequest.class)))
        .thenAnswer(
            invocation -> {
              return ASSUME_ROLE_RESPONSE;
            });
    switch (awsPartition) {
      case AWS_PARTITION:
      case "aws-cn":
        StorageAccessConfig storageAccessConfig =
            new AwsCredentialsStorageIntegration(
                    AwsStorageConfigurationInfo.builder()
                        .addAllowedLocation(s3Path(bucket, warehouseKeyPrefix))
                        .roleARN(roleARN)
                        .externalId(externalId)
                        .build(),
                    stsClient)
                .getSubscopedCreds(
                    EMPTY_REALM_CONFIG,
                    true, /* allowList = true */
                    Set.of(),
                    Set.of(),
                    POLARIS_PRINCIPAL,
                    Optional.empty(),
                    CredentialVendingContext.empty());
        assertThat(storageAccessConfig.credentials())
            .isNotEmpty()
            .doesNotContainKey(StorageAccessProperty.CLIENT_REGION.getPropertyName());
        break;
      case "aws-us-gov":
        Assertions.assertThatThrownBy(
                () ->
                    new AwsCredentialsStorageIntegration(
                            AwsStorageConfigurationInfo.builder()
                                .addAllowedLocation(s3Path(bucket, warehouseKeyPrefix))
                                .roleARN(roleARN)
                                .externalId(externalId)
                                .build(),
                            stsClient)
                        .getSubscopedCreds(
                            EMPTY_REALM_CONFIG,
                            true, /* allowList = true */
                            Set.of(),
                            Set.of(),
                            POLARIS_PRINCIPAL,
                            Optional.empty(),
                            CredentialVendingContext.empty()))
            .isInstanceOf(IllegalArgumentException.class);
        break;
      default:
        throw new IllegalArgumentException("Unknown aws partition: " + awsPartition);
    }
    ;
  }

  @Test
  public void testKmsKeyPolicyLogic() {
    StsClient stsClient = Mockito.mock(StsClient.class);
    String roleARN = "arn:aws:iam::012345678901:role/jdoe";
    String externalId = "externalId";
    String bucket = "bucket";
    String warehouseKeyPrefix = "path/to/warehouse";
    String region = "us-east-1";
    String accountId = "012345678901";
    String currentKmsKey = "arn:aws:kms:us-east-1:012345678901:key/current-key";
    List<String> allowedKmsKeys =
        List.of(
            "arn:aws:kms:us-east-1:012345678901:key/allowed-key-1",
            "arn:aws:kms:us-east-1:012345678901:key/allowed-key-2");

    // Test with current KMS key and write permissions
    Mockito.when(stsClient.assumeRole(Mockito.isA(AssumeRoleRequest.class)))
        .thenAnswer(
            invocation -> {
              AssumeRoleRequest request = invocation.getArgument(0);
              IamPolicy policy = IamPolicy.fromJson(request.policy());

              // Verify KMS statement exists with write permissions
              assertThat(policy.statements())
                  .anySatisfy(
                      stmt -> {
                        assertThat(stmt.actions())
                            .containsAll(
                                List.of(
                                    IamAction.create("kms:GenerateDataKeyWithoutPlaintext"),
                                    IamAction.create("kms:DescribeKey"),
                                    IamAction.create("kms:Decrypt"),
                                    IamAction.create("kms:GenerateDataKey"),
                                    IamAction.create("kms:Encrypt")));
                        assertThat(stmt.resources()).contains(IamResource.create(currentKmsKey));
                      });

              return ASSUME_ROLE_RESPONSE;
            });

    new AwsCredentialsStorageIntegration(
            AwsStorageConfigurationInfo.builder()
                .addAllowedLocation(s3Path(bucket, warehouseKeyPrefix))
                .roleARN(roleARN)
                .externalId(externalId)
                .region(region)
                .currentKmsKey(currentKmsKey)
                .build(),
            stsClient)
        .getSubscopedCreds(
            EMPTY_REALM_CONFIG,
            true,
            Set.of(s3Path(bucket, warehouseKeyPrefix + "/table")),
            Set.of(s3Path(bucket, warehouseKeyPrefix + "/table")),
            POLARIS_PRINCIPAL,
            Optional.empty(),
            CredentialVendingContext.empty());

    // Test with allowed KMS keys and read-only permissions
    Mockito.reset(stsClient);
    Mockito.when(stsClient.assumeRole(Mockito.isA(AssumeRoleRequest.class)))
        .thenAnswer(
            invocation -> {
              AssumeRoleRequest request = invocation.getArgument(0);
              IamPolicy policy = IamPolicy.fromJson(request.policy());

              // Verify KMS statement exists with read-only permissions
              assertThat(policy.statements())
                  .anySatisfy(
                      stmt -> {
                        assertThat(stmt.actions())
                            .containsAll(
                                List.of(
                                    IamAction.create("kms:GenerateDataKeyWithoutPlaintext"),
                                    IamAction.create("kms:DescribeKey"),
                                    IamAction.create("kms:Decrypt"),
                                    IamAction.create("kms:GenerateDataKey")));
                        assertThat(stmt.actions()).doesNotContain(IamAction.create("kms:Encrypt"));
                        assertThat(stmt.resources())
                            .containsExactlyInAnyOrder(
                                IamResource.create(allowedKmsKeys.get(0)),
                                IamResource.create(allowedKmsKeys.get(1)));
                      });

              return ASSUME_ROLE_RESPONSE;
            });

    new AwsCredentialsStorageIntegration(
            AwsStorageConfigurationInfo.builder()
                .addAllowedLocation(s3Path(bucket, warehouseKeyPrefix))
                .roleARN(roleARN)
                .externalId(externalId)
                .region(region)
                .allowedKmsKeys(allowedKmsKeys)
                .build(),
            stsClient)
        .getSubscopedCreds(
            EMPTY_REALM_CONFIG,
            true,
            Set.of(s3Path(bucket, warehouseKeyPrefix + "/table")),
            Set.of(),
            POLARIS_PRINCIPAL,
            Optional.empty(),
            CredentialVendingContext.empty());

    // Test with no KMS keys and read-only (should add wildcard KMS access)
    Mockito.reset(stsClient);
    Mockito.when(stsClient.assumeRole(Mockito.isA(AssumeRoleRequest.class)))
        .thenAnswer(
            invocation -> {
              AssumeRoleRequest request = invocation.getArgument(0);
              IamPolicy policy = IamPolicy.fromJson(request.policy());

              // Verify wildcard KMS statement exists
              assertThat(policy.statements())
                  .anySatisfy(
                      stmt -> {
                        assertThat(stmt.resources())
                            .contains(
                                IamResource.create(
                                    String.format("arn:aws:kms:%s:%s:key/*", region, accountId)));
                      });

              return ASSUME_ROLE_RESPONSE;
            });

    new AwsCredentialsStorageIntegration(
            AwsStorageConfigurationInfo.builder()
                .addAllowedLocation(s3Path(bucket, warehouseKeyPrefix))
                .roleARN(roleARN)
                .externalId(externalId)
                .region(region)
                .build(),
            stsClient)
        .getSubscopedCreds(
            EMPTY_REALM_CONFIG,
            true,
            Set.of(s3Path(bucket, warehouseKeyPrefix + "/table")),
            Set.of(),
            POLARIS_PRINCIPAL,
            Optional.empty(),
            CredentialVendingContext.empty());

    // Test with no KMS keys and write permissions (should not add KMS statement)
    Mockito.reset(stsClient);
    Mockito.when(stsClient.assumeRole(Mockito.isA(AssumeRoleRequest.class)))
        .thenAnswer(
            invocation -> {
              AssumeRoleRequest request = invocation.getArgument(0);
              IamPolicy policy = IamPolicy.fromJson(request.policy());

              // Verify no KMS statement exists
              assertThat(policy.statements())
                  .noneMatch(
                      stmt ->
                          stmt.actions().stream()
                              .anyMatch(action -> action.value().startsWith("kms:")));

              return ASSUME_ROLE_RESPONSE;
            });

    new AwsCredentialsStorageIntegration(
            AwsStorageConfigurationInfo.builder()
                .addAllowedLocation(s3Path(bucket, warehouseKeyPrefix))
                .roleARN(roleARN)
                .externalId(externalId)
                .region(region)
                .build(),
            stsClient)
        .getSubscopedCreds(
            EMPTY_REALM_CONFIG,
            true,
            Set.of(s3Path(bucket, warehouseKeyPrefix + "/table")),
            Set.of(s3Path(bucket, warehouseKeyPrefix + "/table")),
            POLARIS_PRINCIPAL,
            Optional.empty(),
            CredentialVendingContext.empty());
  }

  @Test
  public void testGetSubscopedCredsLongPrincipalName() {
    StsClient stsClient = Mockito.mock(StsClient.class);
    String roleARN = "arn:aws:iam::012345678901:role/jdoe";
    String externalId = "externalId";
    PolarisPrincipal polarisPrincipalWithLongName =
        PolarisPrincipal.of(
            "very-long-principal-name-that-exceeds-the-maximum-allowed-length-of-64-characters",
            Map.of(),
            Set.of());

    Mockito.when(stsClient.assumeRole(Mockito.isA(AssumeRoleRequest.class)))
        .thenAnswer(
            invocation -> {
              assertThat(invocation.getArguments()[0])
                  .isInstanceOf(AssumeRoleRequest.class)
                  .asInstanceOf(InstanceOfAssertFactories.type(AssumeRoleRequest.class))
                  .returns(externalId, AssumeRoleRequest::externalId)
                  .returns(roleARN, AssumeRoleRequest::roleArn)
                  .returns(
                      "polaris-very-long-principal-name-that-exceeds-the-maximum-allowe",
                      AssumeRoleRequest::roleSessionName);
              return ASSUME_ROLE_RESPONSE;
            });
    String warehouseDir = "s3://bucket/path/to/warehouse";
    new AwsCredentialsStorageIntegration(
            AwsStorageConfigurationInfo.builder()
                .addAllowedLocation(warehouseDir)
                .roleARN(roleARN)
                .externalId(externalId)
                .build(),
            stsClient)
        .getSubscopedCreds(
            PRINCIPAL_INCLUDER_REALM_CONFIG,
            true,
            Set.of(warehouseDir + "/namespace/table"),
            Set.of(warehouseDir + "/namespace/table"),
            polarisPrincipalWithLongName,
            Optional.of("/namespace/table/credentials"),
            CredentialVendingContext.empty());
  }

  private static @Nonnull String s3Arn(String partition, String bucket, String keyPrefix) {
    String bucketArn = "arn:" + partition + ":s3:::" + bucket;
    if (keyPrefix == null) {
      return bucketArn;
    }
    return bucketArn + "/" + keyPrefix + "/*";
  }

  private static @Nonnull String s3Path(String bucket, String keyPrefix) {
    return "s3://" + bucket + "/" + keyPrefix;
  }

  // Tests for AWS STS Session Tags functionality

  @Test
  public void testSessionTagsIncludedWhenFeatureEnabled() {
    StsClient stsClient = Mockito.mock(StsClient.class);
    String roleARN = "arn:aws:iam::012345678901:role/jdoe";
    String externalId = "externalId";
    String bucket = "bucket";
    String warehouseKeyPrefix = "path/to/warehouse";

    // Create a realm config with session tags enabled
    RealmConfig sessionTagsEnabledConfig =
        new RealmConfigImpl(
            new PolarisConfigurationStore() {
              @SuppressWarnings("unchecked")
              @Override
              public String getConfiguration(@Nonnull RealmContext ctx, String configName) {
                if (configName.equals(
                    FeatureConfiguration.INCLUDE_SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL.key())) {
                  return "true";
                }
                return null;
              }
            },
            () -> "realm");

    ArgumentCaptor<AssumeRoleRequest> requestCaptor =
        ArgumentCaptor.forClass(AssumeRoleRequest.class);
    Mockito.when(stsClient.assumeRole(requestCaptor.capture())).thenReturn(ASSUME_ROLE_RESPONSE);

    // Roles are included in context (not extracted from principal) to be part of cache key
    CredentialVendingContext context =
        CredentialVendingContext.builder()
            .catalogName(Optional.of("test-catalog"))
            .namespace(Optional.of("db.schema"))
            .tableName(Optional.of("my_table"))
            .activatedRoles(Optional.of("admin,reader"))
            .build();

    new AwsCredentialsStorageIntegration(
            AwsStorageConfigurationInfo.builder()
                .addAllowedLocation(s3Path(bucket, warehouseKeyPrefix))
                .roleARN(roleARN)
                .externalId(externalId)
                .build(),
            stsClient)
        .getSubscopedCreds(
            sessionTagsEnabledConfig,
            true,
            Set.of(s3Path(bucket, warehouseKeyPrefix)),
            Set.of(s3Path(bucket, warehouseKeyPrefix)),
            POLARIS_PRINCIPAL,
            Optional.empty(),
            context);

    AssumeRoleRequest capturedRequest = requestCaptor.getValue();
    Assertions.assertThat(capturedRequest.tags()).isNotEmpty();
    Assertions.assertThat(capturedRequest.tags())
        .anyMatch(tag -> tag.key().equals("polaris:catalog") && tag.value().equals("test-catalog"));
    Assertions.assertThat(capturedRequest.tags())
        .anyMatch(tag -> tag.key().equals("polaris:namespace") && tag.value().equals("db.schema"));
    Assertions.assertThat(capturedRequest.tags())
        .anyMatch(tag -> tag.key().equals("polaris:table") && tag.value().equals("my_table"));
    Assertions.assertThat(capturedRequest.tags())
        .anyMatch(
            tag -> tag.key().equals("polaris:principal") && tag.value().equals("test-principal"));
    // Roles are sorted alphabetically and joined with comma
    Assertions.assertThat(capturedRequest.tags())
        .anyMatch(tag -> tag.key().equals("polaris:roles") && tag.value().equals("admin,reader"));
    // trace_id is "unknown" when not provided in context
    Assertions.assertThat(capturedRequest.tags())
        .anyMatch(tag -> tag.key().equals("polaris:trace_id") && tag.value().equals("unknown"));

    // Verify transitive tag keys are set
    Assertions.assertThat(capturedRequest.transitiveTagKeys())
        .containsExactlyInAnyOrder(
            "polaris:catalog",
            "polaris:namespace",
            "polaris:table",
            "polaris:principal",
            "polaris:roles",
            "polaris:trace_id");
  }

  @Test
  public void testSessionTagsNotIncludedWhenFeatureDisabled() {
    StsClient stsClient = Mockito.mock(StsClient.class);
    String roleARN = "arn:aws:iam::012345678901:role/jdoe";
    String externalId = "externalId";
    String bucket = "bucket";
    String warehouseKeyPrefix = "path/to/warehouse";

    ArgumentCaptor<AssumeRoleRequest> requestCaptor =
        ArgumentCaptor.forClass(AssumeRoleRequest.class);
    Mockito.when(stsClient.assumeRole(requestCaptor.capture())).thenReturn(ASSUME_ROLE_RESPONSE);

    CredentialVendingContext context =
        CredentialVendingContext.builder()
            .catalogName(Optional.of("test-catalog"))
            .namespace(Optional.of("db.schema"))
            .tableName(Optional.of("my_table"))
            .build();

    // Use EMPTY_REALM_CONFIG which has session tags disabled by default
    new AwsCredentialsStorageIntegration(
            AwsStorageConfigurationInfo.builder()
                .addAllowedLocation(s3Path(bucket, warehouseKeyPrefix))
                .roleARN(roleARN)
                .externalId(externalId)
                .build(),
            stsClient)
        .getSubscopedCreds(
            EMPTY_REALM_CONFIG,
            true,
            Set.of(s3Path(bucket, warehouseKeyPrefix)),
            Set.of(s3Path(bucket, warehouseKeyPrefix)),
            POLARIS_PRINCIPAL,
            Optional.empty(),
            context);

    AssumeRoleRequest capturedRequest = requestCaptor.getValue();
    // Tags should be empty when feature is disabled
    Assertions.assertThat(capturedRequest.tags()).isEmpty();
    Assertions.assertThat(capturedRequest.transitiveTagKeys()).isEmpty();
  }

  @Test
  public void testSessionTagsWithPartialContext() {
    StsClient stsClient = Mockito.mock(StsClient.class);
    String roleARN = "arn:aws:iam::012345678901:role/jdoe";
    String externalId = "externalId";
    String bucket = "bucket";
    String warehouseKeyPrefix = "path/to/warehouse";

    RealmConfig sessionTagsEnabledConfig =
        new RealmConfigImpl(
            new PolarisConfigurationStore() {
              @SuppressWarnings("unchecked")
              @Override
              public String getConfiguration(@Nonnull RealmContext ctx, String configName) {
                if (configName.equals(
                    FeatureConfiguration.INCLUDE_SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL.key())) {
                  return "true";
                }
                return null;
              }
            },
            () -> "realm");

    ArgumentCaptor<AssumeRoleRequest> requestCaptor =
        ArgumentCaptor.forClass(AssumeRoleRequest.class);
    Mockito.when(stsClient.assumeRole(requestCaptor.capture())).thenReturn(ASSUME_ROLE_RESPONSE);

    // Only provide catalog name, no namespace/table
    CredentialVendingContext context =
        CredentialVendingContext.builder().catalogName(Optional.of("test-catalog")).build();

    new AwsCredentialsStorageIntegration(
            AwsStorageConfigurationInfo.builder()
                .addAllowedLocation(s3Path(bucket, warehouseKeyPrefix))
                .roleARN(roleARN)
                .externalId(externalId)
                .build(),
            stsClient)
        .getSubscopedCreds(
            sessionTagsEnabledConfig,
            true,
            Set.of(s3Path(bucket, warehouseKeyPrefix)),
            Set.of(s3Path(bucket, warehouseKeyPrefix)),
            POLARIS_PRINCIPAL,
            Optional.empty(),
            context);

    AssumeRoleRequest capturedRequest = requestCaptor.getValue();
    // All 6 tags are always included; missing values use "unknown" placeholder
    Assertions.assertThat(capturedRequest.tags()).hasSize(6);
    Assertions.assertThat(capturedRequest.tags())
        .anyMatch(tag -> tag.key().equals("polaris:catalog") && tag.value().equals("test-catalog"));
    Assertions.assertThat(capturedRequest.tags())
        .anyMatch(
            tag -> tag.key().equals("polaris:principal") && tag.value().equals("test-principal"));
    // Absent values should be "unknown"
    Assertions.assertThat(capturedRequest.tags())
        .anyMatch(tag -> tag.key().equals("polaris:namespace") && tag.value().equals("unknown"));
    Assertions.assertThat(capturedRequest.tags())
        .anyMatch(tag -> tag.key().equals("polaris:table") && tag.value().equals("unknown"));
    Assertions.assertThat(capturedRequest.tags())
        .anyMatch(tag -> tag.key().equals("polaris:roles") && tag.value().equals("unknown"));
    Assertions.assertThat(capturedRequest.tags())
        .anyMatch(tag -> tag.key().equals("polaris:trace_id") && tag.value().equals("unknown"));
  }

  @Test
  public void testSessionTagsWithLongValues() {
    StsClient stsClient = Mockito.mock(StsClient.class);
    String roleARN = "arn:aws:iam::012345678901:role/jdoe";
    String externalId = "externalId";
    String bucket = "bucket";
    String warehouseKeyPrefix = "path/to/warehouse";

    RealmConfig sessionTagsEnabledConfig =
        new RealmConfigImpl(
            new PolarisConfigurationStore() {
              @SuppressWarnings("unchecked")
              @Override
              public String getConfiguration(@Nonnull RealmContext ctx, String configName) {
                if (configName.equals(
                    FeatureConfiguration.INCLUDE_SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL.key())) {
                  return "true";
                }
                return null;
              }
            },
            () -> "realm");

    ArgumentCaptor<AssumeRoleRequest> requestCaptor =
        ArgumentCaptor.forClass(AssumeRoleRequest.class);
    Mockito.when(stsClient.assumeRole(requestCaptor.capture())).thenReturn(ASSUME_ROLE_RESPONSE);

    // Create context with very long namespace (over 256 chars)
    String longNamespace = "db." + "a".repeat(300) + ".schema";
    CredentialVendingContext context =
        CredentialVendingContext.builder()
            .catalogName(Optional.of("test-catalog"))
            .namespace(Optional.of(longNamespace))
            .build();

    new AwsCredentialsStorageIntegration(
            AwsStorageConfigurationInfo.builder()
                .addAllowedLocation(s3Path(bucket, warehouseKeyPrefix))
                .roleARN(roleARN)
                .externalId(externalId)
                .build(),
            stsClient)
        .getSubscopedCreds(
            sessionTagsEnabledConfig,
            true,
            Set.of(s3Path(bucket, warehouseKeyPrefix)),
            Set.of(s3Path(bucket, warehouseKeyPrefix)),
            POLARIS_PRINCIPAL,
            Optional.empty(),
            context);

    AssumeRoleRequest capturedRequest = requestCaptor.getValue();
    // Verify namespace tag is truncated to 256 characters
    Assertions.assertThat(capturedRequest.tags())
        .anyMatch(
            tag ->
                tag.key().equals("polaris:namespace")
                    && tag.value().length() == 256
                    && tag.value().startsWith("db."));
  }

  @Test
  public void testSessionTagsWithEmptyContext() {
    StsClient stsClient = Mockito.mock(StsClient.class);
    String roleARN = "arn:aws:iam::012345678901:role/jdoe";
    String externalId = "externalId";
    String bucket = "bucket";
    String warehouseKeyPrefix = "path/to/warehouse";

    RealmConfig sessionTagsEnabledConfig =
        new RealmConfigImpl(
            new PolarisConfigurationStore() {
              @SuppressWarnings("unchecked")
              @Override
              public String getConfiguration(@Nonnull RealmContext ctx, String configName) {
                if (configName.equals(
                    FeatureConfiguration.INCLUDE_SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL.key())) {
                  return "true";
                }
                return null;
              }
            },
            () -> "realm");

    ArgumentCaptor<AssumeRoleRequest> requestCaptor =
        ArgumentCaptor.forClass(AssumeRoleRequest.class);
    Mockito.when(stsClient.assumeRole(requestCaptor.capture())).thenReturn(ASSUME_ROLE_RESPONSE);

    // Use empty context
    new AwsCredentialsStorageIntegration(
            AwsStorageConfigurationInfo.builder()
                .addAllowedLocation(s3Path(bucket, warehouseKeyPrefix))
                .roleARN(roleARN)
                .externalId(externalId)
                .build(),
            stsClient)
        .getSubscopedCreds(
            sessionTagsEnabledConfig,
            true,
            Set.of(s3Path(bucket, warehouseKeyPrefix)),
            Set.of(s3Path(bucket, warehouseKeyPrefix)),
            POLARIS_PRINCIPAL,
            Optional.empty(),
            CredentialVendingContext.empty());

    AssumeRoleRequest capturedRequest = requestCaptor.getValue();
    // All 6 tags are always included; missing values use "unknown" placeholder
    Assertions.assertThat(capturedRequest.tags()).hasSize(6);
    Assertions.assertThat(capturedRequest.tags())
        .anyMatch(
            tag -> tag.key().equals("polaris:principal") && tag.value().equals("test-principal"));
    // All context tags should be "unknown" when context is empty
    Assertions.assertThat(capturedRequest.tags())
        .anyMatch(tag -> tag.key().equals("polaris:catalog") && tag.value().equals("unknown"));
    Assertions.assertThat(capturedRequest.tags())
        .anyMatch(tag -> tag.key().equals("polaris:namespace") && tag.value().equals("unknown"));
    Assertions.assertThat(capturedRequest.tags())
        .anyMatch(tag -> tag.key().equals("polaris:table") && tag.value().equals("unknown"));
    Assertions.assertThat(capturedRequest.tags())
        .anyMatch(tag -> tag.key().equals("polaris:roles") && tag.value().equals("unknown"));
    Assertions.assertThat(capturedRequest.tags())
        .anyMatch(tag -> tag.key().equals("polaris:trace_id") && tag.value().equals("unknown"));
  }

  /**
   * Tests graceful error handling when STS throws an exception due to missing sts:TagSession
   * permission. When the IAM role's trust policy doesn't allow sts:TagSession, the assumeRole call
   * should fail and the exception should be propagated appropriately.
   *
   * <p>NOTE: Full integration tests with LocalStack or real AWS to verify sts:TagSession permission
   * behavior are recommended but out of scope for unit tests.
   */
  @Test
  public void testSessionTagsAccessDeniedGracefulHandling() {
    StsClient stsClient = Mockito.mock(StsClient.class);
    String roleARN = "arn:aws:iam::012345678901:role/jdoe";
    String externalId = "externalId";
    String bucket = "bucket";
    String warehouseKeyPrefix = "path/to/warehouse";

    RealmConfig sessionTagsEnabledConfig =
        new RealmConfigImpl(
            new PolarisConfigurationStore() {
              @SuppressWarnings("unchecked")
              @Override
              public String getConfiguration(@Nonnull RealmContext ctx, String configName) {
                if (configName.equals(
                    FeatureConfiguration.INCLUDE_SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL.key())) {
                  return "true";
                }
                return null;
              }
            },
            () -> "realm");

    // Simulate STS throwing AccessDeniedException when sts:TagSession is not allowed
    // In AWS SDK v2, this is represented as StsException with error code "AccessDenied"
    StsException accessDeniedException =
        (StsException)
            StsException.builder()
                .message(
                    "User: arn:aws:iam::012345678901:user/test is not authorized to perform: "
                        + "sts:TagSession on resource: arn:aws:iam::012345678901:role/jdoe")
                .awsErrorDetails(
                    AwsErrorDetails.builder()
                        .errorCode("AccessDenied")
                        .errorMessage("Not authorized to perform sts:TagSession")
                        .serviceName("STS")
                        .build())
                .statusCode(403)
                .build();

    Mockito.when(stsClient.assumeRole(Mockito.any(AssumeRoleRequest.class)))
        .thenThrow(accessDeniedException);

    CredentialVendingContext context =
        CredentialVendingContext.builder()
            .catalogName(Optional.of("test-catalog"))
            .namespace(Optional.of("test-namespace"))
            .tableName(Optional.of("test-table"))
            .build();

    // Verify that the StsException is thrown (not swallowed) when sts:TagSession is denied
    Assertions.assertThatThrownBy(
            () ->
                new AwsCredentialsStorageIntegration(
                        AwsStorageConfigurationInfo.builder()
                            .addAllowedLocation(s3Path(bucket, warehouseKeyPrefix))
                            .roleARN(roleARN)
                            .externalId(externalId)
                            .build(),
                        stsClient)
                    .getSubscopedCreds(
                        sessionTagsEnabledConfig,
                        true,
                        Set.of(s3Path(bucket, warehouseKeyPrefix)),
                        Set.of(s3Path(bucket, warehouseKeyPrefix)),
                        POLARIS_PRINCIPAL,
                        Optional.empty(),
                        context))
        .isInstanceOf(software.amazon.awssdk.services.sts.model.StsException.class)
        .hasMessageContaining("sts:TagSession");
  }
}
