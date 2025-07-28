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
package org.apache.polaris.core.storage.aws;

import static org.apache.polaris.core.config.FeatureConfiguration.KMS_SUPPORT_LEVEL_S3;
import static org.apache.polaris.core.config.FeatureConfiguration.STORAGE_CREDENTIAL_DURATION_SECONDS;

import jakarta.annotation.Nonnull;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.storage.AccessConfig;
import org.apache.polaris.core.storage.InMemoryStorageIntegration;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.StorageUtil;
import org.apache.polaris.core.storage.aws.StsClientProvider.StsDestination;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.policybuilder.iam.IamConditionOperator;
import software.amazon.awssdk.policybuilder.iam.IamEffect;
import software.amazon.awssdk.policybuilder.iam.IamPolicy;
import software.amazon.awssdk.policybuilder.iam.IamResource;
import software.amazon.awssdk.policybuilder.iam.IamStatement;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;

/** Credential vendor that supports generating */
public class AwsCredentialsStorageIntegration
    extends InMemoryStorageIntegration<AwsStorageConfigurationInfo> {
  private final StsClientProvider stsClientProvider;
  private final Optional<AwsCredentialsProvider> credentialsProvider;

  public AwsCredentialsStorageIntegration(StsClient fixedClient) {
    this((destination) -> fixedClient);
  }

  public AwsCredentialsStorageIntegration(StsClientProvider stsClientProvider) {
    this(stsClientProvider, Optional.empty());
  }

  public AwsCredentialsStorageIntegration(
      StsClientProvider stsClientProvider, Optional<AwsCredentialsProvider> credentialsProvider) {
    super(AwsCredentialsStorageIntegration.class.getName());
    this.stsClientProvider = stsClientProvider;
    this.credentialsProvider = credentialsProvider;
  }

  /** {@inheritDoc} */
  @Override
  public AccessConfig getSubscopedCreds(
      @Nonnull CallContext callContext,
      @Nonnull AwsStorageConfigurationInfo storageConfig,
      boolean allowListOperation,
      @Nonnull Set<String> allowedReadLocations,
      @Nonnull Set<String> allowedWriteLocations) {
    int storageCredentialDurationSeconds =
        callContext.getRealmConfig().getConfig(STORAGE_CREDENTIAL_DURATION_SECONDS);
    AssumeRoleRequest.Builder request =
        AssumeRoleRequest.builder()
            .externalId(storageConfig.getExternalId())
            .roleArn(storageConfig.getRoleARN())
            .roleSessionName("PolarisAwsCredentialsStorageIntegration")
            .policy(
                policyString(
                        storageConfig,
                        allowListOperation,
                        allowedReadLocations,
                        allowedWriteLocations,
                        callContext)
                    .toJson())
            .durationSeconds(storageCredentialDurationSeconds);
    credentialsProvider.ifPresent(
        cp -> request.overrideConfiguration(b -> b.credentialsProvider(cp)));

    String region = storageConfig.getRegion();
    @SuppressWarnings("resource")
    // Note: stsClientProvider returns "thin" clients that do not need closing
    StsClient stsClient =
        stsClientProvider.stsClient(StsDestination.of(storageConfig.getStsEndpointUri(), region));

    AssumeRoleResponse response = stsClient.assumeRole(request.build());
    AccessConfig.Builder accessConfig = AccessConfig.builder();
    accessConfig.put(StorageAccessProperty.AWS_KEY_ID, response.credentials().accessKeyId());
    accessConfig.put(
        StorageAccessProperty.AWS_SECRET_KEY, response.credentials().secretAccessKey());
    accessConfig.put(StorageAccessProperty.AWS_TOKEN, response.credentials().sessionToken());
    Optional.ofNullable(response.credentials().expiration())
        .ifPresent(
            i -> {
              accessConfig.put(
                  StorageAccessProperty.EXPIRATION_TIME, String.valueOf(i.toEpochMilli()));
              accessConfig.put(
                  StorageAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS,
                  String.valueOf(i.toEpochMilli()));
            });

    if (region != null) {
      accessConfig.put(StorageAccessProperty.CLIENT_REGION, region);
    }

    URI endpointUri = storageConfig.getEndpointUri();
    if (endpointUri != null) {
      accessConfig.put(StorageAccessProperty.AWS_ENDPOINT, endpointUri.toString());
    }

    if (Boolean.TRUE.equals(storageConfig.getPathStyleAccess())) {
      accessConfig.put(StorageAccessProperty.AWS_PATH_STYLE_ACCESS, Boolean.TRUE.toString());
    }

    if (storageConfig.getAwsPartition().equals("aws-us-gov") && region == null) {
      throw new IllegalArgumentException(
          String.format(
              "AWS region must be set when using partition %s", storageConfig.getAwsPartition()));
    }

    return accessConfig.build();
  }

  /**
   * generate an IamPolicy from the input readLocations and writeLocations, optionally with list
   * support. Credentials will be scoped to exactly the resources provided. If read and write
   * locations are empty, a non-empty policy will be generated that grants GetObject and optionally
   * ListBucket privileges with no resources. This prevents us from sending an empty policy to AWS
   * and just assuming the role with full privileges.
   */
  private IamPolicy policyString(
      AwsStorageConfigurationInfo awsStorageConfigurationInfo,
      boolean allowList,
      Set<String> readLocations,
      Set<String> writeLocations,
      CallContext callContext) {
    IamPolicy.Builder policyBuilder = IamPolicy.builder();
    IamStatement.Builder allowGetObjectStatementBuilder =
        IamStatement.builder()
            .effect(IamEffect.ALLOW)
            .addAction("s3:GetObject")
            .addAction("s3:GetObjectVersion");
    Map<String, IamStatement.Builder> bucketListStatementBuilder = new HashMap<>();
    Map<String, IamStatement.Builder> bucketGetLocationStatementBuilder = new HashMap<>();

    String roleARN = awsStorageConfigurationInfo.getRoleARN();
    String arnPrefix = getArnPrefixFor(roleARN);
    String region = awsStorageConfigurationInfo.getRegion();
    String awsAccountId = awsStorageConfigurationInfo.getAwsAccountId();
    Stream.concat(readLocations.stream(), writeLocations.stream())
        .distinct()
        .forEach(
            location -> {
              URI uri = URI.create(location);
              allowGetObjectStatementBuilder.addResource(
                  IamResource.create(
                      arnPrefix + StorageUtil.concatFilePrefixes(parseS3Path(uri), "*", "/")));
              final var bucket = arnPrefix + StorageUtil.getBucket(uri);
              if (allowList) {
                bucketListStatementBuilder
                    .computeIfAbsent(
                        bucket,
                        (String key) ->
                            IamStatement.builder()
                                .effect(IamEffect.ALLOW)
                                .addAction("s3:ListBucket")
                                .addResource(key))
                    .addCondition(
                        IamConditionOperator.STRING_LIKE,
                        "s3:prefix",
                        StorageUtil.concatFilePrefixes(trimLeadingSlash(uri.getPath()), "*", "/"));
              }
              bucketGetLocationStatementBuilder.computeIfAbsent(
                  bucket,
                  key ->
                      IamStatement.builder()
                          .effect(IamEffect.ALLOW)
                          .addAction("s3:GetBucketLocation")
                          .addResource(key));
            });

    if (!writeLocations.isEmpty()) {
      IamStatement.Builder allowPutObjectStatementBuilder =
          IamStatement.builder()
              .effect(IamEffect.ALLOW)
              .addAction("s3:PutObject")
              .addAction("s3:DeleteObject");
      writeLocations.forEach(
          location -> {
            URI uri = URI.create(location);
            allowPutObjectStatementBuilder.addResource(
                IamResource.create(
                    arnPrefix + StorageUtil.concatFilePrefixes(parseS3Path(uri), "*", "/")));
          });
      policyBuilder.addStatement(allowPutObjectStatementBuilder.build());
    }
    if (!bucketListStatementBuilder.isEmpty()) {
      bucketListStatementBuilder
          .values()
          .forEach(statementBuilder -> policyBuilder.addStatement(statementBuilder.build()));
    } else if (allowList) {
      // add list privilege with 0 resources
      policyBuilder.addStatement(
          IamStatement.builder().effect(IamEffect.ALLOW).addAction("s3:ListBucket").build());
    }

    bucketGetLocationStatementBuilder
        .values()
        .forEach(statementBuilder -> policyBuilder.addStatement(statementBuilder.build()));

    policyBuilder.addStatement(allowGetObjectStatementBuilder.build());

    if (isKMSSupported(callContext)) {
      policyBuilder.addStatement(
          IamStatement.builder()
              .effect(IamEffect.ALLOW)
              .addAction("kms:GenerateDataKey")
              .addAction("kms:Decrypt")
              .addAction("kms:DescribeKey")
              .addResource(getKMSArnPrefix(roleARN) + region + ":" + awsAccountId + ":key/*")
              .addCondition(IamConditionOperator.STRING_EQUALS, "aws:PrincipalArn", roleARN)
              .addCondition(
                  IamConditionOperator.STRING_LIKE,
                  "kms:EncryptionContext:aws:s3:arn",
                  getArnPrefixFor(roleARN)
                      + StorageUtil.getBucket(
                          URI.create(awsStorageConfigurationInfo.getAllowedLocations().get(0)))
                      + "/*")
              .addCondition(
                  IamConditionOperator.STRING_EQUALS,
                  "kms:ViaService",
                  getS3Endpoint(roleARN, region))
              .build());
    }
    return policyBuilder.build();
  }

  private String getArnPrefixFor(String roleArn) {
    if (roleArn.contains("aws-cn")) {
      return "arn:aws-cn:s3:::";
    } else if (roleArn.contains("aws-us-gov")) {
      return "arn:aws-us-gov:s3:::";
    } else {
      return "arn:aws:s3:::";
    }
  }

  private static String getKMSArnPrefix(String roleArn) {
    if (roleArn.contains("aws-cn")) {
      return "arn:aws-cn:kms:";
    } else if (roleArn.contains("aws-us-gov")) {
      return "arn:aws-us-gov:kms:";
    } else {
      return "arn:aws:kms:";
    }
  }

  private static String getS3Endpoint(String roleArn, String region) {
    if (roleArn.contains("aws-cn")) {
      return "s3." + region + ".amazonaws.com.cn";
    } else {
      return "s3." + region + ".amazonaws.com";
    }
  }

  private static @Nonnull String parseS3Path(URI uri) {
    String bucket = StorageUtil.getBucket(uri);
    String path = trimLeadingSlash(uri.getPath());
    return String.join("/", bucket, path);
  }

  private static @Nonnull String trimLeadingSlash(String path) {
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    return path;
  }

  private boolean isKMSSupported(CallContext callContext) {
    return !callContext
        .getRealmConfig()
        .getConfig(KMS_SUPPORT_LEVEL_S3)
        .equals(FeatureConfiguration.KmsSupportLevel.NONE);
  }
}
