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

import static org.apache.polaris.core.config.FeatureConfiguration.STORAGE_CREDENTIAL_DURATION_SECONDS;

import jakarta.annotation.Nonnull;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.InMemoryStorageIntegration;
import org.apache.polaris.core.storage.StorageAccessConfig;
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

  public AwsCredentialsStorageIntegration(
      AwsStorageConfigurationInfo config, StsClient fixedClient) {
    this(config, (destination) -> fixedClient);
  }

  public AwsCredentialsStorageIntegration(
      AwsStorageConfigurationInfo config, StsClientProvider stsClientProvider) {
    this(config, stsClientProvider, Optional.empty());
  }

  public AwsCredentialsStorageIntegration(
      AwsStorageConfigurationInfo config,
      StsClientProvider stsClientProvider,
      Optional<AwsCredentialsProvider> credentialsProvider) {
    super(config, AwsCredentialsStorageIntegration.class.getName());
    this.stsClientProvider = stsClientProvider;
    this.credentialsProvider = credentialsProvider;
  }

  /** {@inheritDoc} */
  @Override
  public StorageAccessConfig getSubscopedCreds(
      @Nonnull RealmConfig realmConfig,
      boolean allowListOperation,
      @Nonnull Set<String> allowedReadLocations,
      @Nonnull Set<String> allowedWriteLocations,
      Optional<String> refreshCredentialsEndpoint) {
    int storageCredentialDurationSeconds =
        realmConfig.getConfig(STORAGE_CREDENTIAL_DURATION_SECONDS);
    AwsStorageConfigurationInfo storageConfig = config();
    String region = storageConfig.getRegion();
    StorageAccessConfig.Builder accessConfig = StorageAccessConfig.builder();

    if (shouldUseSts(storageConfig)) {
      AssumeRoleRequest.Builder request =
          AssumeRoleRequest.builder()
              .externalId(storageConfig.getExternalId())
              .roleArn(storageConfig.getRoleARN())
              .roleSessionName("PolarisAwsCredentialsStorageIntegration")
              .policy(
                  policyString(
                          storageConfig.getAwsPartition(),
                          allowListOperation,
                          allowedReadLocations,
                          allowedWriteLocations)
                      .toJson())
              .durationSeconds(storageCredentialDurationSeconds);
      credentialsProvider.ifPresent(
          cp -> request.overrideConfiguration(b -> b.credentialsProvider(cp)));

      @SuppressWarnings("resource")
      // Note: stsClientProvider returns "thin" clients that do not need closing
      StsClient stsClient =
          stsClientProvider.stsClient(StsDestination.of(storageConfig.getStsEndpointUri(), region));

      AssumeRoleResponse response = stsClient.assumeRole(request.build());
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
    }

    if (region != null) {
      accessConfig.put(StorageAccessProperty.CLIENT_REGION, region);
    }

    refreshCredentialsEndpoint.ifPresent(
        endpoint -> {
          accessConfig.put(StorageAccessProperty.AWS_REFRESH_CREDENTIALS_ENDPOINT, endpoint);
        });

    URI endpointUri = storageConfig.getEndpointUri();
    if (endpointUri != null) {
      accessConfig.put(StorageAccessProperty.AWS_ENDPOINT, endpointUri.toString());
    }
    URI internalEndpointUri = storageConfig.getInternalEndpointUri();
    if (internalEndpointUri != null) {
      accessConfig.putInternalProperty(
          StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), internalEndpointUri.toString());
    }

    if (Boolean.TRUE.equals(storageConfig.getPathStyleAccess())) {
      accessConfig.put(StorageAccessProperty.AWS_PATH_STYLE_ACCESS, Boolean.TRUE.toString());
    }

    if ("aws-us-gov".equals(storageConfig.getAwsPartition()) && region == null) {
      throw new IllegalArgumentException(
          String.format(
              "AWS region must be set when using partition %s", storageConfig.getAwsPartition()));
    }

    return accessConfig.build();
  }

  private boolean shouldUseSts(AwsStorageConfigurationInfo storageConfig) {
    return !Boolean.TRUE.equals(storageConfig.getStsUnavailable());
  }

  /**
   * generate an IamPolicy from the input readLocations and writeLocations, optionally with list
   * support. Credentials will be scoped to exactly the resources provided. If read and write
   * locations are empty, a non-empty policy will be generated that grants GetObject and optionally
   * ListBucket privileges with no resources. This prevents us from sending an empty policy to AWS
   * and just assuming the role with full privileges.
   */
  // TODO - add KMS key access
  private IamPolicy policyString(
      String awsPartition,
      boolean allowList,
      Set<String> readLocations,
      Set<String> writeLocations) {
    IamPolicy.Builder policyBuilder = IamPolicy.builder();
    IamStatement.Builder allowGetObjectStatementBuilder =
        IamStatement.builder()
            .effect(IamEffect.ALLOW)
            .addAction("s3:GetObject")
            .addAction("s3:GetObjectVersion");
    Map<String, IamStatement.Builder> bucketListStatementBuilder = new HashMap<>();
    Map<String, IamStatement.Builder> bucketGetLocationStatementBuilder = new HashMap<>();

    String arnPrefix = arnPrefixForPartition(awsPartition);
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
    return policyBuilder.addStatement(allowGetObjectStatementBuilder.build()).build();
  }

  private static String arnPrefixForPartition(String awsPartition) {
    return String.format("arn:%s:s3:::", awsPartition != null ? awsPartition : "aws");
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
}
