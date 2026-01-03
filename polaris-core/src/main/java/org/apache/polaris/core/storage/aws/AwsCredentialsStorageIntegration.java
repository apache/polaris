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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.InMemoryStorageIntegration;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.StorageUtil;
import org.apache.polaris.core.storage.aws.StsClientProvider.StsDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AwsCredentialsStorageIntegration.class);

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
      @Nonnull PolarisPrincipal polarisPrincipal,
      Optional<String> refreshCredentialsEndpoint) {
    int storageCredentialDurationSeconds =
        realmConfig.getConfig(STORAGE_CREDENTIAL_DURATION_SECONDS);
    AwsStorageConfigurationInfo storageConfig = config();
    String region = storageConfig.getRegion();
    String accountId = storageConfig.getAwsAccountId();
    StorageAccessConfig.Builder accessConfig =
        StorageAccessConfig.builder().supportsCredentialVending(true).supportsRemoteSigning(false);

    boolean includePrincipalNameInSubscopedCredential =
        realmConfig.getConfig(FeatureConfiguration.INCLUDE_PRINCIPAL_NAME_IN_SUBSCOPED_CREDENTIAL);

    String roleSessionName =
        includePrincipalNameInSubscopedCredential
            ? "polaris-" + polarisPrincipal.getName()
            : "PolarisAwsCredentialsStorageIntegration";
    String cappedRoleSessionName =
        roleSessionName.substring(0, Math.min(roleSessionName.length(), 64));

    if (shouldUseSts(storageConfig)) {
      AssumeRoleRequest.Builder request =
          AssumeRoleRequest.builder()
              .externalId(storageConfig.getExternalId())
              .roleArn(storageConfig.getRoleARN())
              .roleSessionName(cappedRoleSessionName)
              .policy(
                  policyString(
                          storageConfig,
                          allowListOperation,
                          allowedReadLocations,
                          allowedWriteLocations,
                          region,
                          accountId)
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

    addCommonProperties(region, accessConfig, storageConfig, refreshCredentialsEndpoint);

    return accessConfig.build();
  }

  public StorageAccessConfig getRemoteSigningAccessConfig(URI signerUri, String signerEndpoint) {

    AwsStorageConfigurationInfo storageConfig = config();
    StorageAccessConfig.Builder accessConfig =
        StorageAccessConfig.builder().supportsCredentialVending(false).supportsRemoteSigning(true);

    accessConfig.put(StorageAccessProperty.AWS_REMOTE_SIGNING_ENABLED, Boolean.TRUE.toString());
    accessConfig.put(StorageAccessProperty.AWS_REMOTE_SIGNER_URI, signerUri.toString());
    accessConfig.put(StorageAccessProperty.AWS_REMOTE_SIGNER_ENDPOINT, signerEndpoint);

    addCommonProperties(storageConfig.getRegion(), accessConfig, storageConfig, Optional.empty());

    return accessConfig.build();
  }

  private static void addCommonProperties(
      String region,
      StorageAccessConfig.Builder accessConfig,
      AwsStorageConfigurationInfo storageConfig,
      Optional<String> refreshCredentialsEndpoint) {
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
  private IamPolicy policyString(
      AwsStorageConfigurationInfo storageConfigurationInfo,
      boolean allowList,
      Set<String> readLocations,
      Set<String> writeLocations,
      String region,
      String accountId) {
    IamPolicy.Builder policyBuilder = IamPolicy.builder();
    IamStatement.Builder allowGetObjectStatementBuilder =
        IamStatement.builder()
            .effect(IamEffect.ALLOW)
            .addAction("s3:GetObject")
            .addAction("s3:GetObjectVersion");
    Map<String, IamStatement.Builder> bucketListStatementBuilder = new HashMap<>();
    Map<String, IamStatement.Builder> bucketGetLocationStatementBuilder = new HashMap<>();

    String arnPrefix = arnPrefixForPartition(storageConfigurationInfo.getAwsPartition());
    String currentKmsKey = storageConfigurationInfo.getCurrentKmsKey();
    List<String> allowedKmsKeys = storageConfigurationInfo.getAllowedKmsKeys();
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
      addKmsKeyPolicy(currentKmsKey, allowedKmsKeys, policyBuilder, true, region, accountId);
    } else {
      addKmsKeyPolicy(currentKmsKey, allowedKmsKeys, policyBuilder, false, region, accountId);
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

  private static void addKmsKeyPolicy(
      String kmsKeyArn,
      List<String> allowedKmsKeys,
      IamPolicy.Builder policyBuilder,
      boolean canWrite,
      String region,
      String accountId) {

    IamStatement.Builder allowKms = buildBaseKmsStatement(canWrite);
    boolean hasCurrentKey = kmsKeyArn != null;
    boolean hasAllowedKeys = hasAllowedKmsKeys(allowedKmsKeys);

    if (hasCurrentKey) {
      addKmsKeyResource(kmsKeyArn, allowKms);
    }

    if (hasAllowedKeys) {
      addAllowedKmsKeyResources(allowedKmsKeys, allowKms);
    }

    // Add KMS statement if we have any KMS key configuration
    if (hasCurrentKey || hasAllowedKeys) {
      policyBuilder.addStatement(allowKms.build());
    } else if (!canWrite) {
      // Only add wildcard KMS access for read-only operations when no specific keys are configured
      // this check is for minio because it doesn't have region or account id
      if (region != null && accountId != null) {
        addAllKeysResource(region, accountId, allowKms);
        policyBuilder.addStatement(allowKms.build());
      }
    }
  }

  private static IamStatement.Builder buildBaseKmsStatement(boolean canEncrypt) {
    IamStatement.Builder allowKms =
        IamStatement.builder()
            .effect(IamEffect.ALLOW)
            .addAction("kms:GenerateDataKeyWithoutPlaintext")
            .addAction("kms:DescribeKey")
            .addAction("kms:Decrypt")
            .addAction("kms:GenerateDataKey");

    if (canEncrypt) {
      allowKms.addAction("kms:Encrypt");
    }

    return allowKms;
  }

  private static void addKmsKeyResource(String kmsKeyArn, IamStatement.Builder allowKms) {
    if (kmsKeyArn != null) {
      LOGGER.debug("Adding KMS key policy for key {}", kmsKeyArn);
      allowKms.addResource(IamResource.create(kmsKeyArn));
    }
  }

  private static boolean hasAllowedKmsKeys(List<String> allowedKmsKeys) {
    return allowedKmsKeys != null && !allowedKmsKeys.isEmpty();
  }

  private static void addAllowedKmsKeyResources(
      List<String> allowedKmsKeys, IamStatement.Builder allowKms) {
    allowedKmsKeys.forEach(
        keyArn -> {
          LOGGER.debug("Adding allowed KMS key policy for key {}", keyArn);
          allowKms.addResource(IamResource.create(keyArn));
        });
  }

  private static void addAllKeysResource(
      String region, String accountId, IamStatement.Builder allowKms) {
    String allKeysArn = arnKeyAll(region, accountId);
    allowKms.addResource(IamResource.create(allKeysArn));
    LOGGER.debug("Adding KMS key policy for all keys in account {}", accountId);
  }

  private static String arnKeyAll(String region, String accountId) {
    return String.format("arn:aws:kms:%s:%s:key/*", region, accountId);
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
