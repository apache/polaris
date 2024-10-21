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
package org.apache.polaris.core.storage.s3compatible;

import static org.apache.polaris.core.PolarisConfiguration.STORAGE_CREDENTIAL_DURATION_SECONDS;

import jakarta.annotation.Nonnull;
import java.net.URI;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.storage.InMemoryStorageIntegration;
import org.apache.polaris.core.storage.PolarisCredentialProperty;
import org.apache.polaris.core.storage.StorageUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.policybuilder.iam.IamConditionOperator;
import software.amazon.awssdk.policybuilder.iam.IamEffect;
import software.amazon.awssdk.policybuilder.iam.IamPolicy;
import software.amazon.awssdk.policybuilder.iam.IamResource;
import software.amazon.awssdk.policybuilder.iam.IamStatement;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;

/** S3 compatible implementation of PolarisStorageIntegration */
public class S3CompatibleCredentialsStorageIntegration
    extends InMemoryStorageIntegration<S3CompatibleStorageConfigurationInfo> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(S3CompatibleCredentialsStorageIntegration.class);
  private final PolarisConfigurationStore configurationStore;

  public S3CompatibleCredentialsStorageIntegration(PolarisConfigurationStore configurationStore) {
    super(configurationStore, S3CompatibleCredentialsStorageIntegration.class.getName());
    this.configurationStore = configurationStore;
  }

  /** {@inheritDoc} */
  @Override
  public EnumMap<PolarisCredentialProperty, String> getSubscopedCreds(
      @Nonnull RealmContext realmContext,
      @Nonnull PolarisDiagnostics diagnostics,
      @Nonnull S3CompatibleStorageConfigurationInfo storageConfig,
      boolean allowListOperation,
      @Nonnull Set<String> allowedReadLocations,
      @Nonnull Set<String> allowedWriteLocations) {

    StsClient stsClient;
    String caI = System.getenv(storageConfig.getS3CredentialsCatalogAccessKeyId());
    String caS = System.getenv(storageConfig.getS3CredentialsCatalogSecretAccessKey());

    EnumMap<PolarisCredentialProperty, String> propertiesMap =
        new EnumMap<>(PolarisCredentialProperty.class);
    propertiesMap.put(PolarisCredentialProperty.AWS_ENDPOINT, storageConfig.getS3Endpoint());
    propertiesMap.put(
        PolarisCredentialProperty.AWS_PATH_STYLE_ACCESS,
        storageConfig.getS3PathStyleAccess().toString());
    if (storageConfig.getS3Region() != null) {
      propertiesMap.put(PolarisCredentialProperty.CLIENT_REGION, storageConfig.getS3Region());
    }

    LOGGER.debug("S3Compatible - createStsClient()");
    try {
      StsClientBuilder stsBuilder = software.amazon.awssdk.services.sts.StsClient.builder();
      stsBuilder.endpointOverride(URI.create(storageConfig.getS3Endpoint()));
      if (caI != null && caS != null) {
        // else default provider build credentials from profile or standard AWS env var
        stsBuilder.credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(caI, caS)));
        LOGGER.debug(
            "S3Compatible - stsClient using keys from catalog settings - overiding default constructor");
      }
      stsClient = stsBuilder.build();
      LOGGER.debug("S3Compatible - stsClient successfully built");
      AssumeRoleResponse response =
          stsClient.assumeRole(
              AssumeRoleRequest.builder()
                  .roleSessionName("PolarisCredentialsSTS")
                  .roleArn(
                      (storageConfig.getS3RoleArn() == null) ? "" : storageConfig.getS3RoleArn())
                  .policy(
                      policyString(allowListOperation, allowedReadLocations, allowedWriteLocations)
                          .toJson())
                  .durationSeconds(
                      configurationStore.getConfiguration(
                          realmContext, STORAGE_CREDENTIAL_DURATION_SECONDS))
                  .build());
      propertiesMap.put(PolarisCredentialProperty.AWS_KEY_ID, response.credentials().accessKeyId());
      propertiesMap.put(
          PolarisCredentialProperty.AWS_SECRET_KEY, response.credentials().secretAccessKey());
      propertiesMap.put(PolarisCredentialProperty.AWS_TOKEN, response.credentials().sessionToken());
      LOGGER.debug(
          "S3Compatible - assumeRole - Token Expiration at : {}",
          response.credentials().expiration().toString());

    } catch (Exception e) {
      System.err.println("S3Compatible - stsClient - build failure : " + e.getMessage());
    }

    return propertiesMap;
  }

  /*
   * function from AwsCredentialsStorageIntegration but without roleArn parameter
   */
  private IamPolicy policyString(
      boolean allowList, Set<String> readLocations, Set<String> writeLocations) {
    IamPolicy.Builder policyBuilder = IamPolicy.builder();
    IamStatement.Builder allowGetObjectStatementBuilder =
        IamStatement.builder()
            .effect(IamEffect.ALLOW)
            .addAction("s3:GetObject")
            .addAction("s3:GetObjectVersion");
    Map<String, IamStatement.Builder> bucketListStatementBuilder = new HashMap<>();
    Map<String, IamStatement.Builder> bucketGetLocationStatementBuilder = new HashMap<>();

    String arnPrefix = "arn:aws:s3:::";
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

  /* function from AwsCredentialsStorageIntegration */
  private static @Nonnull String parseS3Path(URI uri) {
    String bucket = StorageUtil.getBucket(uri);
    String path = trimLeadingSlash(uri.getPath());
    return String.join("/", bucket, path);
  }

  /* function from AwsCredentialsStorageIntegration */
  private static @Nonnull String trimLeadingSlash(String path) {
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    return path;
  }
}
