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
package org.apache.polaris.core.storage.s3;

import java.net.URI;
import java.util.EnumMap;
import java.util.Set;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.storage.InMemoryStorageIntegration;
import org.apache.polaris.core.storage.PolarisCredentialProperty;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;

/** Credential vendor that supports generating */
public class S3CredentialsStorageIntegration
    extends InMemoryStorageIntegration<S3StorageConfigurationInfo> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(S3CredentialsStorageIntegration.class);

  private StsClient stsClient;

  // Constructor
  public S3CredentialsStorageIntegration() {
    super(S3CredentialsStorageIntegration.class.getName());
  }

  public void createStsClient(S3StorageConfigurationInfo s3storageConfig) {

    LOGGER.debug("S3Compatible - createStsClient()");

    LOGGER.info(
        "S3Compatible - AWS STS endpoint is unique and different from the S3 Endpoint. AWS SDK need to be overided with dedicated Endpoint from S3Compatible, otherwise the AWS STS url is targeted");

    StsClientBuilder stsBuilder = software.amazon.awssdk.services.sts.StsClient.builder();

    stsBuilder.region(
        Region
            .US_WEST_1); // default region to avoid bug, because most (all?) S3 compatible softwares
    // do not care about regions
    stsBuilder.endpointOverride(URI.create(s3storageConfig.getS3Endpoint()));
    stsBuilder.credentialsProvider(
        StaticCredentialsProvider.create(
            AwsBasicCredentials.create(
                s3storageConfig.getS3CredentialsCatalogAccessKeyId(),
                s3storageConfig.getS3CredentialsCatalogSecretAccessKey())));

    this.stsClient = stsBuilder.build();
    LOGGER.debug("S3Compatible - stsClient successfully built");
  }

  /** {@inheritDoc} */
  @Override
  public EnumMap<PolarisCredentialProperty, String> getSubscopedCreds(
      @NotNull PolarisDiagnostics diagnostics,
      @NotNull S3StorageConfigurationInfo storageConfig,
      boolean allowListOperation,
      @NotNull Set<String> allowedReadLocations,
      @NotNull Set<String> allowedWriteLocations) {

    LOGGER.debug("S3Compatible - getSubscopedCreds - applying credential strategy");

    EnumMap<PolarisCredentialProperty, String> propertiesMap =
        new EnumMap<>(PolarisCredentialProperty.class);
    propertiesMap.put(PolarisCredentialProperty.AWS_ENDPOINT, storageConfig.getS3Endpoint());
    propertiesMap.put(
        PolarisCredentialProperty.AWS_PATH_STYLE_ACCESS,
        storageConfig.getS3PathStyleAccess().toString());

    switch (storageConfig.getCredsVendingStrategy()) {
      case KEYS_SAME_AS_CATALOG:
        propertiesMap.put(
            PolarisCredentialProperty.AWS_KEY_ID,
            storageConfig.getS3CredentialsCatalogAccessKeyId());
        propertiesMap.put(
            PolarisCredentialProperty.AWS_SECRET_KEY,
            storageConfig.getS3CredentialsCatalogSecretAccessKey());
        break;

      case KEYS_DEDICATED_TO_CLIENT:
        propertiesMap.put(
            PolarisCredentialProperty.AWS_KEY_ID,
            storageConfig.getS3CredentialsClientAccessKeyId());
        propertiesMap.put(
            PolarisCredentialProperty.AWS_SECRET_KEY,
            storageConfig.getS3CredentialsClientSecretAccessKey());
        break;

      case TOKEN_WITH_ASSUME_ROLE:
        if (this.stsClient == null) {
          createStsClient(storageConfig);
        }
        LOGGER.debug("S3Compatible - assumeRole !");
        AssumeRoleResponse response =
            stsClient.assumeRole(
                AssumeRoleRequest.builder().roleSessionName("PolarisCredentialsSTS").build());

        propertiesMap.put(
            PolarisCredentialProperty.AWS_KEY_ID, response.credentials().accessKeyId());
        propertiesMap.put(
            PolarisCredentialProperty.AWS_SECRET_KEY, response.credentials().secretAccessKey());
        propertiesMap.put(
            PolarisCredentialProperty.AWS_TOKEN, response.credentials().sessionToken());
        break;

        // @TODO implement the MinIO external OpenID Connect -
        // https://min.io/docs/minio/linux/developers/security-token-service.html?ref=docs-redirect#id1
        // case TOKEN_WITH_ASSUME_ROLE_WITH_WEB_IDENTITY:
        //   break;
    }

    return propertiesMap;
  }
}
