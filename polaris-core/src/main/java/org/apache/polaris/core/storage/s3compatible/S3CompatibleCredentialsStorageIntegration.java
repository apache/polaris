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
import static org.apache.polaris.core.PolarisConfiguration.loadConfig;

import jakarta.ws.rs.NotAuthorizedException;
import java.net.URI;
import java.util.EnumMap;
import java.util.Set;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.storage.InMemoryStorageIntegration;
import org.apache.polaris.core.storage.PolarisCredentialProperty;
import org.apache.polaris.core.storage.StorageUtil;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.profiles.ProfileFileSupplier;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;

/** S3 compatible implementation of PolarisStorageIntegration */
public class S3CompatibleCredentialsStorageIntegration
    extends InMemoryStorageIntegration<S3CompatibleStorageConfigurationInfo> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(S3CompatibleCredentialsStorageIntegration.class);

  public S3CompatibleCredentialsStorageIntegration() {
    super(S3CompatibleCredentialsStorageIntegration.class.getName());
  }

  @Override
  public EnumMap<PolarisCredentialProperty, String> getSubscopedCreds(
      @NotNull PolarisDiagnostics diagnostics,
      @NotNull S3CompatibleStorageConfigurationInfo storageConfig,
      boolean allowListOperation,
      @NotNull Set<String> allowedReadLocations,
      @NotNull Set<String> allowedWriteLocations) {

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
    StsClientBuilder stsBuilder = software.amazon.awssdk.services.sts.StsClient.builder();
    stsBuilder.endpointOverride(URI.create(storageConfig.getS3Endpoint()));
    if (storageConfig.getS3ProfileName() != null) {
      stsBuilder.credentialsProvider(
          ProfileCredentialsProvider.builder()
              .profileFile(ProfileFileSupplier.defaultSupplier())
              .profileName(storageConfig.getS3ProfileName())
              .build());
      LOGGER.debug("S3Compatible - stsClient using profile from catalog settings");
    } else if (caI != null && caS != null) {
      stsBuilder.credentialsProvider(
          StaticCredentialsProvider.create(AwsBasicCredentials.create(caI, caS)));
      LOGGER.debug("S3Compatible - stsClient using keys from catalog settings");
    }
    try (StsClient stsClient = stsBuilder.build()) {
      LOGGER.debug("S3Compatible - stsClient successfully built");
      AssumeRoleResponse response =
          stsClient.assumeRole(
              AssumeRoleRequest.builder()
                  .roleSessionName("PolarisCredentialsSTS")
                  .roleArn(storageConfig.getS3RoleArn())
                  .policy(
                      StorageUtil.policyString(
                              storageConfig.getS3RoleArn(),
                              allowListOperation,
                              allowedReadLocations,
                              allowedWriteLocations)
                          .toJson())
                  .durationSeconds(loadConfig(STORAGE_CREDENTIAL_DURATION_SECONDS))
                  .build());

      propertiesMap.put(AWS_KEY_ID, response.credentials().accessKeyId());
      propertiesMap.put(AWS_SECRET_KEY, response.credentials().secretAccessKey());
      propertiesMap.put(AWS_TOKEN, response.credentials().sessionToken());
      LOGGER.debug(
          "S3Compatible - assumeRole - Obtained token expiration : {}",
          response.credentials().expiration().toString());
    } catch (Exception e) {
      throw new NotAuthorizedException(
          "Unable to build S3 Security Token Service client", e);
    }

    return propertiesMap;
  }
}
