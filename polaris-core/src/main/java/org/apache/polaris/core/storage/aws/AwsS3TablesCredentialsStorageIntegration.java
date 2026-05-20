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

import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.InMemoryStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.policybuilder.iam.IamEffect;
import software.amazon.awssdk.policybuilder.iam.IamPolicy;
import software.amazon.awssdk.policybuilder.iam.IamResource;
import software.amazon.awssdk.policybuilder.iam.IamStatement;

/**
 * Credential vendor for Amazon S3 Tables. Generates IAM session policies using {@code s3tables:*}
 * actions scoped to table-level ARNs, rather than the path-based {@code s3:*} policies used by
 * {@link AwsCredentialsStorageIntegration}.
 */
public class AwsS3TablesCredentialsStorageIntegration
    extends InMemoryStorageIntegration<AwsS3TablesStorageConfigurationInfo> {

  // S3 Tables IAM actions for read operations
  public static final String ACTION_GET_TABLE_DATA = "s3tables:GetTableData";
  public static final String ACTION_GET_TABLE_METADATA_LOCATION =
      "s3tables:GetTableMetadataLocation";

  // S3 Tables IAM actions for write operations
  public static final String ACTION_UPDATE_TABLE_METADATA_LOCATION =
      "s3tables:UpdateTableMetadataLocation";
  public static final String ACTION_PUT_TABLE_DATA = "s3tables:PutTableData";

  private final StsClientProvider stsClientProvider;
  private final Optional<AwsCredentialsProvider> credentialsProvider;

  public AwsS3TablesCredentialsStorageIntegration(
      AwsS3TablesStorageConfigurationInfo config,
      StsClientProvider stsClientProvider,
      Optional<AwsCredentialsProvider> credentialsProvider) {
    super(config, AwsS3TablesCredentialsStorageIntegration.class.getName());
    this.stsClientProvider = stsClientProvider;
    this.credentialsProvider = credentialsProvider;
  }

  @Override
  public StorageAccessConfig getSubscopedCreds(
      @Nonnull RealmConfig realmConfig,
      boolean allowListOperation,
      @Nonnull Set<String> allowedReadLocations,
      @Nonnull Set<String> allowedWriteLocations,
      @Nonnull PolarisPrincipal polarisPrincipal,
      Optional<String> refreshCredentialsEndpoint,
      @Nonnull CredentialVendingContext credentialVendingContext) {

    AwsS3TablesStorageConfigurationInfo storageConfig = config();
    String region = storageConfig.getRegion();
    StorageAccessConfig.Builder accessConfig = StorageAccessConfig.builder();

    // Generate s3tables:* session policy
    IamPolicy policy =
        buildS3TablesPolicy(storageConfig, allowedReadLocations, allowedWriteLocations);

    AwsStsUtil.assumeRoleAndPopulateConfig(
        accessConfig,
        realmConfig,
        storageConfig.getRoleARN(),
        storageConfig.getExternalId(),
        storageConfig.getStsEndpointUri(),
        region,
        policy.toJson(),
        "PolarisAwsS3TablesCredentialsStorageIntegration",
        polarisPrincipal,
        credentialVendingContext,
        credentialsProvider,
        stsClientProvider,
        refreshCredentialsEndpoint);

    return accessConfig.build();
  }

  /**
   * Builds an IAM policy with s3tables:* actions scoped to the provided ARN resources.
   *
   * <p>Read actions ({@code s3tables:GetTableData}, {@code s3tables:GetTableMetadataLocation}) are
   * scoped to all read and write ARNs. Write actions ({@code s3tables:UpdateTableMetadataLocation},
   * {@code s3tables:PutTableData}) are scoped to write ARNs only.
   */
  private IamPolicy buildS3TablesPolicy(
      AwsS3TablesStorageConfigurationInfo storageConfig,
      Set<String> readLocations,
      Set<String> writeLocations) {
    IamPolicy.Builder policyBuilder = IamPolicy.builder();

    // Read statement scoped to all ARNs (read + write)
    IamStatement.Builder readStatement =
        IamStatement.builder()
            .effect(IamEffect.ALLOW)
            .addAction(ACTION_GET_TABLE_DATA)
            .addAction(ACTION_GET_TABLE_METADATA_LOCATION);

    Stream.concat(readLocations.stream(), writeLocations.stream())
        .distinct()
        .forEach(arn -> readStatement.addResource(IamResource.create(arn)));

    policyBuilder.addStatement(readStatement.build());

    // Write statement scoped to write ARNs only
    boolean canWrite = !writeLocations.isEmpty();
    if (canWrite) {
      IamStatement.Builder writeStatement =
          IamStatement.builder()
              .effect(IamEffect.ALLOW)
              .addAction(ACTION_UPDATE_TABLE_METADATA_LOCATION)
              .addAction(ACTION_PUT_TABLE_DATA);

      writeLocations.forEach(arn -> writeStatement.addResource(IamResource.create(arn)));
      policyBuilder.addStatement(writeStatement.build());
    }

    // KMS policy if configured
    AwsCredentialsStorageIntegration.addKmsKeyPolicy(
        storageConfig.getCurrentKmsKey(),
        storageConfig.getAllowedKmsKeys(),
        policyBuilder,
        canWrite,
        storageConfig.getRegion(),
        null);

    return policyBuilder.build();
  }

  @Override
  public @Nonnull Map<String, Map<PolarisStorageActions, ValidationResult>>
      validateAccessToLocations(
          @Nonnull RealmConfig realmConfig,
          @Nonnull AwsS3TablesStorageConfigurationInfo storageConfig,
          @Nonnull Set<PolarisStorageActions> actions,
          @Nonnull Set<String> locations) {
    // TODO(https://github.com/apache/polaris/issues/4302): Implement S3 Tables location
    // validation. This requires calling s3tables:GetTableBucket or similar to verify that the
    // configured role can actually access the table bucket ARN.
    return Map.of();
  }
}
