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
import static org.apache.polaris.core.storage.aws.AwsSessionTagsBuilder.buildSessionTags;

import jakarta.annotation.Nonnull;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.InMemoryStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.aws.StsClientProvider.StsDestination;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.policybuilder.iam.IamEffect;
import software.amazon.awssdk.policybuilder.iam.IamPolicy;
import software.amazon.awssdk.policybuilder.iam.IamResource;
import software.amazon.awssdk.policybuilder.iam.IamStatement;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Tag;

/**
 * Credential vendor for Amazon S3 Tables. Generates IAM session policies using {@code s3tables:*}
 * actions scoped to table-level ARNs, rather than the path-based {@code s3:*} policies used by
 * {@link AwsCredentialsStorageIntegration}.
 */
public class AwsS3TablesCredentialsStorageIntegration
    extends InMemoryStorageIntegration<AwsS3TablesStorageConfigurationInfo> {

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
    int durationSeconds = realmConfig.getConfig(STORAGE_CREDENTIAL_DURATION_SECONDS);

    StorageAccessConfig.Builder accessConfig = StorageAccessConfig.builder();

    // Generate s3tables:* session policy
    IamPolicy policy =
        buildS3TablesPolicy(storageConfig, allowedReadLocations, allowedWriteLocations);

    // Role session name
    boolean includePrincipalName =
        realmConfig.getConfig(FeatureConfiguration.INCLUDE_PRINCIPAL_NAME_IN_SUBSCOPED_CREDENTIAL);
    String roleSessionName =
        includePrincipalName
            ? AwsRoleSessionNameSanitizer.sanitize("polaris-" + polarisPrincipal.getName())
            : "PolarisAwsS3TablesCredentialsStorageIntegration";

    AssumeRoleRequest.Builder request =
        AssumeRoleRequest.builder()
            .externalId(storageConfig.getExternalId())
            .roleArn(storageConfig.getRoleARN())
            .roleSessionName(roleSessionName)
            .policy(policy.toJson())
            .durationSeconds(durationSeconds);

    // Session tags support
    List<String> sessionTagFieldNames =
        realmConfig.getConfig(FeatureConfiguration.SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL);
    Set<SessionTagField> enabledSessionTagFields =
        sessionTagFieldNames.stream()
            .map(SessionTagField::fromConfigName)
            .flatMap(Optional::stream)
            .collect(Collectors.toCollection(() -> EnumSet.noneOf(SessionTagField.class)));

    if (!enabledSessionTagFields.isEmpty()) {
      List<Tag> sessionTags =
          buildSessionTags(
              polarisPrincipal.getName(), credentialVendingContext, enabledSessionTagFields);
      if (!sessionTags.isEmpty()) {
        request.tags(sessionTags);
        request.transitiveTagKeys(sessionTags.stream().map(Tag::key).collect(Collectors.toList()));
      }
    }

    credentialsProvider.ifPresent(
        cp -> request.overrideConfiguration(b -> b.credentialsProvider(cp)));

    @SuppressWarnings("resource")
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

    if (region != null) {
      accessConfig.put(StorageAccessProperty.CLIENT_REGION, region);
    }

    refreshCredentialsEndpoint.ifPresent(
        endpoint ->
            accessConfig.put(StorageAccessProperty.AWS_REFRESH_CREDENTIALS_ENDPOINT, endpoint));

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
            .addAction("s3tables:GetTableData")
            .addAction("s3tables:GetTableMetadataLocation");

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
              .addAction("s3tables:UpdateTableMetadataLocation")
              .addAction("s3tables:PutTableData");

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
    // No-op stub for S3 Tables — real validation deferred to a follow-up PR.
    return Map.of();
  }
}
