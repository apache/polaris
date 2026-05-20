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

import jakarta.annotation.Nullable;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.aws.StsClientProvider.StsDestination;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Tag;

/** Shared STS assume-role utilities used by both S3 and S3 Tables credential integrations. */
public final class AwsStsUtil {

  private AwsStsUtil() {}

  /**
   * Assumes an IAM role via STS and populates the access config with the resulting temporary
   * credentials.
   */
  public static void assumeRoleAndPopulateConfig(
      StorageAccessConfig.Builder accessConfig,
      RealmConfig realmConfig,
      String roleArn,
      @Nullable String externalId,
      @Nullable URI stsEndpointUri,
      @Nullable String region,
      String policyJson,
      String defaultRoleSessionName,
      PolarisPrincipal polarisPrincipal,
      CredentialVendingContext credentialVendingContext,
      Optional<AwsCredentialsProvider> credentialsProvider,
      StsClientProvider stsClientProvider,
      Optional<String> refreshCredentialsEndpoint) {

    int durationSeconds = realmConfig.getConfig(FeatureConfiguration.STORAGE_CREDENTIAL_DURATION_SECONDS);

    boolean includePrincipalName =
        realmConfig.getConfig(FeatureConfiguration.INCLUDE_PRINCIPAL_NAME_IN_SUBSCOPED_CREDENTIAL);
    String roleSessionName =
        includePrincipalName
            ? AwsRoleSessionNameSanitizer.sanitize("polaris-" + polarisPrincipal.getName())
            : defaultRoleSessionName;

    AssumeRoleRequest.Builder request =
        AssumeRoleRequest.builder()
            .externalId(externalId)
            .roleArn(roleArn)
            .roleSessionName(roleSessionName)
            .policy(policyJson)
            .durationSeconds(durationSeconds);

    List<String> sessionTagFieldNames =
        realmConfig.getConfig(FeatureConfiguration.SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL);
    Set<SessionTagField> enabledSessionTagFields =
        sessionTagFieldNames.stream()
            .map(SessionTagField::fromConfigName)
            .flatMap(Optional::stream)
            .collect(Collectors.toCollection(() -> EnumSet.noneOf(SessionTagField.class)));

    if (!enabledSessionTagFields.isEmpty()) {
      List<Tag> sessionTags =
          AwsSessionTagsBuilder.buildSessionTags(
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
        stsClientProvider.stsClient(StsDestination.of(stsEndpointUri, region));

    AssumeRoleResponse response = stsClient.assumeRole(request.build());
    accessConfig.put(StorageAccessProperty.AWS_KEY_ID, response.credentials().accessKeyId());
    accessConfig.put(StorageAccessProperty.AWS_SECRET_KEY, response.credentials().secretAccessKey());
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
  }
}
