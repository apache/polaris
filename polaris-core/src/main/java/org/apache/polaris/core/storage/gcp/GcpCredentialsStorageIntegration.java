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
package org.apache.polaris.core.storage.gcp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.CredentialAccessBoundary;
import com.google.auth.oauth2.DownscopedCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.iam.credentials.v1.GenerateAccessTokenRequest;
import com.google.cloud.iam.credentials.v1.GenerateAccessTokenResponse;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.google.cloud.iam.credentials.v1.IamCredentialsSettings;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.InMemoryStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.StorageUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GCS implementation of {@link PolarisStorageIntegration} with support for scoping credentials for
 * input read/write locations
 */
public class GcpCredentialsStorageIntegration
    extends InMemoryStorageIntegration<GcpStorageConfigurationInfo> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(GcpCredentialsStorageIntegration.class);
  public static final String SERVICE_ACCOUNT_PREFIX = "projects/-/serviceAccounts/";
  public static final String IMPERSONATION_SCOPE =
      "https://www.googleapis.com/auth/devstorage.read_write";

  private final GoogleCredentials sourceCredentials;
  private final HttpTransportFactory transportFactory;

  public GcpCredentialsStorageIntegration(
      GcpStorageConfigurationInfo config,
      GoogleCredentials sourceCredentials,
      HttpTransportFactory transportFactory) {
    super(config, GcpCredentialsStorageIntegration.class.getName());
    // Needed for when environment variable GOOGLE_APPLICATION_CREDENTIALS points to google service
    // account key json
    this.sourceCredentials =
        sourceCredentials.createScoped("https://www.googleapis.com/auth/cloud-platform");
    this.transportFactory = transportFactory;
  }

  @Override
  public StorageAccessConfig getSubscopedCreds(
      @Nonnull RealmConfig realmConfig,
      boolean allowListOperation,
      @Nonnull Set<String> allowedReadLocations,
      @Nonnull Set<String> allowedWriteLocations,
      @Nonnull PolarisPrincipal polarisPrincipal,
      Optional<String> refreshCredentialsEndpoint) {
    try {
      sourceCredentials.refresh();
    } catch (IOException e) {
      throw new RuntimeException("Unable to refresh GCP credentials", e);
    }

    GoogleCredentials credentialsToDownscope = getBaseCredentials();

    CredentialAccessBoundary accessBoundary =
        generateAccessBoundaryRules(
            allowListOperation, allowedReadLocations, allowedWriteLocations);
    DownscopedCredentials credentials =
        DownscopedCredentials.newBuilder()
            .setHttpTransportFactory(transportFactory)
            .setSourceCredential(credentialsToDownscope)
            .setCredentialAccessBoundary(accessBoundary)
            .build();
    AccessToken token;
    try {
      token = refreshAccessToken(credentials);
    } catch (IOException e) {
      LOGGER
          .atError()
          .addKeyValue("readLocations", allowedReadLocations)
          .addKeyValue("writeLocations", allowedWriteLocations)
          .addKeyValue("includesList", allowListOperation)
          .addKeyValue("accessBoundary", convertToString(accessBoundary))
          .log("Unable to refresh access credentials", e);
      throw new RuntimeException("Unable to fetch access credentials " + e.getMessage());
    }

    // If expires_in missing, use source credential's expire time, which require another api call to
    // get.
    StorageAccessConfig.Builder accessConfig =
        StorageAccessConfig.builder().supportsCredentialVending(true).supportsRemoteSigning(false);
    accessConfig.put(StorageAccessProperty.GCS_ACCESS_TOKEN, token.getTokenValue());
    accessConfig.put(
        StorageAccessProperty.GCS_ACCESS_TOKEN_EXPIRES_AT,
        String.valueOf(token.getExpirationTime().getTime()));

    refreshCredentialsEndpoint.ifPresent(
        endpoint -> {
          accessConfig.put(StorageAccessProperty.GCS_REFRESH_CREDENTIALS_ENDPOINT, endpoint);
        });

    return accessConfig.build();
  }

  /**
   * Returns the credential to be used as the source for downscoping. If a specific service account
   * is configured, it impersonates that account first.
   */
  private GoogleCredentials getBaseCredentials() {
    if (config().getGcpServiceAccount() != null) {
      return createImpersonatedCredentials(sourceCredentials, config().getGcpServiceAccount());
    }
    return sourceCredentials;
  }

  private GoogleCredentials createImpersonatedCredentials(
      GoogleCredentials source, String targetServiceAccount) {
    try (IamCredentialsClient iamCredentialsClient = createIamCredentialsClient(source)) {
      GenerateAccessTokenRequest request =
          GenerateAccessTokenRequest.newBuilder()
              .setName(SERVICE_ACCOUNT_PREFIX + targetServiceAccount)
              .addAllDelegates(new ArrayList<>())
              // 'cloud-platform' is often preferred for impersonation,
              // but devstorage.read_write is sufficient for GCS specific operations.
              // See https://docs.cloud.google.com/storage/docs/oauth-scopes
              .addScope(IMPERSONATION_SCOPE)
              .setLifetime(Duration.newBuilder().setSeconds(3600).build())
              .build();

      GenerateAccessTokenResponse response = iamCredentialsClient.generateAccessToken(request);

      Timestamp expirationTime = response.getExpireTime();
      // Use Instant to avoid precision loss or overflow issues with Date multiplication
      Date expirationDate =
          Date.from(Instant.ofEpochSecond(expirationTime.getSeconds(), expirationTime.getNanos()));

      AccessToken accessToken = new AccessToken(response.getAccessToken(), expirationDate);
      return GoogleCredentials.create(accessToken);
    } catch (IOException e) {
      throw new RuntimeException(
          "Unable to impersonate GCP service account: " + targetServiceAccount, e);
    }
  }

  private String convertToString(CredentialAccessBoundary accessBoundary) {
    try {
      return new ObjectMapper().writeValueAsString(accessBoundary);
    } catch (JsonProcessingException e) {
      LOGGER.warn("Unable to convert access boundary to json", e);
      return Objects.toString(accessBoundary);
    }
  }

  @VisibleForTesting
  public static CredentialAccessBoundary generateAccessBoundaryRules(
      boolean allowListOperation,
      @Nonnull Set<String> allowedReadLocations,
      @Nonnull Set<String> allowedWriteLocations) {
    Map<String, List<String>> readConditionsMap = new HashMap<>();
    Map<String, List<String>> writeConditionsMap = new HashMap<>();

    HashSet<String> readBuckets = new HashSet<>();
    HashSet<String> writeBuckets = new HashSet<>();
    Stream.concat(allowedReadLocations.stream(), allowedWriteLocations.stream())
        .distinct()
        .forEach(
            location -> {
              URI uri = URI.create(location);
              String bucket = StorageUtil.getBucket(uri);
              readBuckets.add(bucket);
              String path = uri.getPath().substring(1);
              List<String> resourceExpressions =
                  readConditionsMap.computeIfAbsent(bucket, key -> new ArrayList<>());
              resourceExpressions.add(
                  String.format(
                      "resource.name.startsWith('projects/_/buckets/%s/objects/%s')",
                      bucket, path));
              if (allowListOperation) {
                resourceExpressions.add(
                    String.format(
                        "api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('%s')",
                        path));
              }
              if (allowedWriteLocations.contains(location)) {
                writeBuckets.add(bucket);
                List<String> writeExpressions =
                    writeConditionsMap.computeIfAbsent(bucket, key -> new ArrayList<>());
                writeExpressions.add(
                    String.format(
                        "resource.name.startsWith('projects/_/buckets/%s/objects/%s')",
                        bucket, path));
              }
            });
    CredentialAccessBoundary.Builder accessBoundaryBuilder = CredentialAccessBoundary.newBuilder();
    readBuckets.forEach(
        bucket -> {
          List<String> readConditions = readConditionsMap.get(bucket);
          if (readConditions == null || readConditions.isEmpty()) {
            return;
          }
          CredentialAccessBoundary.AccessBoundaryRule.Builder builder =
              CredentialAccessBoundary.AccessBoundaryRule.newBuilder();
          builder.setAvailableResource(bucketResource(bucket));
          builder.setAvailabilityCondition(
              CredentialAccessBoundary.AccessBoundaryRule.AvailabilityCondition.newBuilder()
                  .setExpression(String.join(" || ", readConditions))
                  .build());
          builder.setAvailablePermissions(List.of("inRole:roles/storage.legacyObjectReader"));
          if (allowListOperation) {
            builder.addAvailablePermission("inRole:roles/storage.objectViewer");
          }
          accessBoundaryBuilder.addRule(builder.build());
        });
    writeBuckets.forEach(
        bucket -> {
          List<String> writeConditions = writeConditionsMap.get(bucket);
          if (writeConditions == null || writeConditions.isEmpty()) {
            return;
          }
          CredentialAccessBoundary.AccessBoundaryRule.Builder builder =
              CredentialAccessBoundary.AccessBoundaryRule.newBuilder();
          builder.setAvailableResource(bucketResource(bucket));
          builder.setAvailabilityCondition(
              CredentialAccessBoundary.AccessBoundaryRule.AvailabilityCondition.newBuilder()
                  .setExpression(String.join(" || ", writeConditions))
                  .build());
          builder.setAvailablePermissions(List.of("inRole:roles/storage.legacyBucketWriter"));
          accessBoundaryBuilder.addRule(builder.build());
        });
    return accessBoundaryBuilder.build();
  }

  @VisibleForTesting
  protected AccessToken refreshAccessToken(DownscopedCredentials credentials) throws IOException {
    return credentials.refreshAccessToken();
  }

  @VisibleForTesting
  protected IamCredentialsClient createIamCredentialsClient(GoogleCredentials credentials)
      throws IOException {
    return IamCredentialsClient.create(
        IamCredentialsSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
            .build());
  }

  private static String bucketResource(String bucket) {
    return "//storage.googleapis.com/projects/_/buckets/" + bucket;
  }
}
