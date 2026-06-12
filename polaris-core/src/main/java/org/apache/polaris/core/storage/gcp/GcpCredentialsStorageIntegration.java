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
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.CredentialAccessBoundary;
import com.google.auth.oauth2.DownscopedCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.iam.credentials.v1.GenerateAccessTokenRequest;
import com.google.cloud.iam.credentials.v1.GenerateAccessTokenResponse;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.CachingStorageIntegration;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.LocationGrant;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.StorageLocationPreparer;
import org.apache.polaris.core.storage.StorageUri;
import org.apache.polaris.core.storage.cache.StorageCredentialCacheKey;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GCS implementation of {@link PolarisStorageIntegration} with support for scoping credentials for
 * input read/write locations
 */
public class GcpCredentialsStorageIntegration
    extends CachingStorageIntegration<GcpStorageConfigurationInfo> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(GcpCredentialsStorageIntegration.class);
  public static final String SERVICE_ACCOUNT_PREFIX = "projects/-/serviceAccounts/";
  public static final String IMPERSONATION_SCOPE =
      "https://www.googleapis.com/auth/devstorage.read_write";

  private static final ObjectMapper OBJECT_MAPPER = JsonMapper.builder().build();

  private final GoogleCredentials sourceCredentials;
  private final HttpTransportFactory transportFactory;
  private final GcpCredentialOps credentialOps;
  private final StorageLocationPreparer folderPreparer;

  public GcpCredentialsStorageIntegration(
      GoogleCredentials sourceCredentials,
      HttpTransportFactory transportFactory,
      GcpStorageConfigurationInfo storageConfig,
      RealmConfig realmConfig) {
    this(
        sourceCredentials,
        transportFactory,
        null,
        storageConfig,
        realmConfig,
        GcpCredentialOps.DEFAULT,
        locations -> {});
  }

  public GcpCredentialsStorageIntegration(
      GoogleCredentials sourceCredentials,
      HttpTransportFactory transportFactory,
      org.apache.polaris.core.storage.cache.StorageCredentialCache cache,
      GcpStorageConfigurationInfo storageConfig,
      RealmConfig realmConfig) {
    this(
        sourceCredentials,
        transportFactory,
        cache,
        storageConfig,
        realmConfig,
        GcpCredentialOps.DEFAULT,
        locations -> {});
  }

  public GcpCredentialsStorageIntegration(
      GoogleCredentials sourceCredentials,
      HttpTransportFactory transportFactory,
      GcpStorageConfigurationInfo storageConfig,
      RealmConfig realmConfig,
      GcpCredentialOps credentialOps) {
    this(
        sourceCredentials,
        transportFactory,
        null,
        storageConfig,
        realmConfig,
        credentialOps,
        locations -> {});
  }

  public GcpCredentialsStorageIntegration(
      GoogleCredentials sourceCredentials,
      HttpTransportFactory transportFactory,
      org.apache.polaris.core.storage.cache.StorageCredentialCache cache,
      GcpStorageConfigurationInfo storageConfig,
      RealmConfig realmConfig,
      GcpCredentialOps credentialOps) {
    this(
        sourceCredentials,
        transportFactory,
        cache,
        storageConfig,
        realmConfig,
        credentialOps,
        locations -> {});
  }

  public GcpCredentialsStorageIntegration(
      GoogleCredentials sourceCredentials,
      HttpTransportFactory transportFactory,
      org.apache.polaris.core.storage.cache.StorageCredentialCache cache,
      GcpStorageConfigurationInfo storageConfig,
      RealmConfig realmConfig,
      @NonNull StorageLocationPreparer folderPreparer) {
    this(
        sourceCredentials,
        transportFactory,
        cache,
        storageConfig,
        realmConfig,
        GcpCredentialOps.DEFAULT,
        folderPreparer);
  }

  public GcpCredentialsStorageIntegration(
      GoogleCredentials sourceCredentials,
      HttpTransportFactory transportFactory,
      org.apache.polaris.core.storage.cache.StorageCredentialCache cache,
      GcpStorageConfigurationInfo storageConfig,
      RealmConfig realmConfig,
      GcpCredentialOps credentialOps,
      @NonNull StorageLocationPreparer folderPreparer) {
    super(cache, realmConfig, storageConfig);
    // Needed for when environment variable GOOGLE_APPLICATION_CREDENTIALS points to google service
    // account key json
    this.sourceCredentials =
        sourceCredentials.createScoped("https://www.googleapis.com/auth/cloud-platform");
    this.transportFactory = transportFactory;
    this.credentialOps = credentialOps;
    this.folderPreparer = Objects.requireNonNull(folderPreparer, "folderPreparer");
  }

  @Override
  public void prepareLocations(@NonNull List<String> locations) {
    folderPreparer.prepareLocations(locations);
  }

  @Override
  protected StorageCredentialCacheKey buildCacheKey(
      @NonNull List<LocationGrant> grants,
      @NonNull Optional<String> refreshEndpoint,
      @NonNull CredentialVendingContext context) {
    return buildCacheKey(
        readLocations(grants),
        listLocations(grants),
        writeLocations(grants),
        refreshEndpoint,
        context);
  }

  private static Set<String> readLocations(List<LocationGrant> grants) {
    return locationsFor(grants, PolarisStorageActions.READ, PolarisStorageActions.ALL);
  }

  private static Set<String> listLocations(List<LocationGrant> grants) {
    return locationsFor(grants, PolarisStorageActions.LIST, PolarisStorageActions.ALL);
  }

  private static Set<String> writeLocations(List<LocationGrant> grants) {
    return locationsFor(
        grants,
        PolarisStorageActions.WRITE,
        PolarisStorageActions.DELETE,
        PolarisStorageActions.ALL);
  }

  private static Set<String> locationsFor(
      List<LocationGrant> grants, PolarisStorageActions... wantedActions) {
    Set<PolarisStorageActions> wanted = Set.of(wantedActions);
    return grants.stream()
        .filter(g -> g.actions().stream().anyMatch(wanted::contains))
        .flatMap(g -> g.locations().stream())
        .collect(Collectors.toSet());
  }

  private GcpStorageCredentialCacheKey buildCacheKey(
      @NonNull Set<String> readLocations,
      @NonNull Set<String> listLocations,
      @NonNull Set<String> writeLocations,
      @NonNull Optional<String> refreshEndpoint,
      @NonNull CredentialVendingContext context) {
    return GcpStorageCredentialCacheKey.of(
        context.realm().orElse(""),
        storageConfig(),
        readLocations,
        listLocations,
        writeLocations,
        refreshEndpoint,
        sourceCredentials,
        transportFactory,
        realmConfig(),
        credentialOps);
  }

  /** Mint a fresh {@link StorageAccessConfig} for the given GCP cache key. */
  static StorageAccessConfig compute(GcpStorageCredentialCacheKey key) {
    GcpStorageConfigurationInfo gcpStorageConfig = key.storageConfig();
    GoogleCredentials sourceCredentials = key.sourceCredentials();
    HttpTransportFactory transportFactory = key.transportFactory();
    GcpCredentialOps credentialOps = key.credentialOps();
    Set<String> readLocations = key.allowedReadLocations();
    Set<String> listLocations = key.allowedListLocations();
    Set<String> writeLocations = key.allowedWriteLocations();

    try {
      sourceCredentials.refresh();
    } catch (IOException e) {
      throw new RuntimeException("Unable to refresh GCP credentials", e);
    }

    GoogleCredentials credentialsToDownscope =
        getBaseCredentials(gcpStorageConfig, sourceCredentials, credentialOps);

    CredentialAccessBoundary accessBoundary =
        generateAccessBoundaryRules(
            readLocations,
            listLocations,
            writeLocations,
            bucket -> isHnsBucket(bucket, sourceCredentials));
    DownscopedCredentials credentials =
        DownscopedCredentials.newBuilder()
            .setHttpTransportFactory(transportFactory)
            .setSourceCredential(credentialsToDownscope)
            .setCredentialAccessBoundary(accessBoundary)
            .build();
    AccessToken token;
    try {
      token = credentialOps.refreshAccessToken(credentials);
    } catch (IOException e) {
      LOGGER
          .atError()
          .addKeyValue("readLocations", readLocations)
          .addKeyValue("listLocations", listLocations)
          .addKeyValue("writeLocations", writeLocations)
          .addKeyValue("accessBoundary", convertToString(accessBoundary))
          .log("Unable to refresh access credentials", e);
      throw new RuntimeException("Unable to fetch access credentials " + e.getMessage());
    }

    // If expires_in missing, use source credential's expire time, which require another api call to
    // get.
    StorageAccessConfig.Builder accessConfig = StorageAccessConfig.builder();
    accessConfig.put(StorageAccessProperty.GCS_ACCESS_TOKEN, token.getTokenValue());
    accessConfig.put(
        StorageAccessProperty.GCS_ACCESS_TOKEN_EXPIRES_AT,
        String.valueOf(token.getExpirationTime().getTime()));

    key.refreshCredentialsEndpoint()
        .ifPresent(
            endpoint ->
                accessConfig.put(StorageAccessProperty.GCS_REFRESH_CREDENTIALS_ENDPOINT, endpoint));

    return accessConfig.build();
  }

  /**
   * Returns the credential to be used as the source for downscoping. If a specific service account
   * is configured, it impersonates that account first.
   */
  private static GoogleCredentials getBaseCredentials(
      GcpStorageConfigurationInfo storageConfig,
      GoogleCredentials sourceCredentials,
      GcpCredentialOps credentialOps) {
    if (storageConfig.getGcpServiceAccount() != null) {
      return createImpersonatedCredentials(
          sourceCredentials, storageConfig.getGcpServiceAccount(), credentialOps);
    }
    return sourceCredentials;
  }

  private static GoogleCredentials createImpersonatedCredentials(
      GoogleCredentials source, String targetServiceAccount, GcpCredentialOps credentialOps) {
    try (IamCredentialsClient iamCredentialsClient =
        credentialOps.createIamCredentialsClient(source)) {
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

  private static String convertToString(CredentialAccessBoundary accessBoundary) {
    try {
      return OBJECT_MAPPER.writeValueAsString(accessBoundary);
    } catch (JsonProcessingException e) {
      LOGGER.warn("Unable to convert access boundary to json", e);
      return Objects.toString(accessBoundary);
    }
  }

  /**
   * Generate a {@link CredentialAccessBoundary} honoring per-action grants. The {@code
   * legacyObjectReader} permission is granted on locations from {@code allowedReadLocations} (and
   * also on {@code allowedListLocations}, since GCS list requires bucket-level read); the {@code
   * objectViewer} permission is added on buckets that have any location in {@code
   * allowedListLocations}; write access only on {@code allowedWriteLocations}. CEL conditions are
   * de-duplicated so a path that appears in both the read and list sets contributes a single {@code
   * resourceNameStartsWith} expression.
   */
  @VisibleForTesting
  public static CredentialAccessBoundary generateAccessBoundaryRules(
      @NonNull Set<String> allowedReadLocations,
      @NonNull Set<String> allowedListLocations,
      @NonNull Set<String> allowedWriteLocations) {
    return generateAccessBoundaryRules(
        allowedReadLocations, allowedListLocations, allowedWriteLocations, bucket -> false);
  }

  @VisibleForTesting
  public static CredentialAccessBoundary generateAccessBoundaryRules(
      boolean allowListOperation,
      @NonNull Set<String> allowedReadLocations,
      @NonNull Set<String> allowedWriteLocations,
      @NonNull Predicate<String> isHnsBucket) {
    return generateAccessBoundaryRules(
        allowedReadLocations,
        allowListOperation ? allowedReadLocations : Set.of(),
        allowedWriteLocations,
        isHnsBucket);
  }

  @VisibleForTesting
  public static CredentialAccessBoundary generateAccessBoundaryRules(
      @NonNull Set<String> allowedReadLocations,
      @NonNull Set<String> allowedListLocations,
      @NonNull Set<String> allowedWriteLocations,
      @NonNull Predicate<String> isHnsBucket) {
    Map<String, LinkedHashSet<String>> readConditionsByBucket = new LinkedHashMap<>();
    Map<String, LinkedHashSet<String>> writeConditionsByBucket = new LinkedHashMap<>();
    Map<String, LinkedHashSet<String>> folderConditionsByBucket = new LinkedHashMap<>();
    HashSet<String> bucketsWithList = new HashSet<>();

    Stream.concat(allowedReadLocations.stream(), allowedListLocations.stream())
        .distinct()
        .forEach(
            location -> {
              StorageUri uri = StorageUri.parse(location);
              String bucket = uri.authority();
              String path = uri.rawPath().substring(1);
              readConditionsByBucket
                  .computeIfAbsent(bucket, key -> new LinkedHashSet<>())
                  .add(resourceNameStartsWithExpression(bucket, path));
            });

    allowedListLocations.forEach(
        location -> {
          StorageUri uri = StorageUri.parse(location);
          String bucket = uri.authority();
          String path = uri.rawPath().substring(1);
          readConditionsByBucket
              .computeIfAbsent(bucket, key -> new LinkedHashSet<>())
              .add(objectListPrefixStartsWithExpression(path));
          bucketsWithList.add(bucket);
        });

    allowedWriteLocations.forEach(
        location -> {
          StorageUri uri = StorageUri.parse(location);
          String bucket = uri.authority();
          String path = uri.rawPath().substring(1);
          writeConditionsByBucket
              .computeIfAbsent(bucket, key -> new LinkedHashSet<>())
              .add(resourceNameStartsWithExpression(bucket, path));
          folderConditionsByBucket
              .computeIfAbsent(bucket, key -> new LinkedHashSet<>())
              .add(folderNameStartsWithExpression(bucket, path));
        });

    CredentialAccessBoundary.Builder accessBoundaryBuilder = CredentialAccessBoundary.newBuilder();
    readConditionsByBucket.forEach(
        (bucket, conditions) -> {
          if (conditions.isEmpty()) {
            return;
          }
          CredentialAccessBoundary.AccessBoundaryRule.Builder builder =
              CredentialAccessBoundary.AccessBoundaryRule.newBuilder();
          builder.setAvailableResource(bucketResource(bucket));
          builder.setAvailabilityCondition(
              CredentialAccessBoundary.AccessBoundaryRule.AvailabilityCondition.newBuilder()
                  .setExpression(String.join(" || ", conditions))
                  .build());
          builder.setAvailablePermissions(List.of("inRole:roles/storage.legacyObjectReader"));
          if (bucketsWithList.contains(bucket)) {
            builder.addAvailablePermission("inRole:roles/storage.objectViewer");
          }
          accessBoundaryBuilder.addRule(builder.build());
        });
    writeConditionsByBucket.forEach(
        (bucket, conditions) -> {
          if (conditions.isEmpty()) {
            return;
          }
          CredentialAccessBoundary.AccessBoundaryRule.Builder builder =
              CredentialAccessBoundary.AccessBoundaryRule.newBuilder();
          builder.setAvailableResource(bucketResource(bucket));
          builder.setAvailabilityCondition(
              CredentialAccessBoundary.AccessBoundaryRule.AvailabilityCondition.newBuilder()
                  .setExpression(String.join(" || ", conditions))
                  .build());
          builder.setAvailablePermissions(List.of("inRole:roles/storage.legacyBucketWriter"));
          accessBoundaryBuilder.addRule(builder.build());
        });
    folderConditionsByBucket.forEach(
        (bucket, conditions) -> {
          if (conditions.isEmpty() || !isHnsBucket.test(bucket)) {
            return;
          }
          CredentialAccessBoundary.AccessBoundaryRule.Builder builder =
              CredentialAccessBoundary.AccessBoundaryRule.newBuilder();
          builder.setAvailableResource(bucketResource(bucket));
          builder.setAvailabilityCondition(
              CredentialAccessBoundary.AccessBoundaryRule.AvailabilityCondition.newBuilder()
                  .setExpression(String.join(" || ", conditions))
                  .build());
          builder.setAvailablePermissions(List.of("storage.folders.create", "storage.folders.get"));
          accessBoundaryBuilder.addRule(builder.build());
        });
    return accessBoundaryBuilder.build();
  }

  @VisibleForTesting
  static boolean isHnsBucket(String bucket, GoogleCredentials sourceCredentials) {
    try {
      Storage storage = newStorageClient(sourceCredentials);
      Bucket bucketMetadata =
          storage.get(
              bucket, Storage.BucketGetOption.fields(Storage.BucketField.HIERARCHICAL_NAMESPACE));
      if (bucketMetadata == null) {
        return false;
      }
      BucketInfo.HierarchicalNamespace hns = bucketMetadata.getHierarchicalNamespace();
      return hns != null && Boolean.TRUE.equals(hns.getEnabled());
    } catch (RuntimeException e) {
      LOGGER
          .atWarn()
          .addKeyValue("bucket", bucket)
          .log("Failed to determine HNS status; defaulting to non-HNS", e);
      return false;
    }
  }

  @VisibleForTesting
  static Storage newStorageClient(GoogleCredentials sourceCredentials) {
    return StorageOptions.newBuilder().setCredentials(sourceCredentials).build().getService();
  }

  @VisibleForTesting
  static String resourceNameStartsWithExpression(String bucket, String path) {
    return String.format(
        "resource.name.startsWith('projects/_/buckets/%s/objects/%s')",
        escapeCelLiteral(bucket), escapeCelLiteral(path));
  }

  @VisibleForTesting
  static String folderNameStartsWithExpression(String bucket, String path) {
    return String.format(
        "resource.name.startsWith('projects/_/buckets/%s/folders/%s')",
        escapeCelLiteral(bucket), escapeCelLiteral(path));
  }

  @VisibleForTesting
  static String objectListPrefixStartsWithExpression(String path) {
    return String.format(
        "api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('%s')",
        escapeCelLiteral(path));
  }

  @VisibleForTesting
  static String escapeCelLiteral(String value) {
    StringBuilder escaped = new StringBuilder(value.length() * 3 / 2);
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '\'' -> escaped.append("\\'");
        case '"' -> escaped.append("\\\"");
        case '\\' -> escaped.append("\\\\");
        case '\b' -> escaped.append("\\b");
        case '\f' -> escaped.append("\\f");
        case '\n' -> escaped.append("\\n");
        case '\r' -> escaped.append("\\r");
        case '\t' -> escaped.append("\\t");
        default -> {
          if (Character.isSurrogate(c)) {
            if (!Character.isHighSurrogate(c)
                || i + 1 >= value.length()
                || !Character.isLowSurrogate(value.charAt(i + 1))) {
              throw new IllegalArgumentException(
                  "Unsupported unpaired surrogate in GCS credential access boundary input");
            }
            escaped.append(c).append(value.charAt(++i));
          } else if (Character.isISOControl(c)) {
            throw new IllegalArgumentException(
                "Unsupported control character in GCS credential access boundary input");
          } else {
            escaped.append(c);
          }
        }
      }
    }
    return escaped.toString();
  }

  private static String bucketResource(String bucket) {
    return "//storage.googleapis.com/projects/_/buckets/" + bucket;
  }
}
