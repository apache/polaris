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

import static org.apache.polaris.core.storage.StorageLocation.ensureTrailingSlash;
import static org.apache.polaris.core.storage.StorageLocation.trimLeadingSlash;

import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Folder;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.CredentialAccessBoundary;
import com.google.auth.oauth2.DownscopedCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.iam.credentials.v1.GenerateAccessTokenRequest;
import com.google.cloud.iam.credentials.v1.GenerateAccessTokenResponse;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
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
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.polaris.core.StructuredLogKeys;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.CachingStorageIntegration;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.LocationGrant;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.StorageUri;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.core.storage.cache.StorageCredentialCacheKey;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

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

  private static final ObjectMapper OBJECT_MAPPER = JsonMapper.shared();

  private static final int FOLDER_CREATE_TIMEOUT_MS = 5_000;

  private final GoogleCredentials sourceCredentials;
  private final HttpTransportFactory transportFactory;
  private final GcpCredentialOps credentialOps;
  private final Optional<GcpAttributionParams> attributionParams;

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
        resolveAttributionParams(realmConfig));
  }

  public GcpCredentialsStorageIntegration(
      GoogleCredentials sourceCredentials,
      HttpTransportFactory transportFactory,
      StorageCredentialCache cache,
      GcpStorageConfigurationInfo storageConfig,
      RealmConfig realmConfig) {
    this(
        sourceCredentials,
        transportFactory,
        cache,
        storageConfig,
        realmConfig,
        GcpCredentialOps.DEFAULT,
        resolveAttributionParams(realmConfig));
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
        resolveAttributionParams(realmConfig));
  }

  public GcpCredentialsStorageIntegration(
      GoogleCredentials sourceCredentials,
      HttpTransportFactory transportFactory,
      StorageCredentialCache cache,
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
        resolveAttributionParams(realmConfig));
  }

  /**
   * Full constructor accepting pre-resolved attribution params. Downstream integrations can pass
   * their own {@link GcpAttributionParams} without depending on {@link
   * org.apache.polaris.core.config.FeatureConfiguration}.
   */
  public GcpCredentialsStorageIntegration(
      GoogleCredentials sourceCredentials,
      HttpTransportFactory transportFactory,
      StorageCredentialCache cache,
      GcpStorageConfigurationInfo storageConfig,
      RealmConfig realmConfig,
      GcpCredentialOps credentialOps,
      Optional<GcpAttributionParams> attributionParams) {
    super(cache, realmConfig, storageConfig);
    // Needed for when environment variable GOOGLE_APPLICATION_CREDENTIALS points to google service
    // account key json
    this.sourceCredentials =
        sourceCredentials.createScoped("https://www.googleapis.com/auth/cloud-platform");
    this.transportFactory = transportFactory;
    this.credentialOps = credentialOps;
    this.attributionParams = attributionParams;
  }

  /**
   * Resolves {@link GcpAttributionParams} from realm config. Returns empty when attribution is
   * disabled; throws {@link IllegalStateException} when enabled but misconfigured.
   *
   * <p>Static so it can be called from constructor-chaining expressions ({@code this(...)}).
   */
  public static Optional<GcpAttributionParams> resolveAttributionParams(RealmConfig realmConfig) {
    if (!realmConfig.getConfig(FeatureConfiguration.GCS_PRINCIPAL_ATTRIBUTION_ENABLED)) {
      return Optional.empty();
    }
    List<String> missing = new ArrayList<>();
    if (realmConfig
        .getConfig(FeatureConfiguration.GCS_PRINCIPAL_ATTRIBUTION_WIF_AUDIENCE)
        .isEmpty()) {
      missing.add("GCS_PRINCIPAL_ATTRIBUTION_WIF_AUDIENCE");
    }
    if (realmConfig
        .getConfig(FeatureConfiguration.GCS_PRINCIPAL_ATTRIBUTION_TOKEN_ISSUER)
        .isEmpty()) {
      missing.add("GCS_PRINCIPAL_ATTRIBUTION_TOKEN_ISSUER");
    }
    if (realmConfig
        .getConfig(FeatureConfiguration.GCS_PRINCIPAL_ATTRIBUTION_SIGNING_KEY_FILE)
        .isEmpty()) {
      missing.add("GCS_PRINCIPAL_ATTRIBUTION_SIGNING_KEY_FILE");
    }
    if (!missing.isEmpty()) {
      throw new IllegalStateException(
          "GCS_PRINCIPAL_ATTRIBUTION_ENABLED is true but the following required config values are"
              + " missing: "
              + String.join(", ", missing));
    }
    return Optional.of(
        GcpAttributionParams.of(
            realmConfig.getConfig(FeatureConfiguration.GCS_PRINCIPAL_ATTRIBUTION_TOKEN_ISSUER),
            realmConfig.getConfig(FeatureConfiguration.GCS_PRINCIPAL_ATTRIBUTION_WIF_AUDIENCE),
            realmConfig.getConfig(FeatureConfiguration.GCS_PRINCIPAL_ATTRIBUTION_SIGNING_KEY_FILE),
            realmConfig.getConfig(FeatureConfiguration.GCS_PRINCIPAL_ATTRIBUTION_SIGNING_KEY_ID)));
  }

  @Override
  protected StorageCredentialCacheKey buildCacheKey(
      @NonNull List<LocationGrant> grants, @NonNull CredentialVendingContext context) {
    return buildCacheKey(
        readLocations(grants),
        listLocations(grants),
        writeLocations(grants),
        parentFolderLocations(grants),
        context);
  }

  @Override
  protected StorageAccessConfig addExtraProperties(
      @NonNull StorageAccessConfig accessConfig, @NonNull Optional<String> refreshEndpoint) {
    return refreshEndpoint
        .map(endpoint ->
            withExtraProperty(
                accessConfig, StorageAccessProperty.GCS_REFRESH_CREDENTIALS_ENDPOINT, endpoint))
        .orElse(accessConfig);
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

  /**
   * Write locations whose missing parent folders Polaris creates: those granted WRITE or ALL. A
   * DELETE grant without WRITE, as requested by file cleanup tasks, needs no new folders.
   */
  private static Set<String> parentFolderLocations(List<LocationGrant> grants) {
    return locationsFor(grants, PolarisStorageActions.WRITE, PolarisStorageActions.ALL);
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
      @NonNull Set<String> parentFolderLocations,
      @NonNull CredentialVendingContext context) {
    // Principal attribution makes the vended token per-principal, so the principal must
    // participate in cache identity; otherwise it is left empty to preserve cross-principal cache
    // reuse. Attribution requires a service account to impersonate and a principal to attribute.
    Optional<String> principalName = Optional.empty();
    Optional<GcpAttributionParams> resolvedAttributionParams = Optional.empty();
    if (attributionParams.isPresent()) {
      if (storageConfig().getGcpServiceAccount() == null) {
        LOGGER.warn(
            "GCS principal attribution is enabled but no gcpServiceAccount is configured"
                + " on the StorageConfiguration; falling back to non-attributed credentials");
      } else {
        Optional<String> ctxPrincipal = context.principalName().filter(n -> !n.isEmpty());
        if (ctxPrincipal.isPresent()) {
          principalName = ctxPrincipal;
          resolvedAttributionParams = attributionParams;
        } else {
          LOGGER.warn(
              "GCS principal attribution is enabled but no principal name is present in the"
                  + " credential vending context; falling back to non-attributed credentials");
        }
      }
    }
    return GcpStorageCredentialCacheKey.of(
        context.realm().orElse(""),
        storageConfig(),
        readLocations,
        listLocations,
        writeLocations,
        parentFolderLocations,
        principalName,
        sourceCredentials,
        transportFactory,
        realmConfig(),
        credentialOps,
        resolvedAttributionParams);
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
        resolveSourceCredentials(key, gcpStorageConfig, sourceCredentials, credentialOps);

    CredentialAccessBoundary accessBoundary =
        generateAccessBoundaryRules(readLocations, listLocations, writeLocations);
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
          .addKeyValue(StructuredLogKeys.READ_LOCATIONS, readLocations)
          .addKeyValue(StructuredLogKeys.LIST_LOCATIONS, listLocations)
          .addKeyValue(StructuredLogKeys.WRITE_LOCATIONS, writeLocations)
          .addKeyValue(StructuredLogKeys.ACCESS_BOUNDARY, convertToString(accessBoundary))
          .log("Unable to refresh access credentials", e);
      throw new RuntimeException("Unable to fetch access credentials " + e.getMessage());
    }

    if (key.realmConfig().getConfig(FeatureConfiguration.GCS_HNS_PARENT_FOLDER_CREATION_ENABLED)) {
      createParentFolders(key.parentFolderLocations(), credentialsToDownscope, transportFactory);
    }

    // If expires_in missing, use source credential's expire time, which require another api call to
    // get.
    StorageAccessConfig.Builder accessConfig = StorageAccessConfig.builder();
    accessConfig.put(StorageAccessProperty.GCS_ACCESS_TOKEN, token.getTokenValue());
    accessConfig.put(
        StorageAccessProperty.GCS_ACCESS_TOKEN_EXPIRES_AT_MS,
        String.valueOf(token.getExpirationTime().getTime()));

    return accessConfig.build();
  }

  /**
   * Returns the credential to be used as the source for downscoping.
   *
   * <p>When GCS principal attribution is configured and a principal is present (so the cache key
   * carries pre-computed {@link GcpAttributionParams}), the impersonation source is a federated
   * identity whose subject is {@code <realm>/<principal>}, which surfaces the principal in {@code
   * serviceAccountDelegationInfo} of GCS Data Access audit logs. Otherwise this is the standard
   * path: impersonate the configured service account from the ambient source credentials, or use
   * those credentials directly.
   */
  private static GoogleCredentials resolveSourceCredentials(
      GcpStorageCredentialCacheKey key,
      GcpStorageConfigurationInfo storageConfig,
      GoogleCredentials sourceCredentials,
      GcpCredentialOps credentialOps) {
    Optional<GcpAttributionParams> attributionParams = key.attributionParams();
    if (attributionParams.isPresent()) {
      GcpAttributionParams params = attributionParams.get();
      String subject =
          GcpAttributionSubjectBuilder.buildSubject(key.realmId(), key.principalName().orElse(""));
      GcpFederatedCredentialsExchanger exchanger =
          new GcpFederatedCredentialsExchanger(
              params.tokenIssuer(),
              params.wifAudience(),
              params.signingKeyPath(),
              params.signingKeyId(),
              key.transportFactory());
      GoogleCredentials federated = exchanger.federatedCredentials(subject, key.realmId());
      return createImpersonatedCredentials(
          federated, storageConfig.getGcpServiceAccount(), credentialOps);
    }
    return getBaseCredentials(storageConfig, sourceCredentials, credentialOps);
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

  /**
   * Creates the missing parent folders of each write location on Hierarchical Namespace (HNS)
   * buckets, using the identity the vended token is downscoped from. HNS creates missing parent
   * folders when an object is written, which needs {@code storage.folders.create} on each of them;
   * the vended token does not get that permission outside its write locations, because a folder
   * rename is authorized by the same permission on its destination. Creating the parents here keeps
   * the token's folder access inside its write locations, as on buckets without HNS.
   *
   * <p>Failures never fail the vend: they leave the token unchanged, and writes under a
   * still-missing parent fail until it exists. Creation is tried again whenever new credentials are
   * generated. A bucket without HNS rejects the call with 409, or with 403 when the identity lacks
   * {@code storage.folders.create}. Runs only when {@code GCS_HNS_PARENT_FOLDER_CREATION_ENABLED}
   * is true, which is not the default.
   */
  private static void createParentFolders(
      Set<String> writeLocations,
      GoogleCredentials credentials,
      HttpTransportFactory transportFactory) {
    parentFoldersToCreate(writeLocations)
        .forEach((bucket, folders) ->
            folders.forEach(folder -> createFolder(transportFactory, credentials, bucket, folder)));
  }

  private static void createFolder(
      HttpTransportFactory transportFactory,
      GoogleCredentials credentials,
      String bucket,
      String folder) {
    // The client is built inside the try, so that a failure to build it is logged like a failed
    // request and does not fail the vend.
    try {
      HttpCredentialsAdapter credentialsAdapter = new HttpCredentialsAdapter(credentials);
      Storage storage =
          new Storage.Builder(
                  transportFactory.create(),
                  GsonFactory.getDefaultInstance(),
                  request -> {
                    credentialsAdapter.initialize(request);
                    request
                        .setConnectTimeout(FOLDER_CREATE_TIMEOUT_MS)
                        .setReadTimeout(FOLDER_CREATE_TIMEOUT_MS);
                  })
              .setApplicationName("Apache Polaris")
              .build();
      int status;
      try {
        HttpResponse response =
            storage
                .folders()
                .insert(bucket, new Folder().setName(folder))
                .setRecursive(true)
                .setDisableGZipContent(true)
                .executeUnparsed();
        try {
          status = response.getStatusCode();
        } finally {
          response.disconnect();
        }
      } catch (HttpResponseException e) {
        status = e.getStatusCode();
      }
      // 409: the folder already exists, or the bucket has no hierarchical namespace.
      if (status == 200 || status == 409) {
        LOGGER.debug("Parent folder {} in GCS bucket {}: HTTP {}", folder, bucket, status);
      } else {
        warnFolderNotCreated(bucket, folder, "HTTP " + status, null);
      }
    } catch (IOException | RuntimeException e) {
      warnFolderNotCreated(bucket, folder, e.getClass().getSimpleName(), e);
    }
  }

  private static void warnFolderNotCreated(
      String bucket, String folder, String reason, Exception cause) {
    // Logged on every failure: a bucket without hierarchical namespace also answers 403 here when
    // the identity lacks storage.folders.create, and that repeats on each credential generation.
    LOGGER.warn(
        "Could not create parent folder {} in GCS bucket {} ({}). If the bucket has hierarchical"
            + " namespace enabled, writes under this folder fail until it exists. Polaris tries again"
            + " whenever it generates new credentials, which needs storage.folders.create on the"
            + " bucket for the identity it downscopes from. Buckets without hierarchical namespace"
            + " are not affected.",
        folder,
        bucket,
        reason,
        cause);
  }

  /**
   * Returns, per bucket, the parent folder of each write location: {@code a/b/tbl/} gives {@code
   * a/b/}. Bucket-root and single-segment locations have no parent. A parent that is an ancestor of
   * another parent in the same bucket is dropped, because creating the deeper one recursively also
   * creates it. A bucket can still have several parents when its write locations are in different
   * subtrees: {@code gs://b/ns/db/tbl/} with a {@code write.data.path} of {@code
   * gs://b/other/data/} gives {@code ns/db/} and {@code other/}.
   */
  @VisibleForTesting
  static Map<String, Set<String>> parentFoldersToCreate(Set<String> writeLocations) {
    Map<String, TreeSet<String>> parentsByBucket = new TreeMap<>();
    for (String location : writeLocations) {
      StorageUri uri = StorageUri.parse(location);
      String path = ensureTrailingSlash(trimLeadingSlash(uri.rawPath()));
      int parentEnd = path.lastIndexOf('/', path.length() - 2);
      if (parentEnd > 0) {
        parentsByBucket
            .computeIfAbsent(uri.authority(), key -> new TreeSet<>())
            .add(path.substring(0, parentEnd + 1));
      }
    }
    // In sorted order a folder's descendants directly follow it, so a parent is an ancestor of
    // another parent exactly when the next parent starts with it.
    Map<String, Set<String>> leafParents = new TreeMap<>();
    parentsByBucket.forEach((bucket, parents) -> {
      for (String parent : parents) {
        String next = parents.higher(parent);
        if (next == null || !next.startsWith(parent)) {
          leafParents.computeIfAbsent(bucket, key -> new TreeSet<>()).add(parent);
        }
      }
    });
    return leafParents;
  }

  private static String convertToString(CredentialAccessBoundary accessBoundary) {
    try {
      return OBJECT_MAPPER.writeValueAsString(accessBoundary);
    } catch (JacksonException e) {
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
   *
   * <p>On Hierarchical Namespace (HNS) buckets, the write rule also covers {@code folders/<path>/},
   * so folders inside a write location can be created, renamed and deleted, as directories can on
   * buckets without HNS. No folder outside the write locations is covered: the missing parent
   * folders are created by Polaris before the token is vended, when {@code
   * GCS_HNS_PARENT_FOLDER_CREATION_ENABLED} is true (see {@code compute}). Buckets without HNS have
   * no folder resources, so the folder conditions grant nothing there.
   */
  @VisibleForTesting
  public static CredentialAccessBoundary generateAccessBoundaryRules(
      @NonNull Set<String> allowedReadLocations,
      @NonNull Set<String> allowedListLocations,
      @NonNull Set<String> allowedWriteLocations) {
    Map<String, LinkedHashSet<String>> readConditionsByBucket = new LinkedHashMap<>();
    Map<String, LinkedHashSet<String>> writeConditionsByBucket = new LinkedHashMap<>();
    HashSet<String> bucketsWithList = new HashSet<>();

    Stream.concat(allowedReadLocations.stream(), allowedListLocations.stream())
        .distinct()
        .forEach(location -> {
          StorageUri uri = StorageUri.parse(location);
          String bucket = uri.authority();
          // Treat the granted path as a directory prefix: a trailing slash is required so
          // that the downstream startsWith() CEL conditions cannot be satisfied by sibling
          // objects or list prefixes that merely share the granted path as a string prefix
          // (e.g. a grant on "data/" must not authorize access to "data_foo/*)".
          String path = ensureTrailingSlash(trimLeadingSlash(uri.rawPath()));
          readConditionsByBucket
              .computeIfAbsent(bucket, key -> new LinkedHashSet<>())
              .add(resourceNameStartsWithExpression(bucket, path));
        });

    allowedListLocations.forEach(location -> {
      StorageUri uri = StorageUri.parse(location);
      String bucket = uri.authority();
      String path = ensureTrailingSlash(trimLeadingSlash(uri.rawPath()));
      readConditionsByBucket
          .computeIfAbsent(bucket, key -> new LinkedHashSet<>())
          .add(objectListPrefixStartsWithExpression(path));
      bucketsWithList.add(bucket);
    });

    allowedWriteLocations.forEach(location -> {
      StorageUri uri = StorageUri.parse(location);
      String bucket = uri.authority();
      String path = ensureTrailingSlash(trimLeadingSlash(uri.rawPath()));
      LinkedHashSet<String> conditions =
          writeConditionsByBucket.computeIfAbsent(bucket, key -> new LinkedHashSet<>());
      conditions.add(resourceNameStartsWithExpression(bucket, path));
      // HNS buckets authorize storage.folders.* (from legacyBucketWriter) on folders/<path>. The
      // parent folders get no rule: folders.create there would also authorize renaming a folder
      // from this location into a missing parent of another location.
      conditions.add(folderNameStartsWithExpression(bucket, path));
    });

    CredentialAccessBoundary.Builder accessBoundaryBuilder = CredentialAccessBoundary.newBuilder();
    readConditionsByBucket.forEach((bucket, conditions) -> {
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
    writeConditionsByBucket.forEach((bucket, conditions) -> {
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
    return accessBoundaryBuilder.build();
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
