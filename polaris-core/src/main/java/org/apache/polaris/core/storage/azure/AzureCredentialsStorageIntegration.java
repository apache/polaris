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
package org.apache.polaris.core.storage.azure;

import static org.apache.polaris.core.config.FeatureConfiguration.AZURE_RETRY_COUNT;
import static org.apache.polaris.core.config.FeatureConfiguration.AZURE_RETRY_DELAY_MILLIS;
import static org.apache.polaris.core.config.FeatureConfiguration.AZURE_RETRY_JITTER_FACTOR;
import static org.apache.polaris.core.config.FeatureConfiguration.AZURE_TIMEOUT_MILLIS;
import static org.apache.polaris.core.config.FeatureConfiguration.STORAGE_CREDENTIAL_DURATION_SECONDS;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.UserDelegationKey;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import com.azure.storage.file.datalake.DataLakePathClientBuilder;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.sas.DataLakeServiceSasSignatureValues;
import com.azure.storage.file.datalake.sas.PathSasPermission;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.polaris.core.StructuredLogKeys;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.CachingStorageIntegration;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.LocationGrant;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.cache.StorageCredentialCacheKey;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/** Azure credential vendor that supports generating SAS token */
public class AzureCredentialsStorageIntegration
    extends CachingStorageIntegration<AzureStorageConfigurationInfo> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AzureCredentialsStorageIntegration.class);

  // Microsoft's Java user-delegation SAS example backdates the key start time to account for
  // clock skew. See
  // https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blob-user-delegation-sas-create-java.
  private static final long SAS_CLOCK_SKEW_BUFFER_SECONDS = Duration.ofMinutes(5).toSeconds();

  final DefaultAzureCredential defaultAzureCredential;

  public AzureCredentialsStorageIntegration(
      AzureStorageConfigurationInfo storageConfig, RealmConfig realmConfig) {
    this(null, storageConfig, realmConfig);
  }

  public AzureCredentialsStorageIntegration(
      org.apache.polaris.core.storage.cache.StorageCredentialCache cache,
      AzureStorageConfigurationInfo storageConfig,
      RealmConfig realmConfig) {
    super(cache, realmConfig, storageConfig);
    // The DefaultAzureCredential will by default load the environment variables for client id,
    // client secret, tenant id
    defaultAzureCredential = new DefaultAzureCredentialBuilder().build();
  }

  @Override
  protected StorageCredentialCacheKey buildCacheKey(
      @NonNull List<LocationGrant> grants,
      @NonNull Optional<String> refreshEndpoint,
      @NonNull CredentialVendingContext context) {
    return buildCacheKey(
        allowList(grants), readLocations(grants), writeLocations(grants), refreshEndpoint, context);
  }

  private static boolean allowList(List<LocationGrant> grants) {
    return grants.stream()
        .flatMap(g -> g.actions().stream())
        .anyMatch(a -> a == PolarisStorageActions.LIST || a == PolarisStorageActions.ALL);
  }

  private static Set<String> readLocations(List<LocationGrant> grants) {
    return grants.stream().flatMap(g -> g.locations().stream()).collect(Collectors.toSet());
  }

  private static Set<String> writeLocations(List<LocationGrant> grants) {
    return grants.stream()
        .filter(
            g ->
                g.actions().contains(PolarisStorageActions.WRITE)
                    || g.actions().contains(PolarisStorageActions.DELETE)
                    || g.actions().contains(PolarisStorageActions.ALL))
        .flatMap(g -> g.locations().stream())
        .collect(Collectors.toSet());
  }

  private AzureStorageCredentialCacheKey buildCacheKey(
      boolean allowList,
      @NonNull Set<String> locations,
      @NonNull Set<String> writeLocations,
      @NonNull Optional<String> refreshEndpoint,
      @NonNull CredentialVendingContext context) {
    return AzureStorageCredentialCacheKey.of(
        context.realm().orElse(""),
        storageConfig(),
        allowList,
        locations,
        writeLocations,
        refreshEndpoint,
        defaultAzureCredential,
        realmConfig());
  }

  /** Mint a fresh {@link StorageAccessConfig} for the given Azure cache key. */
  static StorageAccessConfig compute(AzureStorageCredentialCacheKey key) {
    RealmConfig realmConfig = key.realmConfig();
    AzureStorageConfigurationInfo azureStorageConfig = key.storageConfig();
    DefaultAzureCredential defaultAzureCredential = key.defaultAzureCredential();
    boolean allowList = key.allowedListAction();
    Set<String> locations = key.allowedReadLocations();
    Set<String> writeLocations = key.allowedWriteLocations();
    Optional<String> refreshEndpoint = key.refreshCredentialsEndpoint();

    Set<String> allLocations = new LinkedHashSet<>();
    allLocations.addAll(locations);
    allLocations.addAll(writeLocations);
    if (allLocations.isEmpty()) {
      throw new IllegalArgumentException("Expect valid location");
    }

    Instant start = Instant.now();

    // Backdate the user delegation key start time by five minutes, following Microsoft's Java
    // user-delegation SAS example, to prevent authorization failures caused by clock skew.
    // Microsoft's general SAS guidance recommends a 15-minute backdate for an explicit SAS start
    // time; Polaris does not set a SAS start time, and this buffer applies only to the key. The
    // key is limited to Azure's seven-day validity window, with a one-minute end-time margin.
    Instant clockSkewAdjustedStart = getClockSkewAdjustedStart(start);
    OffsetDateTime startTime =
        clockSkewAdjustedStart.truncatedTo(ChronoUnit.SECONDS).atOffset(ZoneOffset.UTC);
    int intendedDurationSeconds = realmConfig.getConfig(STORAGE_CREDENTIAL_DURATION_SECONDS);
    OffsetDateTime intendedEndTime =
        start.plusSeconds(intendedDurationSeconds).atOffset(ZoneOffset.UTC);
    OffsetDateTime maxAllowedEndTime =
        clockSkewAdjustedStart.plus(Period.ofDays(7)).minusSeconds(60).atOffset(ZoneOffset.UTC);
    OffsetDateTime sanitizedEndTime =
        intendedEndTime.isBefore(maxAllowedEndTime) ? intendedEndTime : maxAllowedEndTime;

    LOGGER
        .atDebug()
        .addKeyValue(StructuredLogKeys.ALLOWED_LIST_ACTION, allowList)
        .addKeyValue(StructuredLogKeys.LOCATIONS, locations)
        .addKeyValue(StructuredLogKeys.WRITE_LOCATIONS, writeLocations)
        .log("Subscope Azure SAS");

    StorageAccessConfig.Builder accessConfig = StorageAccessConfig.builder();
    // A single access token is reused for every storage scope, since the token is scoped to the
    // Azure tenant and not to an individual storage account.
    AccessToken accessToken =
        getAccessToken(defaultAzureCredential, realmConfig, azureStorageConfig.getTenantId());
    // Credentials are vended per storage account / container so that metadata and data may live in
    // different storage accounts and containers. Each distinct scope gets a SAS token whose read
    // and write permissions are derived only from the locations that fall inside that scope.
    Map<AzureLocationScope, ScopedLocations> scopes =
        groupByStorageScope(locations, writeLocations);
    // The account-name keys (bare and endpoint-less) identify a storage account rather than a
    // container, so they can only be emitted when every vended scope belongs to one account.
    boolean singleStorageAccount =
        scopes.keySet().stream()
                .map(scope -> scope.location().getStorageAccount())
                .distinct()
                .count()
            == 1;
    boolean first = true;
    // Credentials are collected in a plain map first: two containers in the same storage account
    // map
    // onto the same account-host key, and a duplicate key must not fail credential vending.
    Map<String, String> credentials = new LinkedHashMap<>();
    for (Map.Entry<AzureLocationScope, ScopedLocations> entry : scopes.entrySet()) {
      // Account-scoped keys (bare and account-name-suffixed) are emitted once, for the first scope
      // only, and only when a single account is being vended.
      boolean emitAccountScopedKeys = singleStorageAccount && first;
      first = false;
      AzureLocationScope scope = entry.getKey();
      AzureLocation location = scope.location();
      ScopedLocations scoped = entry.getValue();

      BlobSasPermission blobSasPermission = new BlobSasPermission();
      // pathSasPermission is for Data lake storage
      PathSasPermission pathSasPermission = new PathSasPermission();

      if (allowList) {
        // container level
        blobSasPermission.setListPermission(true);
        pathSasPermission.setListPermission(true);
      }
      if (!scoped.readLocations().isEmpty()) {
        blobSasPermission.setReadPermission(true);
        pathSasPermission.setReadPermission(true);
      }
      if (!scoped.writeLocations().isEmpty()) {
        blobSasPermission.setAddPermission(true);
        blobSasPermission.setWritePermission(true);
        blobSasPermission.setDeletePermission(true);
        pathSasPermission.setAddPermission(true);
        pathSasPermission.setWritePermission(true);
        pathSasPermission.setDeletePermission(true);
      }

      LOGGER
          .atDebug()
          .addKeyValue(StructuredLogKeys.LOCATION, location.withoutScheme())
          .addKeyValue(StructuredLogKeys.STORAGE_ACCOUNT, location.getStorageAccount())
          .addKeyValue(StructuredLogKeys.ENDPOINT, location.getEndpoint())
          .addKeyValue(StructuredLogKeys.CONTAINER, location.getContainer())
          .addKeyValue(StructuredLogKeys.FILE_PATH, location.getFilePath())
          .addKeyValue(StructuredLogKeys.READ_LOCATIONS, new HashSet<>(scoped.readLocations()))
          .addKeyValue(StructuredLogKeys.WRITE_LOCATIONS, new HashSet<>(scoped.writeLocations()))
          .log("Subscope Azure SAS");
      String sasToken;
      if (location.isBlob()) {
        sasToken =
            getBlobUserDelegationSas(
                startTime,
                sanitizedEndTime,
                sanitizedEndTime,
                location.getStorageDnsName(),
                location.getContainer(),
                blobSasPermission,
                Mono.just(accessToken));
      } else if (location.isAdls()) {
        String path = null;
        if (Boolean.TRUE.equals(azureStorageConfig.isHierarchical())) {
          Preconditions.checkArgument(
              scoped.readLocations().size() <= 1,
              "Allowed read locations must not have more that one entry per storage scope");
          Preconditions.checkArgument(
              scoped.writeLocations().size() <= 1,
              "Allowed write locations must not have more that one entry per storage scope");
          path = location.getFilePath();
        }

        sasToken =
            getAdlsUserDelegationSas(
                startTime,
                sanitizedEndTime,
                sanitizedEndTime,
                location.getStorageDnsName(),
                location.getContainer(),
                pathSasPermission,
                path,
                Mono.just(accessToken));
      } else {
        throw new RuntimeException(
            String.format("Endpoint %s not supported", location.getEndpoint()));
      }

      handleAzureCredential(
          credentials, sasToken, location, sanitizedEndTime.toInstant(), emitAccountScopedKeys);
    }

    credentials.forEach(accessConfig::putCredential);
    accessConfig.expiresAt(sanitizedEndTime.toInstant());
    refreshEndpoint.ifPresent(
        endpoint ->
            accessConfig.put(StorageAccessProperty.AZURE_REFRESH_CREDENTIALS_ENDPOINT, endpoint));
    return accessConfig.build();
  }

  /**
   * Vends one SAS token per storage account / container / endpoint scope. Two locations share a
   * scope only when they resolve to the same storage account, container and endpoint, because a
   * single Azure SAS token cannot span accounts or containers.
   */
  private static Map<AzureLocationScope, ScopedLocations> groupByStorageScope(
      Set<String> readLocations, Set<String> writeLocations) {
    Map<AzureLocationScope, ScopedLocations> grouped = new LinkedHashMap<>();
    for (String loc : readLocations) {
      grouped.computeIfAbsent(AzureLocationScope.of(loc), k -> new ScopedLocations()).addRead(loc);
    }
    for (String loc : writeLocations) {
      grouped.computeIfAbsent(AzureLocationScope.of(loc), k -> new ScopedLocations()).addWrite(loc);
    }
    return grouped;
  }

  /** Locations sharing the same storage account, container and endpoint. */
  private record AzureLocationScope(AzureLocation location) {
    static AzureLocationScope of(String location) {
      // schema://<container_name>@<account_name>.<endpoint>/<file_path>
      AzureLocation azureLocation = new AzureLocation(location);
      if (azureLocation.getFilePath() != null && !azureLocation.getFilePath().isEmpty()) {
        // Scope the SAS token at the container level, which is what a single SAS token can cover.
        azureLocation =
            new AzureLocation(
                azureLocation.getScheme()
                    + "://"
                    + azureLocation.getContainer()
                    + "@"
                    + azureLocation.getStorageDnsName());
      }
      return new AzureLocationScope(azureLocation);
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof AzureLocationScope other && location.equals(other.location);
    }

    @Override
    public int hashCode() {
      return location.hashCode();
    }
  }

  /** The read and write locations belonging to a single storage scope. */
  private static final class ScopedLocations {
    private final Set<String> readLocations = new LinkedHashSet<>();
    private final Set<String> writeLocations = new LinkedHashSet<>();

    ScopedLocations addRead(String location) {
      readLocations.add(location);
      return this;
    }

    ScopedLocations addWrite(String location) {
      writeLocations.add(location);
      return this;
    }

    Set<String> readLocations() {
      return readLocations;
    }

    Set<String> writeLocations() {
      return writeLocations;
    }
  }

  @VisibleForTesting
  static Instant getClockSkewAdjustedStart(Instant start) {
    return start.minusSeconds(SAS_CLOCK_SKEW_BUFFER_SECONDS);
  }

  @VisibleForTesting
  static StorageAccessConfig toAccessConfig(
      String sasToken,
      AzureLocation location,
      Instant expiresAt,
      Optional<String> refreshCredentialsEndpoint) {
    StorageAccessConfig.Builder accessConfig = StorageAccessConfig.builder();
    Map<String, String> credentials = new LinkedHashMap<>();
    handleAzureCredential(credentials, sasToken, location, expiresAt, true);
    credentials.forEach(accessConfig::putCredential);
    accessConfig.expiresAt(expiresAt);
    refreshCredentialsEndpoint.ifPresent(
        endpoint -> {
          accessConfig.put(StorageAccessProperty.AZURE_REFRESH_CREDENTIALS_ENDPOINT, endpoint);
        });
    return accessConfig.build();
  }

  private static void handleAzureCredential(
      Map<String, String> credentials,
      String sasToken,
      AzureLocation location,
      Instant expiresAt,
      boolean emitAccountScopedKeys) {
    String storageDnsName = location.getStorageDnsName();
    String accountName = location.getStorageAccount();

    // The per-account-host SAS key is the only key that can carry a container-scoped token, so when
    // two containers share a storage account the later scope wins. The token is still restricted to
    // one container, and clients that need every container use the hosted credential refresh path.
    // Duplicate keys must therefore be tolerated here rather than rejected by the immutable
    // builder.
    String accountHostKey =
        StorageAccessProperty.AZURE_SAS_TOKEN_ACCOUNT_HOST.getPropertyName() + "." + storageDnsName;
    String previous = credentials.put(accountHostKey, sasToken);
    if (previous != null && !previous.equals(sasToken)) {
      LOGGER.debug(
          "Replacing credential {} because another container in the same storage account was vended first",
          accountHostKey);
    }
    // The expiry key is likewise account scoped: every scope minted by this call shares one expiry,
    // so writing it more than once is redundant and must not happen for two containers in one
    // account.
    if (emitAccountScopedKeys) {
      credentials.put(
          StorageAccessProperty.AZURE_SAS_TOKEN_EXPIRES_AT_MS.getPropertyName()
              + "."
              + storageDnsName,
          String.valueOf(expiresAt.toEpochMilli()));
    }

    // The keys below identify a storage account rather than a container, so a single value can only
    // represent a single account. They are emitted only for the first scope, and only when every
    // vended scope shares one account. Otherwise they would silently claim that one account's token
    // covers the others; clients that span accounts must use the per-account-host keys above.
    if (!emitAccountScopedKeys) {
      return;
    }

    // Iceberg 1.7.x may expect the credential key to _not_ be suffixed with endpoint.
    // Use accountName (from location) for the stripped variant.
    if (location.isAdls() || location.isBlob()) {
      credentials.put(
          StorageAccessProperty.AZURE_SAS_TOKEN_ACCOUNT_NAME.getPropertyName() + "." + accountName,
          sasToken);
    }

    // PyIceberg and other clients need bare adls.sas-token and adls.account-name for compatibility
    // with adlfs/fsspec (and similar libraries). The bare keys are emitted in addition to the
    // suffixed variants used by Spark.
    // See https://github.com/apache/polaris/issues/418
    credentials.put(StorageAccessProperty.AZURE_SAS_TOKEN_BARE.getPropertyName(), sasToken);
    credentials.put(StorageAccessProperty.AZURE_ACCOUNT_NAME.getPropertyName(), accountName);
  }

  private static String getBlobUserDelegationSas(
      OffsetDateTime startTime,
      OffsetDateTime keyEndtime,
      OffsetDateTime sasExpiry,
      String storageDnsName,
      String container,
      BlobSasPermission blobSasPermission,
      Mono<AccessToken> accessTokenMono) {
    String endpoint = "https://" + storageDnsName;
    try {
      BlobServiceClient serviceClient =
          new BlobServiceClientBuilder()
              .endpoint(endpoint)
              .credential(c -> accessTokenMono)
              .buildClient();
      UserDelegationKey userDelegationKey =
          serviceClient.getUserDelegationKey(startTime, keyEndtime);
      BlobServiceSasSignatureValues sigValues =
          new BlobServiceSasSignatureValues(sasExpiry, blobSasPermission);
      // scoped to the container
      return new BlobContainerClientBuilder()
          .endpoint(endpoint)
          .containerName(container)
          .buildClient()
          .generateUserDelegationSas(sigValues, userDelegationKey);
    } catch (BlobStorageException ex) {
      LOGGER.debug(
          "Azure DataLakeStorageException for getBlobUserDelegationSas. keyStart={} keyEnd={}, storageDns={}, container={}",
          startTime,
          keyEndtime,
          storageDnsName,
          container,
          ex);
      throw ex;
    }
  }

  private static String getAdlsUserDelegationSas(
      OffsetDateTime startTime,
      OffsetDateTime endTime,
      OffsetDateTime sasExpiry,
      String storageDnsName,
      String fileSystemNameOrContainer,
      PathSasPermission pathSasPermission,
      String path,
      Mono<AccessToken> accessTokenMono) {
    String endpoint = "https://" + storageDnsName;
    try {
      DataLakeServiceClient dataLakeServiceClient =
          new DataLakeServiceClientBuilder()
              .endpoint(endpoint)
              .credential(c -> accessTokenMono)
              .buildClient();
      com.azure.storage.file.datalake.models.UserDelegationKey userDelegationKey =
          dataLakeServiceClient.getUserDelegationKey(startTime, endTime);

      DataLakeServiceSasSignatureValues signatureValues =
          new DataLakeServiceSasSignatureValues(sasExpiry, pathSasPermission);

      if (path != null) {
        return new DataLakePathClientBuilder()
            .endpoint(endpoint)
            .fileSystemName(fileSystemNameOrContainer)
            .pathName(path)
            .buildDirectoryClient()
            .generateUserDelegationSas(signatureValues, userDelegationKey);

      } else {
        return new DataLakeFileSystemClientBuilder()
            .endpoint(endpoint)
            .fileSystemName(fileSystemNameOrContainer)
            .buildClient()
            .generateUserDelegationSas(signatureValues, userDelegationKey);
      }
    } catch (DataLakeStorageException ex) {
      LOGGER.debug(
          "Azure DataLakeStorageException for getAdlsUserDelegationSas. keyStart={} keyEnd={}, storageDns={}, fileSystemName={}",
          startTime,
          endTime,
          storageDnsName,
          fileSystemNameOrContainer,
          ex);
      throw ex;
    }
  }

  /**
   * Fetches an Azure AD access token with timeout and retry logic to handle transient failures.
   *
   * <p>This access token is used internally to obtain a user delegation key from Azure Storage,
   * which is then used to generate SAS tokens for client credential vending.
   *
   * <p>This method implements a defensive strategy against slow or failing cloud provider requests:
   *
   * <ul>
   *   <li>Per-attempt timeout (configurable via AZURE_TIMEOUT_MILLIS, default 15000ms)
   *   <li>Exponential backoff retry (configurable count and initial delay via AZURE_RETRY_COUNT and
   *       AZURE_RETRY_DELAY_MILLIS, defaults: 3 attempts starting at 2000ms)
   *   <li>Jitter to prevent thundering herd (configurable via AZURE_RETRY_JITTER_FACTOR, default
   *       0.5 = 50%%)
   * </ul>
   *
   * @param realmConfig the realm configuration to get timeout and retry settings
   * @param tenantId the Azure tenant ID
   * @return the access token
   * @throws RuntimeException if token fetch fails after all retries or times out
   */
  private static AccessToken getAccessToken(
      DefaultAzureCredential defaultAzureCredential, RealmConfig realmConfig, String tenantId) {
    int timeoutMillis = realmConfig.getConfig(AZURE_TIMEOUT_MILLIS);
    int retryCount = realmConfig.getConfig(AZURE_RETRY_COUNT);
    int initialDelayMillis = realmConfig.getConfig(AZURE_RETRY_DELAY_MILLIS);
    double jitter = realmConfig.getConfig(AZURE_RETRY_JITTER_FACTOR);
    int maxAttempts = retryCount + 1;

    String scope = "https://storage.azure.com/.default";
    AccessToken accessToken =
        defaultAzureCredential
            .getToken(new TokenRequestContext().addScopes(scope).setTenantId(tenantId))
            .timeout(Duration.ofMillis(timeoutMillis))
            .doOnError(
                error ->
                    LOGGER.warn("Error fetching Azure access token for tenant {}", tenantId, error))
            .retryWhen(
                Retry.backoff(retryCount, Duration.ofMillis(initialDelayMillis))
                    .jitter(jitter) // Apply jitter factor to computed delay
                    .filter(AzureCredentialsStorageIntegration::isRetriableAzureException)
                    .doBeforeRetry(
                        retrySignal ->
                            LOGGER.info(
                                "Retrying Azure token fetch for tenant {} (attempt {}/{})",
                                tenantId,
                                retrySignal.totalRetries() + 1,
                                maxAttempts))
                    .onRetryExhaustedThrow(
                        (retryBackoffSpec, retrySignal) ->
                            new RuntimeException(
                                String.format(
                                    "Azure token fetch exhausted after %d attempts for tenant %s",
                                    retrySignal.totalRetries(), tenantId),
                                retrySignal.failure())))
            .blockOptional()
            .orElse(null);

    if (accessToken == null) {
      throw new RuntimeException(
          String.format("Failed to fetch Azure access token for tenant %s", tenantId));
    }
    return accessToken;
  }

  /**
   * Determines if an exception is retriable for Azure token requests.
   *
   * <p>Retries are attempted for:
   *
   * <ul>
   *   <li>TimeoutException - per-attempt timeout exceeded
   *   <li>AADSTS50058 - Token endpoint timeout
   *   <li>AADSTS50078 - Service temporarily unavailable
   *   <li>AADSTS700084 - Token refresh required
   *   <li>503 - Service unavailable
   *   <li>429 - Too many requests (rate limited)
   * </ul>
   *
   * @param throwable the exception to check
   * @return true if the exception should trigger a retry
   */
  private static boolean isRetriableAzureException(Throwable throwable) {
    // Retry on timeout exceptions
    if (throwable instanceof java.util.concurrent.TimeoutException) {
      return true;
    }
    // Retry on common transient Azure credential exceptions
    String message = throwable.getMessage();
    if (message != null) {
      return message.contains("AADSTS50058") // Token endpoint timeout
          || message.contains("AADSTS50078") // Service temporarily unavailable
          || message.contains("AADSTS700084") // Token refresh required
          || message.contains("503") // Service unavailable
          || message.contains("429"); // Too many requests
    }
    return false;
  }
}
