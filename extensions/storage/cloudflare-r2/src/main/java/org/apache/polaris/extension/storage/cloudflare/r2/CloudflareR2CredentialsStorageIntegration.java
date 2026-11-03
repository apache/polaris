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
package org.apache.polaris.extension.storage.cloudflare.r2;

import static org.apache.polaris.core.config.FeatureConfiguration.STORAGE_CREDENTIAL_DURATION_SECONDS;

import java.time.Clock;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.CachingStorageIntegration;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.LocationGrant;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.StorageUri;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.core.storage.cache.StorageCredentialCacheKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Vends Cloudflare R2 temporary credentials for an S3 catalog whose {@code
 * credentialVendingMechanism} is {@code R2_LOCAL_SIGNING}, scoped to one bucket and the table's key
 * prefixes. Credentials are minted locally from the server-side parent token (see {@link
 * CloudflareR2TemporaryCredentialSigner}); no call to Cloudflare is made. The token subject and
 * audience come from the catalog's {@code endpoint}. Each time a credential is minted, the
 * integration checks the {@code endpoint}, {@code pathStyleAccess}, and {@code region} again, so a
 * stored config that never passed the mechanism's {@code validate} fails closed.
 */
public class CloudflareR2CredentialsStorageIntegration
    extends CachingStorageIntegration<AwsStorageConfigurationInfo> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CloudflareR2CredentialsStorageIntegration.class);

  private static final Set<PolarisStorageActions> WRITE_ACTIONS =
      EnumSet.of(
          PolarisStorageActions.WRITE, PolarisStorageActions.DELETE, PolarisStorageActions.ALL);

  private final CloudflareR2ParentTokenResolver parentTokenResolver;
  private final Clock clock;

  public CloudflareR2CredentialsStorageIntegration(
      @NonNull CloudflareR2ParentTokenResolver parentTokenResolver,
      @NonNull Clock clock,
      @Nullable StorageCredentialCache cache,
      @NonNull AwsStorageConfigurationInfo storageConfig,
      @NonNull RealmConfig realmConfig) {
    super(cache, realmConfig, storageConfig);
    this.parentTokenResolver = parentTokenResolver;
    this.clock = clock;
  }

  /**
   * Builds the cache key. The parent token comes from the storage name that {@link
   * CloudflareR2ParentTokenResolver#effectiveStorageName} returns: the catalog's name when the
   * realm resolves credentials by storage name, else null for the default token.
   */
  @Override
  protected StorageCredentialCacheKey buildCacheKey(
      @NonNull List<LocationGrant> grants, @NonNull CredentialVendingContext context) {
    AwsStorageConfigurationInfo config = storageConfig();
    String storageName =
        CloudflareR2ParentTokenResolver.effectiveStorageName(realmConfig(), config);
    CloudflareR2ParentToken token =
        parentTokenResolver
            .resolve(storageName)
            .orElseThrow(() -> noParentToken(storageName, context));
    Duration ttl = Duration.ofSeconds(realmConfig().getConfig(STORAGE_CREDENTIAL_DURATION_SECONDS));
    requireUniformScope(grants, context);
    return CloudflareR2CredentialCacheKey.of(
        context.realm().orElse(""),
        config,
        singleBucket(grants, context),
        normalizedPrefixes(grants),
        scopeFor(grants),
        token.accessKeyId(),
        CloudflareR2ParentToken.fingerprint(token.secretAccessKey()),
        ttl,
        token.secretAccessKey(),
        clock,
        realmConfig());
  }

  /**
   * Adds the refresh endpoint after the cache lookup, so it is not part of the cache key and two
   * refresh endpoints share one cached credential.
   */
  @Override
  protected StorageAccessConfig addExtraProperties(
      @NonNull StorageAccessConfig accessConfig, @NonNull Optional<String> refreshEndpoint) {
    return refreshEndpoint
        .map(
            endpoint ->
                withExtraProperty(
                    accessConfig, StorageAccessProperty.AWS_REFRESH_CREDENTIALS_ENDPOINT, endpoint))
        .orElse(accessConfig);
  }

  /**
   * Reports a vend attempt against a storage name the server holds no parent token for. The
   * exception message reaches the client, so it names no server property. The WARN line holds the
   * detailed message, which names the properties an operator has to set.
   */
  private static IllegalArgumentException noParentToken(
      @Nullable String storageName, CredentialVendingContext context) {
    LOGGER.warn(
        "{} (catalog {})",
        CloudflareR2ParentTokenResolver.missingTokenMessage(storageName),
        context.catalogName().orElse("<none>"));
    return new IllegalArgumentException(
        storageName == null
            ? "No Cloudflare R2 parent token is configured on the server"
            : "Storage name '"
                + storageName
                + "' has no Cloudflare R2 parent token configured on the server");
  }

  /**
   * Mint a fresh {@link StorageAccessConfig} for the given key. Called by the cache on miss, so the
   * DEBUG line below is one per mint rather than one per credential request: a request served from
   * the cache logs nothing. The line prints the effective storage name, the one the parent token
   * was resolved with, or {@code <default>} when the realm does not resolve by name. It names the
   * credential's shape and never its material: no key id, no secret, no session token, no JWT.
   *
   * <p>This method checks the endpoint, the path style, and the region again at vend time, so a
   * stored config that never passed the mechanism's {@code validate} fails closed with an {@link
   * IllegalArgumentException}. The client receives the catalog's endpoint, {@code
   * s3.path-style-access=true}, and {@code client.region=auto}.
   */
  static StorageAccessConfig compute(CloudflareR2CredentialCacheKey key) {
    AwsStorageConfigurationInfo config = key.storageConfig();
    String effective =
        CloudflareR2ParentTokenResolver.effectiveStorageName(key.realmConfig(), config);
    CloudflareR2Endpoint endpoint = CloudflareR2Endpoint.parse(config.getEndpoint());
    R2LocalSigningCredentialVendingMechanism.requirePathStyleAndAutoRegion(config);
    CloudflareR2TemporaryCredentialSigner.Credential credential =
        CloudflareR2TemporaryCredentialSigner.sign(
            new CloudflareR2TemporaryCredentialSigner.Request(
                key.parentKeyId(),
                key.parentSecret(),
                endpoint.accountId(),
                endpoint.host(),
                key.bucket(),
                key.prefixes(),
                key.scope(),
                key.ttl(),
                key.clock().instant()));
    LOGGER.debug(
        "Minted R2 credential: storageName={} bucket={} prefixes={} scope={} ttl={}s expiresAt={}",
        effective == null ? "<default>" : effective,
        key.bucket(),
        key.prefixes().size(),
        key.scope(),
        key.ttl().toSeconds(),
        credential.expiresAt());
    StorageAccessConfig.Builder builder = StorageAccessConfig.builder();
    builder.put(StorageAccessProperty.AWS_KEY_ID, credential.accessKeyId());
    builder.put(StorageAccessProperty.AWS_SECRET_KEY, credential.secretAccessKey());
    builder.put(StorageAccessProperty.AWS_TOKEN, credential.sessionToken());
    builder.put(
        StorageAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS,
        String.valueOf(credential.expiresAt().toEpochMilli()));
    builder.put(StorageAccessProperty.AWS_ENDPOINT, String.valueOf(config.getEndpointUri()));
    builder.put(StorageAccessProperty.AWS_PATH_STYLE_ACCESS, "true");
    builder.put(StorageAccessProperty.CLIENT_REGION, "auto");
    return builder.build();
  }

  /**
   * The single bucket all grants refer to; an R2 temporary credential binds to exactly one. Names
   * the table in the failure message whenever the context carries one, because the fix is to give
   * that table a location layout confined to one bucket.
   */
  static String singleBucket(List<LocationGrant> grants, CredentialVendingContext context) {
    Set<String> buckets = new TreeSet<>();
    for (LocationGrant grant : grants) {
      for (String location : grant.locations()) {
        String bucket = StorageUri.parse(location).authority();
        if (bucket == null) {
          throw new IllegalArgumentException(
              "R2 location '"
                  + location
                  + "'"
                  + forTable(context)
                  + " names no bucket; R2 locations must be s3://<bucket>/<prefix>");
        }
        buckets.add(bucket);
      }
    }
    if (buckets.size() != 1) {
      throw new IllegalArgumentException(
          "R2 credentials are scoped to one bucket, but the grants"
              + forTable(context)
              + " span "
              + buckets
              + "; use one bucket per table");
    }
    return buckets.iterator().next();
  }

  /**
   * {@code " for table <catalog>.<namespace>.<table>"} when the context identifies a table, else
   * the empty string.
   */
  private static String forTable(CredentialVendingContext context) {
    return context
        .tableName()
        .map(
            table ->
                " for table "
                    + Stream.of(context.catalogName(), context.namespace(), Optional.of(table))
                        .flatMap(Optional::stream)
                        .collect(Collectors.joining(".")))
        .orElse("");
  }

  /**
   * Key prefixes for the {@code paths.prefixPaths} claim, deduplicated and sorted. A prefix is the
   * grant location's raw path minus exactly one leading slash, with one trailing slash added only
   * when the path has none.
   *
   * <p>The module refuses a location with an empty path segment. This method throws an {@link
   * IllegalArgumentException} for a grant location that holds {@code //} anywhere after the bucket,
   * including right after it. It checks every location before it returns, so the result does not
   * depend on the order of the locations. {@link R2LocalSigningCredentialVendingMechanism#validate}
   * applies the same rule to the allowed locations at create and update.
   *
   * <p>An empty result scopes the credential to the whole bucket, so it is returned only when a
   * grant is literally at bucket root ({@code s3://bucket} or {@code s3://bucket/}). That path is
   * empty after one leading slash comes off.
   *
   * <p>Combining several grants unions their locations: every location under the single bucket
   * contributes a prefix, whatever actions its own grant carries.
   */
  static List<String> normalizedPrefixes(List<LocationGrant> grants) {
    TreeSet<String> prefixes = new TreeSet<>();
    boolean wholeBucket = false;
    for (LocationGrant grant : grants) {
      for (String location : grant.locations()) {
        R2LocalSigningCredentialVendingMechanism.requireNoEmptyPathSegment("location", location);
        String path = StorageUri.parse(location).rawPath();
        if (path.startsWith("/")) {
          path = path.substring(1);
        }
        if (path.isEmpty()) {
          wholeBucket = true;
        } else {
          prefixes.add(path.endsWith("/") ? path : path + "/");
        }
      }
    }
    return wholeBucket ? List.of() : List.copyOf(prefixes);
  }

  /**
   * Rejects a grant list that mixes read-only grants with write-capable ones. R2 attaches one scope
   * to the whole credential, so such a list can only be served by widening the read-only prefixes
   * to read-write, which hands out more than Polaris authorized. One grant per vend is the only
   * shape the callers produce today, but the SPI accepts a list, so fail closed rather than widen.
   */
  private static void requireUniformScope(
      List<LocationGrant> grants, CredentialVendingContext context) {
    if (grants.size() < 2) {
      return;
    }
    long writeCapable =
        grants.stream()
            .filter(grant -> grant.actions().stream().anyMatch(WRITE_ACTIONS::contains))
            .count();
    if (writeCapable != 0 && writeCapable != grants.size()) {
      throw new IllegalArgumentException(
          "R2 credentials carry one scope for all prefixes; grants with mixed read-only and write"
              + " actions cannot be vended together"
              + forTable(context));
    }
  }

  /**
   * The single R2 scope covering all grants. R2 attaches one scope to the whole credential, so a
   * WRITE, DELETE or ALL action yields {@code object-read-write} for every prefix; only an all-read
   * set yields {@code object-read-only}.
   *
   * <p>{@link #requireUniformScope} runs first, so this widening applies only within one grant, or
   * across grants that are uniformly write-capable. A list that mixes a read-only grant with a
   * write-capable one never reaches here.
   */
  static String scopeFor(List<LocationGrant> grants) {
    boolean write =
        grants.stream().flatMap(g -> g.actions().stream()).anyMatch(WRITE_ACTIONS::contains);
    return write
        ? CloudflareR2TemporaryCredentialSigner.SCOPE_OBJECT_READ_WRITE
        : CloudflareR2TemporaryCredentialSigner.SCOPE_OBJECT_READ_ONLY;
  }
}
