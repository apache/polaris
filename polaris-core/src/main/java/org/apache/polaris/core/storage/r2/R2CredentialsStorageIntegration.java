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
package org.apache.polaris.core.storage.r2;

import static org.apache.polaris.core.config.FeatureConfiguration.STORAGE_CREDENTIAL_DURATION_SECONDS;

import java.time.Clock;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.CachingStorageIntegration;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.LocationGrant;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.StorageUri;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.core.storage.cache.StorageCredentialCacheKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Vends Cloudflare R2 temporary credentials scoped to one bucket and the table's key prefixes.
 * Credentials are minted locally from the server-side parent token (see {@link
 * R2TemporaryCredentialSigner}); no call to Cloudflare is made.
 */
public class R2CredentialsStorageIntegration
    extends CachingStorageIntegration<R2StorageConfigurationInfo> {

  private static final Set<PolarisStorageActions> WRITE_ACTIONS =
      EnumSet.of(
          PolarisStorageActions.WRITE, PolarisStorageActions.DELETE, PolarisStorageActions.ALL);

  private final R2ParentTokenResolver parentTokenResolver;
  private final Clock clock;

  public R2CredentialsStorageIntegration(
      @NonNull R2ParentTokenResolver parentTokenResolver,
      @NonNull Clock clock,
      @Nullable StorageCredentialCache cache,
      @NonNull R2StorageConfigurationInfo storageConfig,
      @NonNull RealmConfig realmConfig) {
    super(cache, realmConfig, storageConfig);
    this.parentTokenResolver = parentTokenResolver;
    this.clock = clock;
  }

  @Override
  protected StorageCredentialCacheKey buildCacheKey(
      @NonNull List<LocationGrant> grants,
      @NonNull Optional<String> refreshEndpoint,
      @NonNull CredentialVendingContext context) {
    R2StorageConfigurationInfo config = storageConfig();
    R2ParentToken token =
        parentTokenResolver
            .resolve(config.getStorageName())
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        config.getStorageName() == null
                            ? "No default R2 parent token is configured on the server"
                            : "R2 storage name '"
                                + config.getStorageName()
                                + "' is not configured on the server"));
    Duration ttl = Duration.ofSeconds(realmConfig().getConfig(STORAGE_CREDENTIAL_DURATION_SECONDS));
    return R2StorageCredentialCacheKey.of(
        context.realm().orElse(""),
        config,
        singleBucket(grants),
        normalizedPrefixes(grants),
        scopeFor(grants),
        token.accessKeyId(),
        R2ParentToken.fingerprint(token.secretAccessKey()),
        ttl,
        refreshEndpoint,
        token.secretAccessKey(),
        clock,
        realmConfig());
  }

  /** Mint a fresh {@link StorageAccessConfig} for the given key. Called by the cache on miss. */
  static StorageAccessConfig compute(R2StorageCredentialCacheKey key) {
    R2StorageConfigurationInfo config = key.storageConfig();
    R2TemporaryCredentialSigner.Credential credential =
        R2TemporaryCredentialSigner.sign(
            new R2TemporaryCredentialSigner.Request(
                key.parentKeyId(),
                key.parentSecret(),
                config.getAccountId(),
                config.getEndpointHost(),
                key.bucket(),
                key.prefixes(),
                key.scope(),
                key.ttl(),
                key.clock().instant()));
    StorageAccessConfig.Builder builder = StorageAccessConfig.builder();
    builder.put(StorageAccessProperty.R2_KEY_ID, credential.accessKeyId());
    builder.put(StorageAccessProperty.R2_SECRET_KEY, credential.secretAccessKey());
    builder.put(StorageAccessProperty.R2_TOKEN, credential.sessionToken());
    builder.put(
        StorageAccessProperty.R2_SESSION_TOKEN_EXPIRES_AT_MS,
        String.valueOf(credential.expiresAt().toEpochMilli()));
    builder.put(StorageAccessProperty.R2_ENDPOINT, config.getEndpointUri().toString());
    builder.put(StorageAccessProperty.R2_PATH_STYLE_ACCESS, Boolean.TRUE.toString());
    builder.put(StorageAccessProperty.R2_CLIENT_REGION, "auto");
    key.refreshCredentialsEndpoint()
        .ifPresent(e -> builder.put(StorageAccessProperty.R2_REFRESH_CREDENTIALS_ENDPOINT, e));
    return builder.build();
  }

  /** The single bucket all grants refer to; an R2 temporary credential binds to exactly one. */
  static String singleBucket(List<LocationGrant> grants) {
    Set<String> buckets =
        grants.stream()
            .flatMap(g -> g.locations().stream())
            .map(loc -> StorageUri.parse(loc).authority())
            .collect(Collectors.toCollection(TreeSet::new));
    if (buckets.size() != 1) {
      throw new IllegalArgumentException(
          "R2 credentials are scoped to one bucket, but the grants span " + buckets);
    }
    return buckets.iterator().next();
  }

  /**
   * Key prefixes for the {@code paths.prefixPaths} claim: no leading slash, exactly one trailing
   * slash, deduplicated, sorted. An empty result means at least one grant is at bucket root, and
   * the credential is scoped to the whole bucket.
   */
  static List<String> normalizedPrefixes(List<LocationGrant> grants) {
    TreeSet<String> prefixes = new TreeSet<>();
    for (LocationGrant grant : grants) {
      for (String location : grant.locations()) {
        String path = StorageUri.parse(location).rawPath();
        while (path.startsWith("/")) {
          path = path.substring(1);
        }
        while (path.endsWith("/")) {
          path = path.substring(0, path.length() - 1);
        }
        if (path.isEmpty()) {
          return List.of();
        }
        prefixes.add(path + "/");
      }
    }
    return List.copyOf(prefixes);
  }

  static String scopeFor(List<LocationGrant> grants) {
    boolean write =
        grants.stream().flatMap(g -> g.actions().stream()).anyMatch(WRITE_ACTIONS::contains);
    return write
        ? R2TemporaryCredentialSigner.SCOPE_OBJECT_READ_WRITE
        : R2TemporaryCredentialSigner.SCOPE_OBJECT_READ_ONLY;
  }
}
