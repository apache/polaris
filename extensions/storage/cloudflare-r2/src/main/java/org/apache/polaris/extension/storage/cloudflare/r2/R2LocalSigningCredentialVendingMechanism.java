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

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.List;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.S3CredentialVendingMechanism;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.jspecify.annotations.Nullable;

/**
 * Cloudflare R2 temporary credentials signed by the server with a parent token. A catalog selects
 * this mechanism with {@code credentialVendingMechanism = "R2_LOCAL_SIGNING"}. The bean needs no
 * parent token at construction, because the registry creates every mechanism bean at startup in
 * every deployment. A missing token is reported by {@link #validate} and at vend time.
 *
 * <p>No logic here reads the identifier that a stored config carries, so {@link #validate} and
 * {@link #integrationFor} behave the same for a config of any identifier.
 */
@ApplicationScoped
@Identifier(R2LocalSigningCredentialVendingMechanism.R2_LOCAL_SIGNING)
public class R2LocalSigningCredentialVendingMechanism implements S3CredentialVendingMechanism {

  /** The identifier of this mechanism, as a catalog stores it. */
  public static final String R2_LOCAL_SIGNING = "R2_LOCAL_SIGNING";

  private static final String FOR_THIS_MECHANISM =
      " for the " + R2_LOCAL_SIGNING + " credential vending mechanism";

  /** The request fields that core stores as {@code encryptionKeys}. */
  private static final String ENCRYPTION_KEY_FIELDS =
      "encryptionKeys, currentKmsKey, and allowedKmsKeys";

  private final CloudflareR2ParentTokenResolver parentTokenResolver;
  private final Clock clock;
  private final StorageCredentialCache cache;
  private final RealmConfig realmConfig;

  /**
   * For the client proxy ArC generates for this normal-scoped bean: the proxy subclass needs a
   * no-args constructor. A working instance always comes from the constructor below.
   */
  protected R2LocalSigningCredentialVendingMechanism() {
    this.parentTokenResolver = null;
    this.clock = null;
    this.cache = null;
    this.realmConfig = null;
  }

  /**
   * The injected constructor. It takes only plain values, so a test calls it directly. The {@code
   * cache} may be null, and the integration then mints a credential on every call.
   */
  @Inject
  public R2LocalSigningCredentialVendingMechanism(
      CloudflareR2ParentTokenResolver parentTokenResolver,
      Clock clock,
      StorageCredentialCache cache,
      RealmConfig realmConfig) {
    this.parentTokenResolver = parentTokenResolver;
    this.clock = clock;
    this.cache = cache;
    this.realmConfig = realmConfig;
  }

  /** Builds a new integration on each call, so this must stay cheap. The bean holds no state. */
  @Override
  public PolarisStorageIntegration integrationFor(AwsStorageConfigurationInfo storageConfig) {
    return new CloudflareR2CredentialsStorageIntegration(
        parentTokenResolver, clock, cache, storageConfig, realmConfig);
  }

  /**
   * Refuses a config that this mechanism cannot vend for. Every refusal is an {@link
   * IllegalArgumentException}, which the server returns as HTTP 400. The rules run in this order,
   * and the first refusal wins:
   *
   * <ol>
   *   <li>The endpoint is an R2 endpoint ({@link CloudflareR2Endpoint#parse}).
   *   <li>{@code pathStyleAccess} is true and {@code region} is {@code auto}.
   *   <li>The STS and KMS fields are absent.
   *   <li>No allowed location holds an empty path segment.
   *   <li>A parent token resolves for the config.
   * </ol>
   *
   * The rules check {@code updated} only. An update may change the endpoint, as it may for STS.
   * This method has no side effects.
   */
  @Override
  public void validate(
      @Nullable AwsStorageConfigurationInfo current, AwsStorageConfigurationInfo updated) {
    CloudflareR2Endpoint.parse(updated.getEndpoint());
    requirePathStyleAndAutoRegion(updated);
    requireStsAndKmsFieldsAbsent(updated);
    requireNoEmptyPathSegments(updated);
    requireParentToken(updated);
  }

  /**
   * The extension requires {@code pathStyleAccess=true} and {@code region=auto}. {@link #validate}
   * applies the rule at create and update, and the integration applies it again at vend time.
   */
  static void requirePathStyleAndAutoRegion(AwsStorageConfigurationInfo config) {
    if (!Boolean.TRUE.equals(config.getPathStyleAccess())) {
      throw new IllegalArgumentException("pathStyleAccess must be true" + FOR_THIS_MECHANISM);
    }
    if (!"auto".equals(config.getRegion())) {
      throw new IllegalArgumentException("region must be 'auto'" + FOR_THIS_MECHANISM);
    }
  }

  /**
   * An R2 catalog has no role, no STS, and no KMS. A true {@code stsUnavailable} would also make
   * the access delegation resolver fall back to remote signing. The Python CLI sends false for both
   * flags by default, so false is accepted.
   *
   * <p>The management API cannot set {@code userArn}, {@code currentKmsKey}, or {@code
   * allowedKmsKeys} on the stored config: core does not copy {@code userArn} from the request, and
   * it folds {@code currentKmsKey} and {@code allowedKmsKeys} into {@code encryptionKeys}. So the
   * {@code encryptionKeys} refusal names all three request fields. The checks of the three stored
   * fields stay as defense in depth.
   */
  @SuppressWarnings("deprecation")
  private static void requireStsAndKmsFieldsAbsent(AwsStorageConfigurationInfo updated) {
    requireAbsent("roleArn", updated.getRoleARN());
    requireAbsent("externalId", updated.getExternalId());
    requireAbsent("userArn", updated.getUserARN());
    requireAbsent("stsEndpoint", updated.getStsEndpoint());
    requireNotTrue("stsUnavailable", updated.getStsUnavailable());
    requireAbsent("endpointInternal", updated.getEndpointInternal());
    requireNotTrue("kmsUnavailable", updated.getKmsUnavailable());
    requireAbsent("currentKmsKey", updated.getCurrentKmsKey());
    requireAbsentList("allowedKmsKeys", updated.getAllowedKmsKeys());
    requireAbsentList(ENCRYPTION_KEY_FIELDS, updated.getEncryptionKeys());
    requireAbsentList("decryptionKeys", updated.getDecryptionKeys());
  }

  /**
   * An allowed location must not contain an empty path segment. The module refuses such a location
   * at create and update (allowed locations) and at vend time (grant locations, in the storage
   * integration). The rule sees only the allowed locations of the config.
   */
  private static void requireNoEmptyPathSegments(AwsStorageConfigurationInfo updated) {
    for (String location : updated.getAllowedLocations()) {
      requireNoEmptyPathSegment("allowed location", location);
    }
  }

  /**
   * Refuses a location with an empty path segment, which is {@code //} anywhere after the bucket,
   * including right after it. {@code description} names the kind of location in the message: {@code
   * "allowed location"} at create and update, and {@code "location"} at vend time. Both sites call
   * this method, so the two messages use one wording.
   */
  static void requireNoEmptyPathSegment(String description, String location) {
    String path = location.substring(location.indexOf("://") + 3);
    if (path.contains("//")) {
      throw new IllegalArgumentException(
          description
              + " '"
              + location
              + "' contains an empty path segment ('//'), which the "
              + R2_LOCAL_SIGNING
              + " credential vending mechanism does not accept");
    }
  }

  /**
   * The same lookup that the integration uses at vend time. The message names the property to set
   * and never a value.
   */
  private void requireParentToken(AwsStorageConfigurationInfo updated) {
    String storageName = CloudflareR2ParentTokenResolver.effectiveStorageName(realmConfig, updated);
    if (parentTokenResolver.resolve(storageName).isEmpty()) {
      throw new IllegalArgumentException(
          CloudflareR2ParentTokenResolver.missingTokenMessage(storageName));
    }
  }

  private static void requireAbsent(String field, @Nullable Object value) {
    if (value != null) {
      throw new IllegalArgumentException(field + " must not be set" + FOR_THIS_MECHANISM);
    }
  }

  private static void requireAbsentList(String field, @Nullable List<String> value) {
    if (value != null && !value.isEmpty()) {
      throw new IllegalArgumentException(field + " must not be set" + FOR_THIS_MECHANISM);
    }
  }

  private static void requireNotTrue(String field, @Nullable Boolean value) {
    if (Boolean.TRUE.equals(value)) {
      throw new IllegalArgumentException(field + " must not be true" + FOR_THIS_MECHANISM);
    }
  }
}
