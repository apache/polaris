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
package org.apache.polaris.service.catalog;

import static org.apache.polaris.service.catalog.AccessDelegationMode.REMOTE_SIGNING;
import static org.apache.polaris.service.catalog.AccessDelegationMode.UNKNOWN;
import static org.apache.polaris.service.catalog.AccessDelegationMode.VENDED_CREDENTIALS;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.util.EnumSet;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link AccessDelegationModeResolver} that resolves the optimal access
 * delegation mode based on catalog capabilities and configuration.
 *
 * <p>The resolution logic:
 *
 * <ol>
 *   <li>If no delegation mode is requested, returns {@link AccessDelegationMode#UNKNOWN}
 *   <li>If exactly one delegation mode is requested (excluding UNKNOWN), returns that mode
 *   <li>If both {@link AccessDelegationMode#VENDED_CREDENTIALS} and {@link
 *       AccessDelegationMode#REMOTE_SIGNING} are requested:
 *       <ul>
 *         <li>If STS is unavailable for the catalog's AWS storage, returns {@link
 *             AccessDelegationMode#REMOTE_SIGNING}
 *         <li>If credential subscoping is skipped, returns {@link
 *             AccessDelegationMode#REMOTE_SIGNING}
 *         <li>Otherwise, returns {@link AccessDelegationMode#VENDED_CREDENTIALS}
 *       </ul>
 * </ol>
 */
@RequestScoped
public class DefaultAccessDelegationModeResolver implements AccessDelegationModeResolver {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DefaultAccessDelegationModeResolver.class);

  private final RealmConfig realmConfig;

  @Inject
  public DefaultAccessDelegationModeResolver(CallContext callContext) {
    this.realmConfig = callContext.getRealmConfig();
  }

  @Override
  @Nonnull
  public AccessDelegationMode resolve(
      @Nonnull EnumSet<AccessDelegationMode> requestedModes,
      @Nullable CatalogEntity catalogEntity) {

    // Case 1: No delegation mode requested
    if (requestedModes.isEmpty()) {
      LOGGER.debug("No delegation mode requested, returning UNKNOWN");
      return UNKNOWN;
    }

    // Filter out UNKNOWN mode from consideration for selection logic
    EnumSet<AccessDelegationMode> effectiveModes = EnumSet.copyOf(requestedModes);
    effectiveModes.remove(UNKNOWN);

    if (effectiveModes.isEmpty()) {
      LOGGER.debug("Only UNKNOWN mode requested, returning UNKNOWN");
      return UNKNOWN;
    }

    // Case 2: Exactly one delegation mode requested
    if (effectiveModes.size() == 1) {
      AccessDelegationMode mode = effectiveModes.iterator().next();
      LOGGER.debug("Single delegation mode requested: {}", mode);
      return mode;
    }

    // Case 3: Both VENDED_CREDENTIALS and REMOTE_SIGNING requested
    if (effectiveModes.contains(VENDED_CREDENTIALS) && effectiveModes.contains(REMOTE_SIGNING)) {
      return resolveVendedCredentialsVsRemoteSigning(catalogEntity);
    }

    // Case 4: Unknown combination - reject to prevent unintended credential exposure
    throw new IllegalArgumentException(
        "Unsupported access delegation mode combination: " + requestedModes);
  }

  /**
   * Resolves between VENDED_CREDENTIALS and REMOTE_SIGNING based on catalog capabilities.
   *
   * <p>The logic prefers VENDED_CREDENTIALS when:
   *
   * <ul>
   *   <li>STS is available for the catalog's storage (for AWS)
   *   <li>Credential subscoping is not skipped
   * </ul>
   *
   * <p>Otherwise, REMOTE_SIGNING is preferred as it doesn't require STS.
   */
  private AccessDelegationMode resolveVendedCredentialsVsRemoteSigning(
      @Nullable CatalogEntity catalogEntity) {

    // If no catalog entity available, default to VENDED_CREDENTIALS for backward compatibility
    if (catalogEntity == null) {
      LOGGER.debug(
          "No catalog entity available for mode resolution, defaulting to VENDED_CREDENTIALS");
      return VENDED_CREDENTIALS;
    }

    // Check if credential vending is enabled for this catalog.
    // For internal catalogs, credential vending is always enabled.
    // For external/federated catalogs, check if ALLOW_FEDERATED_CATALOGS_CREDENTIAL_VENDING is
    // enabled.
    boolean credentialVendingEnabled =
        !catalogEntity.isExternal()
            || realmConfig.getConfig(
                FeatureConfiguration.ALLOW_FEDERATED_CATALOGS_CREDENTIAL_VENDING, catalogEntity);

    if (!credentialVendingEnabled) {
      LOGGER.debug(
          "Credential vending is not enabled for external catalog {}, selecting REMOTE_SIGNING",
          catalogEntity.getName());
      return REMOTE_SIGNING;
    }

    // Check if credential subscoping is skipped - if so, VENDED_CREDENTIALS won't work properly
    // Note: This config is realm-level only (not overridable at catalog level) and has a default
    // value
    boolean skipCredentialSubscoping =
        realmConfig.getConfig(FeatureConfiguration.SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION);

    if (skipCredentialSubscoping) {
      LOGGER.debug("Credential subscoping is skipped for this realm, selecting REMOTE_SIGNING");
      return REMOTE_SIGNING;
    }

    // Check credential vending availability from storage configuration
    boolean credentialVendingAvailable = isCredentialVendingAvailable(catalogEntity);

    if (credentialVendingAvailable) {
      LOGGER.debug(
          "Credential vending is available for catalog {}, selecting VENDED_CREDENTIALS",
          catalogEntity.getName());
      return VENDED_CREDENTIALS;
    } else {
      LOGGER.debug(
          "Credential vending is not available for catalog {}, selecting REMOTE_SIGNING",
          catalogEntity.getName());
      return REMOTE_SIGNING;
    }
  }

  /**
   * Determines if STS is available for the catalog's storage configuration.
   *
   * <p>For AWS storage, this checks the {@link AwsStorageConfigurationInfo#getStsUnavailable()}
   * flag. For other storage types, STS is considered available by default (since they may have
   * their own credential vending mechanisms).
   *
   * @param catalogEntity The catalog entity to check
   * @return true if STS is available or the storage type doesn't require STS
   */
  private boolean isCredentialVendingAvailable(@Nonnull CatalogEntity catalogEntity) {
    PolarisStorageConfigurationInfo storageConfig = catalogEntity.getStorageConfigurationInfo();

    if (storageConfig == null) {
      LOGGER.debug(
          "No storage configuration found for catalog {}, assuming STS is available",
          catalogEntity.getName());
      return true;
    }

    if (storageConfig instanceof AwsStorageConfigurationInfo awsConfig) {
      // STS is available unless explicitly marked as unavailable
      boolean stsUnavailable = Boolean.TRUE.equals(awsConfig.getStsUnavailable());
      LOGGER.debug(
          "AWS storage configuration for catalog {}: stsUnavailable={}",
          catalogEntity.getName(),
          stsUnavailable);
      return !stsUnavailable;
    }

    // For non-AWS storage types, assume STS-like functionality is available
    // (e.g., Azure uses different mechanisms, GCP uses service accounts)
    LOGGER.debug(
        "Non-AWS storage type {} for catalog {}, assuming credential vending is available",
        storageConfig.getStorageType(),
        catalogEntity.getName());
    return true;
  }
}
