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
import static org.apache.polaris.service.catalog.AccessDelegationMode.VENDED_CREDENTIALS;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.util.EnumSet;
import java.util.Optional;
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
 *   <li>If no delegation mode is requested, returns empty.
 *   <li>If exactly one delegation mode is requested:
 *       <ul>
 *         <li>If {@link AccessDelegationMode#VENDED_CREDENTIALS} is requested on an external
 *             catalog where credential vending is disabled (via {@link
 *             FeatureConfiguration#ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING} or {@link
 *             FeatureConfiguration#ALLOW_FEDERATED_CATALOGS_CREDENTIAL_VENDING}), throws {@link
 *             IllegalArgumentException}.
 *         <li>Otherwise returns that mode as-is.
 *       </ul>
 *   <li>If both {@link AccessDelegationMode#VENDED_CREDENTIALS} and {@link
 *       AccessDelegationMode#REMOTE_SIGNING} are requested, picks the best viable mode:
 *       <ul>
 *         <li>If the catalog is external and {@link
 *             FeatureConfiguration#ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING} is disabled, returns
 *             {@link AccessDelegationMode#REMOTE_SIGNING} (graceful degradation).
 *         <li>If the catalog is external/federated and {@link
 *             FeatureConfiguration#ALLOW_FEDERATED_CATALOGS_CREDENTIAL_VENDING} is disabled,
 *             returns {@link AccessDelegationMode#REMOTE_SIGNING}.
 *         <li>If credential subscoping is skipped, returns {@link
 *             AccessDelegationMode#REMOTE_SIGNING}.
 *         <li>If STS is unavailable for the catalog's AWS storage, returns {@link
 *             AccessDelegationMode#REMOTE_SIGNING}.
 *         <li>Otherwise returns {@link AccessDelegationMode#VENDED_CREDENTIALS}.
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
  public @Nonnull Optional<AccessDelegationMode> resolve(
      @Nonnull EnumSet<AccessDelegationMode> requestedModes,
      @Nullable CatalogEntity catalogEntity) {

    // Case 1: No valid delegation mode found, or none requested
    if (requestedModes.isEmpty()) {
      return Optional.empty();
    }

    // Case 2: Exactly one delegation mode requested
    if (requestedModes.size() == 1) {
      AccessDelegationMode mode = requestedModes.iterator().next();
      // REMOTE_SIGNING does not involve Polaris vending credentials to the client, so the
      // credential-vending feature flags below do not apply to it.
      if (mode == VENDED_CREDENTIALS) {
        if (isExternalCatalogCredentialVendingDisabled(catalogEntity)) {
          throw new IllegalArgumentException(
              "Credential vending is not enabled for this external catalog. Please consult "
                  + "applicable documentation for the catalog config property '"
                  + FeatureConfiguration.ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING.catalogConfig()
                  + "' to enable this feature");
        }
        if (catalogEntity != null
            && catalogEntity.isExternal()
            && !realmConfig.getConfig(
                FeatureConfiguration.ALLOW_FEDERATED_CATALOGS_CREDENTIAL_VENDING, catalogEntity)) {
          throw new IllegalArgumentException(
              "Credential vending is not enabled for this federated catalog. Please consult "
                  + "applicable documentation for the catalog config property '"
                  + FeatureConfiguration.ALLOW_FEDERATED_CATALOGS_CREDENTIAL_VENDING.catalogConfig()
                  + "' to enable this feature");
        }
      }
      LOGGER.debug("Single delegation mode requested: {}", mode);
      return Optional.of(mode);
    }

    // Case 3: Both VENDED_CREDENTIALS and REMOTE_SIGNING requested

    // If no catalog entity available, default to VENDED_CREDENTIALS for backward compatibility
    if (catalogEntity == null) {
      LOGGER.debug(
          "No catalog entity available for mode resolution, defaulting to VENDED_CREDENTIALS");
      return Optional.of(VENDED_CREDENTIALS);
    }

    // Check ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING first — this is the broader gate that
    // controls whether external catalogs may vend credentials at all. When both modes are
    // requested and this gate is closed, gracefully fall back to REMOTE_SIGNING instead of
    // throwing, because REMOTE_SIGNING is a viable alternative that does not require credential
    // vending.
    if (isExternalCatalogCredentialVendingDisabled(catalogEntity)) {
      LOGGER.debug(
          "Credential vending not allowed for external catalog {}, selecting REMOTE_SIGNING",
          catalogEntity.getName());
      return Optional.of(REMOTE_SIGNING);
    }

    // Check if credential vending is enabled for this catalog.
    // For internal catalogs, credential vending is always enabled.
    // For external/federated catalogs, check if ALLOW_FEDERATED_CATALOGS_CREDENTIAL_VENDING is
    // enabled. Note: reaching here means ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING is true.
    boolean credentialVendingEnabled =
        !catalogEntity.isExternal()
            || realmConfig.getConfig(
                FeatureConfiguration.ALLOW_FEDERATED_CATALOGS_CREDENTIAL_VENDING, catalogEntity);

    if (!credentialVendingEnabled) {
      LOGGER.debug(
          "Credential vending is not enabled for external catalog {}, selecting REMOTE_SIGNING",
          catalogEntity.getName());
      return Optional.of(REMOTE_SIGNING);
    }

    // Check if credential subscoping is skipped - if so, VENDED_CREDENTIALS won't work properly
    // Note: This config is realm-level only (not overridable at catalog level) and has a default
    // value
    boolean skipCredentialSubscoping =
        realmConfig.getConfig(FeatureConfiguration.SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION);

    if (skipCredentialSubscoping) {
      LOGGER.debug("Credential subscoping is skipped for this realm, selecting REMOTE_SIGNING");
      return Optional.of(REMOTE_SIGNING);
    }

    // Check credential vending availability from storage configuration
    boolean credentialVendingAvailable = isCredentialVendingAvailable(catalogEntity);

    if (credentialVendingAvailable) {
      LOGGER.debug(
          "Credential vending is available for catalog {}, selecting VENDED_CREDENTIALS",
          catalogEntity.getName());
      return Optional.of(VENDED_CREDENTIALS);
    } else {
      LOGGER.debug(
          "Credential vending is not available for catalog {}, selecting REMOTE_SIGNING",
          catalogEntity.getName());
      return Optional.of(REMOTE_SIGNING);
    }
  }

  /**
   * Returns true if the catalog is an external catalog and credential vending is disabled for it
   * via {@link FeatureConfiguration#ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING}.
   *
   * <p>Internal catalogs always allow credential vending, so this returns false for them.
   */
  private boolean isExternalCatalogCredentialVendingDisabled(
      @Nullable CatalogEntity catalogEntity) {
    if (catalogEntity == null || !catalogEntity.isExternal()) {
      return false;
    }
    return !realmConfig.getConfig(
        FeatureConfiguration.ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING, catalogEntity);
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
