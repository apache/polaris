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
package org.apache.polaris.core.storage;

import jakarta.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.config.RealmConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Defines storage location access restrictions for Polaris entities within a specific context. */
public class LocationRestrictions {
  private static final Logger LOGGER = LoggerFactory.getLogger(LocationRestrictions.class);

  /**
   * The complete set of storage locations that are permitted for access.
   *
   * <p>This list contains all storage URIs that entities can read from or write to, including both
   * catalog-level allowed locations and any additional user-specified locations when unstructured
   * table access is enabled.
   *
   * <p>All locations in this list have been validated to conform to the storage type's URI scheme
   * requirements during construction.
   */
  private final Collection<String> allowedLocations;

  /**
   * The parent location for structured table enforcement.
   *
   * <p>When non-null, this location represents the root under which all new tables must be created,
   * enforcing a structured hierarchy in addition to residing under {@code allowedLocations}. When
   * null, table creation is allowed anywhere within the {@code allowedLocations}.
   */
  private final String parentLocation;

  public LocationRestrictions(
      @Nonnull PolarisStorageConfigurationInfo storageConfigurationInfo, String parentLocation) {
    this(storageConfigurationInfo.getAllowedLocations(), parentLocation);
    allowedLocations.forEach(storageConfigurationInfo::validatePrefixForStorageType);
  }

  public LocationRestrictions(@Nonnull PolarisStorageConfigurationInfo storageConfigurationInfo) {
    this(storageConfigurationInfo, null);
  }

  public LocationRestrictions(Collection<String> allowedLocations, String parentLocation) {
    this.allowedLocations = allowedLocations;
    this.parentLocation = parentLocation;
  }

  public LocationRestrictions(Collection<String> allowedLocations) {
    this(allowedLocations, null);
  }

  /**
   * Validates that the requested storage locations are permitted for the given table identifier.
   *
   * <p>This method performs location validation by checking the requested locations against:
   *
   * <ul>
   *   <li>The parent location (if configured) for structured table enforcement
   *   <li>The allowed locations list for general access permissions
   * </ul>
   *
   * <p>The validation ensures that all requested locations conform to the storage access
   * restrictions defined for this context. If a parent location is configured, all requests must be
   * under that location hierarchy in addition to being within the allowed locations.
   *
   * @param realmConfig the realm configuration containing storage validation rules
   * @param identifier the table identifier for which locations are being validated
   * @param requestLocations the set of storage locations that need validation
   * @throws ForbiddenException if any of the requested locations violate the configured
   *     restrictions
   */
  public void validate(
      RealmConfig realmConfig, TableIdentifier identifier, Set<String> requestLocations) {
    if (parentLocation != null) {
      validateLocations(realmConfig, List.of(parentLocation), requestLocations, identifier);
    }

    validateLocations(realmConfig, allowedLocations, requestLocations, identifier);
  }

  private void validateLocations(
      RealmConfig realmConfig,
      Collection<String> allowedLocations,
      Set<String> requestLocations,
      TableIdentifier identifier) {
    var validationResults =
        InMemoryStorageIntegration.validateAllowedLocations(
            realmConfig, allowedLocations, Set.of(PolarisStorageActions.ALL), requestLocations);
    validationResults
        .values()
        .forEach(
            actionResult ->
                actionResult
                    .values()
                    .forEach(
                        result -> {
                          if (!result.isSuccess()) {
                            throw new ForbiddenException(
                                "Invalid locations '%s' for identifier '%s': %s",
                                requestLocations, identifier, result.getMessage());
                          } else {
                            LOGGER.debug(
                                "Validated locations '{}' for identifier '{}'",
                                requestLocations,
                                identifier);
                          }
                        }));
  }
}
