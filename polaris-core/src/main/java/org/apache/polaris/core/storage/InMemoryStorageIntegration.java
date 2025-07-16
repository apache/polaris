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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.polaris.core.context.CallContext;

/**
 * Base class for in-memory implementations of {@link PolarisStorageIntegration}. A basic
 * implementation of {@link #validateAccessToLocations(PolarisStorageConfigurationInfo, Set, Set)}
 * is provided that checks to see that the list of locations being accessed is among the list of
 * {@link PolarisStorageConfigurationInfo#getAllowedLocations()}. Locations being accessed must be
 * equal to or a subdirectory of at least one of the allowed locations.
 *
 * @param <T>
 */
public abstract class InMemoryStorageIntegration<T extends PolarisStorageConfigurationInfo>
    extends PolarisStorageIntegration<T> {

  public InMemoryStorageIntegration(String identifierOrId) {
    super(identifierOrId);
  }

  /**
   * Check that the locations being accessed are all equal to or subdirectories of at least one of
   * the {@link PolarisStorageConfigurationInfo#getAllowedLocations}.
   *
   * @param actions a set of operation actions to validate, like LIST/READ/DELETE/WRITE/ALL
   * @param locations a set of locations to get access to
   * @return a map of location to a validation result for each action passed in. In this
   *     implementation, all actions have the same validation result, as we only verify the
   *     locations are equal to or subdirectories of the allowed locations.
   */
  public static Map<String, Map<PolarisStorageActions, ValidationResult>>
      validateSubpathsOfAllowedLocations(
          @Nonnull PolarisStorageConfigurationInfo storageConfig,
          @Nonnull Set<PolarisStorageActions> actions,
          @Nonnull Set<String> locations) {
    // trim trailing / from allowed locations so that locations missing the trailing slash still
    // match
    Set<String> allowedLocationStrings =
        storageConfig.getAllowedLocations().stream()
            .map(
                str -> {
                  if (str.endsWith("/") && str.length() > 1) {
                    return str.substring(0, str.length() - 1);
                  } else {
                    return str;
                  }
                })
            .map(str -> str.replace("file:///", "file:/"))
            .collect(Collectors.toSet());
    List<StorageLocation> allowedLocations =
        allowedLocationStrings.stream().map(StorageLocation::of).collect(Collectors.toList());

    boolean allowWildcardLocation =
        Optional.ofNullable(CallContext.getCurrentContext())
            .map(ctx -> ctx.getRealmConfig().getConfig("ALLOW_WILDCARD_LOCATION", false))
            .orElse(false);

    if (allowWildcardLocation && allowedLocationStrings.contains("*")) {
      return locations.stream()
          .collect(
              Collectors.toMap(
                  Function.identity(),
                  loc ->
                      actions.stream()
                          .collect(
                              Collectors.toMap(
                                  Function.identity(),
                                  a ->
                                      new ValidationResult(
                                          true, loc + " in the list of allowed locations")))));
    }
    Map<String, Map<PolarisStorageActions, ValidationResult>> resultMap = new HashMap<>();
    for (String rawLocation : locations) {
      StorageLocation storageLocation = StorageLocation.of(rawLocation);
      final boolean isValidLocation =
          allowedLocations.stream().anyMatch(storageLocation::isChildOf);
      Map<PolarisStorageActions, ValidationResult> locationResult =
          actions.stream()
              .collect(
                  Collectors.toMap(
                      Function.identity(),
                      a ->
                          new ValidationResult(
                              isValidLocation,
                              rawLocation
                                  + " is "
                                  + (isValidLocation ? "" : "not ")
                                  + "in the list of allowed locations: "
                                  + allowedLocations)));

      resultMap.put(rawLocation, locationResult);
    }
    return resultMap;
  }

  @Override
  @Nonnull
  public Map<String, Map<PolarisStorageActions, ValidationResult>> validateAccessToLocations(
      @Nonnull T storageConfig,
      @Nonnull Set<PolarisStorageActions> actions,
      @Nonnull Set<String> locations) {
    return validateSubpathsOfAllowedLocations(storageConfig, actions, locations);
  }
}
