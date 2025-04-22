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
package org.apache.polaris.service.config;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.MetadataUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to track entity properties reserved for use by the catalog. These properties may not be
 * overridden by the end user.
 */
public interface ReservedProperties {
  Logger LOGGER = LoggerFactory.getLogger(ReservedProperties.class);

  /**
   * A list of prefixes that are considered reserved. Any property starting with one of these
   * prefixes is a reserved property.
   */
  List<String> reservedPrefixes();

  /** If true, attempts to modify a reserved property should throw an exception. */
  default boolean shouldThrow() {
    return true;
  }

  /**
   * Removes reserved properties from a planned change to an entity. If `shouldThrow`returns true,
   * this will throw an IllegalArgumentException.
   *
   * @param existingProperties The properties currently present for an entity
   * @param updateProperties The properties present in an update to an entity
   * @return The keys from the new key list which are not reserved properties
   */
  default Map<String, String> removeReservedPropertiesFromUpdate(
      Map<String, String> existingProperties, Map<String, String> updateProperties)
      throws IllegalArgumentException {
    Map<String, String> updatePropertiesWithoutReservedProperties =
        removeReservedProperties(updateProperties);
    for (var entry : updateProperties.entrySet()) {
      // If a key was removed from the update, we substitute in the existing value as to not remove
      // it
      if (!updatePropertiesWithoutReservedProperties.containsKey(entry.getKey())) {
        if (existingProperties.containsKey(entry.getKey())) {
          updatePropertiesWithoutReservedProperties.put(
              entry.getKey(), existingProperties.get(entry.getKey()));
        }
      }
    }
    return updatePropertiesWithoutReservedProperties;
  }

  /**
   * Removes reserved properties from a list of input property keys. If `shouldThrow`returns true,
   * this will throw an IllegalArgumentException.
   *
   * @param properties A map of properties to remove reserved properties from
   * @return The keys from the input list which are not reserved properties
   */
  default Map<String, String> removeReservedProperties(Map<String, String> properties)
      throws IllegalArgumentException {
    Map<String, String> results = new HashMap<>();
    List<String> prefixes = reservedPrefixes();
    for (var entry : properties.entrySet()) {
      for (String prefix : prefixes) {
        if (entry.getKey().startsWith(prefix)) {
          String message =
              String.format("Property '%s' matches reserved prefix '%s'", entry.getKey(), prefix);
          if (shouldThrow()) {
            throw new IllegalArgumentException(message);
          } else {
            LOGGER.debug(message);
          }
        } else {
          results.put(entry.getKey(), properties.get(entry.getValue()));
        }
      }
    }
    return results;
  }

  /** See {@link #removeReservedProperties(Map)} */
  default List<String> removeReservedProperties(List<String> properties)
      throws IllegalArgumentException {
    Map<String, String> propertyMap =
        properties.stream().collect(Collectors.toMap(k -> k, k -> ""));
    Map<String, String> filteredMap = removeReservedProperties(propertyMap);
    return filteredMap.keySet().stream().toList();
  }

  default MetadataUpdate removeReservedProperties(MetadataUpdate update) {
    return switch (update) {
      case MetadataUpdate.SetProperties p -> {
        yield new MetadataUpdate.SetProperties(removeReservedProperties(p.updated()));
      }
      case MetadataUpdate.RemoveProperties p -> {
        List<String> filteredProperties = removeReservedProperties(p.removed().stream().toList());
        yield new MetadataUpdate.RemoveProperties(new HashSet<>(filteredProperties));
      }
      default -> update;
    };
  }
}
