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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

  /**
   * Checks whether a planned change to an entity's property would modify any reserved properties
   *
   * @param originalKeys The keys currently present for an entity
   * @param newKeys The keys present in an update to an entity
   * @return True if the change would modify a reserved property
   */
  default boolean updateContainsReservedProperty(List<String> originalKeys, List<String> newKeys) {
    Set<String> originalKeySet = new HashSet<>(newKeys);
    Set<String> newKeySet = new HashSet<>(newKeys);

    ArrayList<String> modifiedKeys = new ArrayList<>();
    for (String originalKey : originalKeys) {
      if (!newKeySet.contains(originalKey)) {
        modifiedKeys.add(originalKey);
      }
    }
    for (String newKey : newKeys) {
      if (!originalKeySet.contains(newKey)) {
        modifiedKeys.add(newKey);
      }
    }

    return containsReservedProperty(modifiedKeys);
  }

  /**
   * Checks whether any of the specified keys is a reserved property
   *
   * @param keys A list of property keys to validate
   * @return True if the list of property keys has a reserved property
   */
  default boolean containsReservedProperty(List<String> keys) {
    List<String> prefixes = reservedPrefixes();
    for (String key : keys) {
      for (String prefix : prefixes) {
        if (key.startsWith(prefix)) {
          LOGGER.debug("Property '{}' matches reserved prefix '{}'", key, prefix);
          return true;
        }
      }
    }
    return false;
  }
}
