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
package org.apache.polaris.core.config;

import java.util.Optional;

/**
 * Internal configuration flags for non-feature behavior changes in Polaris. These flags control
 * subtle behavior adjustments and bug fixes, not user-facing catalog settings. They are intended
 * for internal use only, are inherently unstable, and may be removed at any time. When introducing
 * a new flag, consider the trade-off between maintenance burden and the risk of an unguarded
 * behavior change. Flags here are generally short-lived and should either be removed or promoted to
 * stable feature flags before the next release.
 *
 * @param <T> The type of the configuration
 */
public class BehaviorChangeConfiguration<T> extends PolarisConfiguration<T> {

  protected BehaviorChangeConfiguration(
      String key,
      String description,
      T defaultValue,
      Optional<String> catalogConfig,
      Optional<String> catalogConfigUnsafe) {
    super(key, description, defaultValue, catalogConfig, catalogConfigUnsafe);
  }

  public static final BehaviorChangeConfiguration<Boolean> VALIDATE_VIEW_LOCATION_OVERLAP =
      PolarisConfiguration.<Boolean>builder()
          .key("VALIDATE_VIEW_LOCATION_OVERLAP")
          .description("If true, validate that view locations don't overlap when views are created")
          .defaultValue(true)
          .buildBehaviorChangeConfiguration();

  public static final BehaviorChangeConfiguration<Integer> STORAGE_CONFIGURATION_MAX_LOCATIONS =
      PolarisConfiguration.<Integer>builder()
          .key("STORAGE_CONFIGURATION_MAX_LOCATIONS")
          .description(
              "How many locations can be associated with a storage configuration, or -1 for"
                  + " unlimited locations")
          .defaultValue(-1)
          .buildBehaviorChangeConfiguration();

  public static final BehaviorChangeConfiguration<Boolean> ENTITY_CACHE_SOFT_VALUES =
      PolarisConfiguration.<Boolean>builder()
          .key("ENTITY_CACHE_SOFT_VALUES")
          .description("Whether or not to use soft values in the entity cache")
          .defaultValue(false)
          .buildBehaviorChangeConfiguration();

  public static final BehaviorChangeConfiguration<Boolean>
      TABLE_OPERATIONS_MAKE_METADATA_CURRENT_ON_COMMIT =
          PolarisConfiguration.<Boolean>builder()
              .key("TABLE_OPERATIONS_MAKE_METADATA_CURRENT_ON_COMMIT")
              .description(
                  "If true, BasePolarisTableOperations should mark the metadata that is passed into"
                      + " `commit` as current, and re-use it to skip a trip to object storage to re-construct"
                      + " the committed metadata again.")
              .defaultValue(true)
              .buildBehaviorChangeConfiguration();
}
