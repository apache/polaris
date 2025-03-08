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
 * Configurations for non-feature beheavior changes within Polaris. These configurations are not
 * intended for use by end users and govern nuanced behavior changes and bugfixes. The
 * configurations never expose user-facing catalog-level configurations. These configurations are
 * not stable and may be removed at any time.
 *
 * @param <T> The type of the configuration
 */
public class BehaviorChangeConfiguration<T> extends PolarisConfiguration<T> {

  protected BehaviorChangeConfiguration(
      String key, String description, T defaultValue, Optional<String> catalogConfig) {
    super(key, description, defaultValue, catalogConfig);
  }

  public static final BehaviorChangeConfiguration<Boolean> VALIDATE_VIEW_LOCATION_OVERLAP =
      PolarisConfiguration.<Boolean>builder()
          .key("STORAGE_CREDENTIAL_CACHE_DURATION_SECONDS")
          .description("If true, validate that view locations don't overlap when views are created")
          .defaultValue(true)
          .buildBehaviorChangeConfiguration();
}
