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
import java.util.List;

/**
 * Allows overriding the allowed locations for specific entities. Only the allowedLocations
 * specified in the constructor are allowed. allowedLocations are not inherited from the parent
 * storage configuration. All other storage configuration is inherited from the parent configuration
 * and cannot be overridden.
 */
public class StorageConfigurationOverride extends PolarisStorageConfigurationInfo {

  private final PolarisStorageConfigurationInfo parentStorageConfiguration;

  public StorageConfigurationOverride(
      @Nonnull PolarisStorageConfigurationInfo parentStorageConfiguration,
      List<String> allowedLocations) {
    super(parentStorageConfiguration.getStorageType(), allowedLocations, false);
    this.parentStorageConfiguration = parentStorageConfiguration;
    allowedLocations.forEach(this::validatePrefixForStorageType);
  }

  @Override
  public String getFileIoImplClassName() {
    return parentStorageConfiguration.getFileIoImplClassName();
  }

  // delegate to the wrapped class in case they override the parent behavior
  @Override
  protected void validatePrefixForStorageType(String loc) {
    parentStorageConfiguration.validatePrefixForStorageType(loc);
  }

  @Override
  public void validateMaxAllowedLocations(int maxAllowedLocations) {
    parentStorageConfiguration.validateMaxAllowedLocations(maxAllowedLocations);
  }
}
