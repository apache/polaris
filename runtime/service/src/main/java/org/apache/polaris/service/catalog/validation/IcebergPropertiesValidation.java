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
package org.apache.polaris.service.catalog.validation;

import static org.apache.polaris.core.config.FeatureConfiguration.ALLOW_INSECURE_STORAGE_TYPES;
import static org.apache.polaris.core.config.FeatureConfiguration.ALLOW_SPECIFYING_FILE_IO_IMPL;
import static org.apache.polaris.core.config.FeatureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergPropertiesValidation {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergPropertiesValidation.class);

  public static void validateIcebergProperties(
      @Nonnull CallContext callContext, @Nonnull Map<String, String> properties) {
    determineFileIOClassName(callContext, properties, null);
  }

  public static String determineFileIOClassName(
      @Nonnull CallContext callContext,
      @Nonnull Map<String, String> properties,
      @Nullable PolarisStorageConfigurationInfo storageConfigurationInfo) {
    var realmConfig = callContext.getPolarisCallContext().getRealmConfig();
    var ioImpl = properties.get(CatalogProperties.FILE_IO_IMPL);

    if (ioImpl != null) {
      if (!realmConfig.getConfig(ALLOW_SPECIFYING_FILE_IO_IMPL)) {
        throw new ValidationException(
            "Cannot set property '%s' to '%s' for this catalog.",
            CatalogProperties.FILE_IO_IMPL, ioImpl);
      }
      LOGGER.debug(
          "Allowing overriding ioImplClassName to {} for storageConfiguration {}",
          ioImpl,
          storageConfigurationInfo);
    } else if (storageConfigurationInfo != null) {
      ioImpl = storageConfigurationInfo.getFileIoImplClassName();
      LOGGER.debug(
          "Resolved ioImplClassName {} from storageConfiguration {}",
          ioImpl,
          storageConfigurationInfo);
    }

    if (ioImpl != null) {
      var storageType = StorageTypeFileIO.fromFileIoImplementation(ioImpl);
      if (storageType.validateAllowedStorageType()
          && !realmConfig.getConfig(SUPPORTED_CATALOG_STORAGE_TYPES).contains(storageType.name())) {
        throw new ValidationException(
            "File IO implementation '%s', as storage type '%s' is not supported",
            ioImpl, storageType);
      }

      if (!storageType.safe() && !realmConfig.getConfig(ALLOW_INSECURE_STORAGE_TYPES)) {
        throw new ValidationException(
            "File IO implementation '%s' (storage type '%s') is considered insecure and must not be used",
            ioImpl, storageType);
      }
    }

    return ioImpl;
  }

  public static boolean safeStorageType(String name) {
    return StorageTypeFileIO.valueOf(name).safe();
  }
}
