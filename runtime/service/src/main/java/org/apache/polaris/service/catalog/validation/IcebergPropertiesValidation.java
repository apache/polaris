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
import static org.apache.polaris.core.config.FeatureConfiguration.SUPPORTED_S3_CREDENTIAL_ISSUERS;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.S3CredentialIssuer;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergPropertiesValidation {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergPropertiesValidation.class);

  public static void validateIcebergProperties(
      @NonNull RealmConfig realmConfig, @NonNull Map<String, String> properties) {
    determineFileIOClassName(realmConfig, properties, null);
  }

  public static String determineFileIOClassName(
      @NonNull RealmConfig realmConfig,
      @NonNull Map<String, String> properties,
      @Nullable PolarisStorageConfigurationInfo storageConfigurationInfo) {
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

  /**
   * The realm allowlist for S3 credential issuers. The list has no implicit member; {@code STS}
   * must be listed too. Used at catalog create and update.
   */
  public static void validateS3CredentialIssuerAllowed(
      @NonNull RealmConfig realmConfig, @NonNull S3CredentialIssuer issuer) {
    List<String> allowed = realmConfig.getConfig(SUPPORTED_S3_CREDENTIAL_ISSUERS);
    if (!allowed.contains(issuer.name())) {
      throw new ValidationException("S3 credential issuer %s is not enabled in this realm", issuer);
    }
  }

  /**
   * The allowlist plus build availability, for catalog initialization and storage-access
   * resolution. The storage integration provider uses the allowlist check and its own switch arm.
   * The {@code CLOUDFLARE_R2} throw below is removed by the change that adds its integration.
   */
  public static void validateS3CredentialIssuerAvailable(
      @NonNull RealmConfig realmConfig, @NonNull S3CredentialIssuer issuer) {
    validateS3CredentialIssuerAllowed(realmConfig, issuer);
    if (issuer == S3CredentialIssuer.CLOUDFLARE_R2) {
      throw new ValidationException(
          "S3 credential issuer CLOUDFLARE_R2 is not available in this build");
    }
  }

  /** {@link #validateS3CredentialIssuerAvailable(RealmConfig, S3CredentialIssuer)} for a config. */
  public static void validateS3CredentialIssuerAvailable(
      @NonNull RealmConfig realmConfig,
      @Nullable PolarisStorageConfigurationInfo storageConfigurationInfo) {
    if (storageConfigurationInfo instanceof AwsStorageConfigurationInfo awsConfig) {
      validateS3CredentialIssuerAvailable(realmConfig, awsConfig.getCredentialIssuer());
    }
  }

  public static boolean safeStorageType(String name) {
    return StorageTypeFileIO.valueOf(name).safe();
  }
}
