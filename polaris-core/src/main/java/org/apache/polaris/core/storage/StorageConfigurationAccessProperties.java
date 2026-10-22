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

import java.util.HashMap;
import java.util.Map;
import org.jspecify.annotations.NullMarked;

/**
 * Folds {@code fileIoProperties} into a {@link StorageAccessConfig} as extra properties. Typed
 * storage-configuration fields are applied by the cloud-specific {@code PolarisStorageIntegration}
 * implementations. Typed fields win over bag entries when both are set.
 */
@NullMarked
public final class StorageConfigurationAccessProperties {

  private StorageConfigurationAccessProperties() {}

  /**
   * Builds an AccessConfig that carries {@code fileIoProperties} without vending credentials. Used
   * when cloud vending is skipped (S3 {@code stsUnavailable}, or Floci static FileIO credentials).
   */
  public static StorageAccessConfig storageConfigOnly(
      PolarisStorageConfigurationInfo storageConfig) {
    StorageAccessConfig.Builder builder =
        StorageAccessConfig.builder().supportsCredentialVending(false);
    apply(storageConfig, builder);
    return builder.build();
  }

  /** Forwards all {@code fileIoProperties} onto {@code builder} extra properties. */
  public static void apply(
      PolarisStorageConfigurationInfo storageConfig, StorageAccessConfig.Builder builder) {
    storageConfig.getFileIoPropertiesOrEmpty().forEach(builder::putExtraProperty);
  }

  /**
   * True when {@code fileIoProperties} already carries static FileIO credentials used by Floci
   * ADLS/GCP emulators so SAS/STS vending can be skipped — parallel to S3 {@code stsUnavailable}.
   */
  public static boolean hasStaticFileIoCredentials(PolarisStorageConfigurationInfo storageConfig) {
    for (Map.Entry<String, String> entry : storageConfig.getFileIoPropertiesOrEmpty().entrySet()) {
      if (isCredentialPropertyName(entry.getKey())
          || entry.getKey().startsWith("adls.auth.shared-key")
          || entry.getKey().startsWith("adls.connection-string.")) {
        return true;
      }
    }
    return false;
  }

  /**
   * Merges catalog {@code table-default.*} entries (already stripped of the prefix) into {@code
   * accessConfig}. Existing AccessConfig credentials / extras / internals win. Credential-looking
   * table-default keys land in internalProperties (server FileIO only).
   */
  public static StorageAccessConfig mergeTableDefaults(
      StorageAccessConfig accessConfig, Map<String, String> tableDefaultProperties) {
    if (tableDefaultProperties.isEmpty()) {
      return accessConfig;
    }
    Map<String, String> extras = new HashMap<>();
    Map<String, String> internals = new HashMap<>();
    partitionBag(tableDefaultProperties, extras, internals);
    // Start from AccessConfig; only add table-default keys that are not already set.
    StorageAccessConfig.Builder builder = StorageAccessConfig.builder().from(accessConfig);
    extras.forEach(
        (key, value) -> {
          if (!accessConfig.extraProperties().containsKey(key)) {
            builder.putExtraProperty(key, value);
          }
        });
    internals.forEach(
        (key, value) -> {
          if (!accessConfig.internalProperties().containsKey(key)) {
            builder.putInternalProperty(key, value);
          }
        });
    return builder.build();
  }

  /**
   * Splits FileIO properties into client-visible extras vs server-only internals. Keys that match
   * known credential property names go to internals so they feed FileIO without becoming vended
   * credentials.
   */
  private static void partitionBag(
      Map<String, String> bag, Map<String, String> extras, Map<String, String> internals) {
    for (Map.Entry<String, String> entry : bag.entrySet()) {
      if (isCredentialPropertyName(entry.getKey())) {
        internals.put(entry.getKey(), entry.getValue());
      } else {
        extras.put(entry.getKey(), entry.getValue());
      }
    }
  }

  private static boolean isCredentialPropertyName(String propertyName) {
    for (StorageAccessProperty property : StorageAccessProperty.values()) {
      if (property.isCredential() && property.getPropertyName().equals(propertyName)) {
        return true;
      }
    }
    return false;
  }
}
