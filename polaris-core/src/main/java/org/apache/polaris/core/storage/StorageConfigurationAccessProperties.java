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

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * Folds {@link PolarisStorageConfigurationInfo} typed fields and freeform properties into a {@link
 * StorageAccessConfig}. Typed fields win over bag entries when both are set.
 */
@NullMarked
public final class StorageConfigurationAccessProperties {

  private StorageConfigurationAccessProperties() {}

  /**
   * Builds an AccessConfig that carries storage-configuration FileIO settings without vending
   * credentials. Used when credential subscoping is skipped so server FileIO still sees endpoint /
   * path-style / bag entries.
   */
  public static StorageAccessConfig storageConfigOnly(
      @Nullable PolarisStorageConfigurationInfo storageConfig) {
    StorageAccessConfig.Builder builder =
        StorageAccessConfig.builder().supportsCredentialVending(false);
    apply(storageConfig, builder);
    return builder.build();
  }

  /** Applies bag then typed storage-configuration fields onto {@code builder}. */
  public static void apply(
      @Nullable PolarisStorageConfigurationInfo storageConfig,
      StorageAccessConfig.Builder builder) {
    if (storageConfig == null) {
      return;
    }
    Map<String, String> extras = new HashMap<>();
    Map<String, String> internals = new HashMap<>();
    // Credential-looking bag keys go to internalProperties so server FileIO can use them without
    // treating them as vended AccessConfig.credentials() (matches former table-default behavior).
    partitionBag(storageConfig.getPropertiesOrEmpty(), extras, internals);
    if (storageConfig instanceof AwsStorageConfigurationInfo aws) {
      applyAwsTypedFields(aws, extras, internals);
    }
    extras.forEach(builder::putExtraProperty);
    internals.forEach(builder::putInternalProperty);
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
    extras.putAll(accessConfig.extraProperties());
    internals.putAll(accessConfig.internalProperties());
    StorageAccessConfig.Builder builder =
        StorageAccessConfig.builder()
            .supportsCredentialVending(accessConfig.supportsCredentialVending());
    accessConfig.credentials().forEach(builder::putCredential);
    extras.forEach(builder::putExtraProperty);
    internals.forEach(builder::putInternalProperty);
    accessConfig.expiresAt().ifPresent(builder::expiresAt);
    return builder.build();
  }

  /**
   * Splits a freeform bag into client-visible extras vs server-only internals. Keys that match
   * known credential property names go to internals so they feed FileIO without becoming vended
   * credentials.
   */
  private static void partitionBag(
      Map<String, String> bag, Map<String, String> extras, Map<String, String> internals) {
    for (Map.Entry<String, String> entry : bag.entrySet()) {
      if (entry.getKey() == null || entry.getValue() == null) {
        continue;
      }
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

  private static void applyAwsTypedFields(
      AwsStorageConfigurationInfo aws, Map<String, String> extras, Map<String, String> internals) {
    URI endpointUri = aws.getEndpointUri();
    if (endpointUri != null) {
      extras.put(StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), endpointUri.toString());
    }
    URI internalEndpointUri = aws.getInternalEndpointUri();
    if (internalEndpointUri != null) {
      internals.put(
          StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), internalEndpointUri.toString());
    }
    if (Boolean.TRUE.equals(aws.getPathStyleAccess())) {
      extras.put(
          StorageAccessProperty.AWS_PATH_STYLE_ACCESS.getPropertyName(), Boolean.TRUE.toString());
    }
    if (aws.getRegion() != null) {
      extras.put(StorageAccessProperty.CLIENT_REGION.getPropertyName(), aws.getRegion());
    }
  }
}
