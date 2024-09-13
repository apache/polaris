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
package org.apache.polaris.core;

import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

@PolarisImmutable
public interface PolarisConfiguration<T> {

  String key();

  String description();

  T defaultValue();

  Optional<String> catalogConfig();

  @Value.Derived
  default boolean hasCatalogConfig() {
    return catalogConfig().isPresent();
  }

  @Value.Lazy
  default String catalogConfigOrThrow() {
    return catalogConfig()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Attempted to read a catalog config key from a configuration that doesn't have one."));
  }

  @Value.Derived
  default T cast(Object value) {
    @SuppressWarnings("unchecked")
    T cast = (T) defaultValue().getClass().cast(value);
    return cast;
  }

  static <T> ImmutablePolarisConfiguration.Builder<T> builder() {
    return ImmutablePolarisConfiguration.builder();
  }

  PolarisConfiguration<Boolean> ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING =
      PolarisConfiguration.<Boolean>builder()
          .key("ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING")
          .description(
              "If set to true, require that principals must rotate their credentials before being used "
                  + "for anything else.")
          .defaultValue(false)
          .build();

  PolarisConfiguration<Boolean> ALLOW_TABLE_LOCATION_OVERLAP =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_TABLE_LOCATION_OVERLAP")
          .catalogConfig("allow.overlapping.table.location")
          .description(
              "If set to true, allow one table's location to reside within another table's location. "
                  + "This is only enforced within a given namespace.")
          .defaultValue(false)
          .build();

  PolarisConfiguration<Boolean> ALLOW_NAMESPACE_LOCATION_OVERLAP =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_NAMESPACE_LOCATION_OVERLAP")
          .description(
              "If set to true, allow one table's location to reside within another table's location. "
                  + "This is only enforced within a parent catalog or namespace.")
          .defaultValue(false)
          .build();

  PolarisConfiguration<Boolean> ALLOW_EXTERNAL_METADATA_FILE_LOCATION =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_EXTERNAL_METADATA_FILE_LOCATION")
          .description(
              "If set to true, allows metadata files to be located outside the default metadata directory.")
          .defaultValue(false)
          .build();

  PolarisConfiguration<Boolean> ALLOW_OVERLAPPING_CATALOG_URLS =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_OVERLAPPING_CATALOG_URLS")
          .description("If set to true, allows catalog URLs to overlap.")
          .defaultValue(false)
          .build();

  PolarisConfiguration<Boolean> ALLOW_UNSTRUCTURED_TABLE_LOCATION =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_UNSTRUCTURED_TABLE_LOCATION")
          .catalogConfig("allow.unstructured.table.location")
          .description("If set to true, allows unstructured table locations.")
          .defaultValue(false)
          .build();

  PolarisConfiguration<Boolean> ALLOW_EXTERNAL_TABLE_LOCATION =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_EXTERNAL_TABLE_LOCATION")
          .catalogConfig("allow.external.table.location")
          .description(
              "If set to true, allows tables to have external locations outside the default structure.")
          .defaultValue(false)
          .build();

  PolarisConfiguration<Boolean> CLEANUP_ON_NAMESPACE_DROP =
      PolarisConfiguration.<Boolean>builder()
          .key("CLEANUP_ON_NAMESPACE_DROP")
          .catalogConfig("cleanup.on.namespace.drop")
          .description("If set to true, clean up data when a namespace is dropped")
          .defaultValue(false)
          .build();

  PolarisConfiguration<Boolean> CLEANUP_ON_CATALOG_DROP =
      PolarisConfiguration.<Boolean>builder()
          .key("CLEANUP_ON_CATALOG_DROP")
          .catalogConfig("cleanup.on.catalog.drop")
          .description("If set to true, clean up data when a catalog is dropped")
          .defaultValue(false)
          .build();

  PolarisConfiguration<Boolean> DROP_WITH_PURGE_ENABLED =
      PolarisConfiguration.<Boolean>builder()
          .key("DROP_WITH_PURGE_ENABLED")
          .catalogConfig("drop-with-purge.enabled")
          .description(
              "If set to true, allows tables to be dropped with the purge parameter set to true.")
          .defaultValue(true)
          .build();
}
