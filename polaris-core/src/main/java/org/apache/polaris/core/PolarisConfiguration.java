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

import java.util.List;
import java.util.Optional;
import org.apache.polaris.core.admin.model.StorageConfigInfo;

public class PolarisConfiguration<T> {

  public final String key;
  public final String description;
  public final T defaultValue;
  private final Optional<String> catalogConfigImpl;
  private final Class<T> typ;

  @SuppressWarnings("unchecked")
  public PolarisConfiguration(
      String key, String description, T defaultValue, Optional<String> catalogConfig) {
    this.key = key;
    this.description = description;
    this.defaultValue = defaultValue;
    this.catalogConfigImpl = catalogConfig;
    this.typ = (Class<T>) defaultValue.getClass();
  }

  public boolean hasCatalogConfig() {
    return catalogConfigImpl.isPresent();
  }

  public String catalogConfig() {
    return catalogConfigImpl.orElseThrow(
        () ->
            new IllegalStateException(
                "Attempted to read a catalog config key from a configuration that doesn't have one."));
  }

  T cast(Object value) {
    return this.typ.cast(value);
  }

  public static class Builder<T> {
    private String key;
    private String description;
    private T defaultValue;
    private Optional<String> catalogConfig = Optional.empty();

    public Builder<T> key(String key) {
      this.key = key;
      return this;
    }

    public Builder<T> description(String description) {
      this.description = description;
      return this;
    }

    public Builder<T> defaultValue(T defaultValue) {
      this.defaultValue = defaultValue;
      return this;
    }

    public Builder<T> catalogConfig(String catalogConfig) {
      this.catalogConfig = Optional.of(catalogConfig);
      return this;
    }

    public PolarisConfiguration<T> build() {
      if (key == null || description == null || defaultValue == null) {
        throw new IllegalArgumentException("key, description, and defaultValue are required");
      }
      return new PolarisConfiguration<>(key, description, defaultValue, catalogConfig);
    }
  }

  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  public static final PolarisConfiguration<Boolean>
      ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING =
          PolarisConfiguration.<Boolean>builder()
              .key("ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING")
              .description(
                  "If set to true, require that principals must rotate their credentials before being used "
                      + "for anything else.")
              .defaultValue(false)
              .build();

  public static final PolarisConfiguration<Boolean> ALLOW_TABLE_LOCATION_OVERLAP =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_TABLE_LOCATION_OVERLAP")
          .catalogConfig("allow.overlapping.table.location")
          .description(
              "If set to true, allow one table's location to reside within another table's location. "
                  + "This is only enforced within a given namespace.")
          .defaultValue(false)
          .build();

  public static final PolarisConfiguration<Boolean> ALLOW_NAMESPACE_LOCATION_OVERLAP =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_NAMESPACE_LOCATION_OVERLAP")
          .description(
              "If set to true, allow one namespace's location to reside within another namespace's location. "
                  + "This is only enforced within a parent catalog or namespace.")
          .defaultValue(false)
          .build();

  public static final PolarisConfiguration<Boolean> ALLOW_EXTERNAL_METADATA_FILE_LOCATION =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_EXTERNAL_METADATA_FILE_LOCATION")
          .description(
              "If set to true, allows metadata files to be located outside the default metadata directory.")
          .defaultValue(false)
          .build();

  public static final PolarisConfiguration<Boolean> ALLOW_OVERLAPPING_CATALOG_URLS =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_OVERLAPPING_CATALOG_URLS")
          .description("If set to true, allows catalog URLs to overlap.")
          .defaultValue(false)
          .build();

  public static final PolarisConfiguration<Boolean> ALLOW_UNSTRUCTURED_TABLE_LOCATION =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_UNSTRUCTURED_TABLE_LOCATION")
          .catalogConfig("allow.unstructured.table.location")
          .description("If set to true, allows unstructured table locations.")
          .defaultValue(false)
          .build();

  public static final PolarisConfiguration<Boolean> ALLOW_EXTERNAL_TABLE_LOCATION =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_EXTERNAL_TABLE_LOCATION")
          .catalogConfig("allow.external.table.location")
          .description(
              "If set to true, allows tables to have external locations outside the default structure.")
          .defaultValue(false)
          .build();

  public static final PolarisConfiguration<Boolean> ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING")
          .catalogConfig("enable.credential.vending")
          .description("If set to true, allow credential vending for external catalogs.")
          .defaultValue(true)
          .build();

  public static final PolarisConfiguration<List<String>> SUPPORTED_CATALOG_STORAGE_TYPES =
      PolarisConfiguration.<List<String>>builder()
          .key("SUPPORTED_CATALOG_STORAGE_TYPES")
          .catalogConfig("supported.storage.types")
          .description("The list of supported storage types for a catalog")
          .defaultValue(
              List.of(
                  StorageConfigInfo.StorageTypeEnum.S3.name(),
                  StorageConfigInfo.StorageTypeEnum.S3_COMPATIBLE.name(),
                  StorageConfigInfo.StorageTypeEnum.AZURE.name(),
                  StorageConfigInfo.StorageTypeEnum.GCS.name(),
                  StorageConfigInfo.StorageTypeEnum.FILE.name()))
          .build();

  public static final PolarisConfiguration<Boolean> CLEANUP_ON_NAMESPACE_DROP =
      PolarisConfiguration.<Boolean>builder()
          .key("CLEANUP_ON_NAMESPACE_DROP")
          .catalogConfig("cleanup.on.namespace.drop")
          .description("If set to true, clean up data when a namespace is dropped")
          .defaultValue(false)
          .build();

  public static final PolarisConfiguration<Boolean> CLEANUP_ON_CATALOG_DROP =
      PolarisConfiguration.<Boolean>builder()
          .key("CLEANUP_ON_CATALOG_DROP")
          .catalogConfig("cleanup.on.catalog.drop")
          .description("If set to true, clean up data when a catalog is dropped")
          .defaultValue(false)
          .build();

  public static final PolarisConfiguration<Boolean> DROP_WITH_PURGE_ENABLED =
      PolarisConfiguration.<Boolean>builder()
          .key("DROP_WITH_PURGE_ENABLED")
          .catalogConfig("drop-with-purge.enabled")
          .description(
              "If set to true, allows tables to be dropped with the purge parameter set to true.")
          .defaultValue(true)
          .build();
}
