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

import java.util.List;
import java.util.Optional;
import org.apache.polaris.core.admin.model.StorageConfigInfo;

/**
 * Configurations for features within Polaris. These configurations are intended to be customized
 * and many expose user-facing catalog-level configurations. These configurations are stable over
 * time.
 *
 * @param <T>
 */
public class FeatureConfiguration<T> extends PolarisConfiguration<T> {
  protected FeatureConfiguration(
      String key, String description, T defaultValue, Optional<String> catalogConfig) {
    super(key, description, defaultValue, catalogConfig);
  }

  public static final FeatureConfiguration<Boolean>
      ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING =
          PolarisConfiguration.<Boolean>builder()
              .key("ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING")
              .description(
                  "If set to true, require that principals must rotate their credentials before being used "
                      + "for anything else.")
              .defaultValue(false)
              .buildFeatureConfiguration();

  public static final FeatureConfiguration<Boolean> SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION =
      PolarisConfiguration.<Boolean>builder()
          .key("SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION")
          .description(
              "If set to true, skip credential-subscoping indirection entirely whenever trying\n"
                  + "   to obtain storage credentials for instantiating a FileIO. If 'true', no attempt is made\n"
                  + "   to use StorageConfigs to generate table-specific storage credentials, but instead the default\n"
                  + "   fallthrough of table-level credential properties or else provider-specific APPLICATION_DEFAULT\n"
                  + "   credential-loading will be used for the FileIO.\n"
                  + "   Typically this setting is used in single-tenant server deployments that don't rely on\n"
                  + "   \"credential-vending\" and can use server-default environment variables or credential config\n"
                  + "   files for all storage access, or in test/dev scenarios.")
          .defaultValue(false)
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<Boolean> ALLOW_TABLE_LOCATION_OVERLAP =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_TABLE_LOCATION_OVERLAP")
          .catalogConfig("allow.overlapping.table.location")
          .description(
              "If set to true, allow one table's location to reside within another table's location. "
                  + "This is only enforced within a given namespace.")
          .defaultValue(false)
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<Boolean> ALLOW_NAMESPACE_LOCATION_OVERLAP =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_NAMESPACE_LOCATION_OVERLAP")
          .description(
              "If set to true, allow one namespace's location to reside within another namespace's location. "
                  + "This is only enforced within a parent catalog or namespace.")
          .defaultValue(false)
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<Boolean> ALLOW_EXTERNAL_METADATA_FILE_LOCATION =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_EXTERNAL_METADATA_FILE_LOCATION")
          .description(
              "If set to true, allows metadata files to be located outside the default metadata directory.")
          .defaultValue(false)
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<Boolean> ALLOW_OVERLAPPING_CATALOG_URLS =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_OVERLAPPING_CATALOG_URLS")
          .description("If set to true, allows catalog URLs to overlap.")
          .defaultValue(false)
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<Boolean> ALLOW_UNSTRUCTURED_TABLE_LOCATION =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_UNSTRUCTURED_TABLE_LOCATION")
          .catalogConfig("allow.unstructured.table.location")
          .description("If set to true, allows unstructured table locations.")
          .defaultValue(false)
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<Boolean> ALLOW_EXTERNAL_TABLE_LOCATION =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_EXTERNAL_TABLE_LOCATION")
          .catalogConfig("allow.external.table.location")
          .description(
              "If set to true, allows tables to have external locations outside the default structure.")
          .defaultValue(false)
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<Boolean> ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING")
          .catalogConfig("enable.credential.vending")
          .description("If set to true, allow credential vending for external catalogs.")
          .defaultValue(true)
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<List<String>> SUPPORTED_CATALOG_STORAGE_TYPES =
      PolarisConfiguration.<List<String>>builder()
          .key("SUPPORTED_CATALOG_STORAGE_TYPES")
          .catalogConfig("supported.storage.types")
          .description("The list of supported storage types for a catalog")
          .defaultValue(
              List.of(
                  StorageConfigInfo.StorageTypeEnum.S3.name(),
                  StorageConfigInfo.StorageTypeEnum.AZURE.name(),
                  StorageConfigInfo.StorageTypeEnum.GCS.name(),
                  StorageConfigInfo.StorageTypeEnum.FILE.name()))
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<Boolean> CLEANUP_ON_NAMESPACE_DROP =
      PolarisConfiguration.<Boolean>builder()
          .key("CLEANUP_ON_NAMESPACE_DROP")
          .catalogConfig("cleanup.on.namespace.drop")
          .description("If set to true, clean up data when a namespace is dropped")
          .defaultValue(false)
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<Boolean> CLEANUP_ON_CATALOG_DROP =
      PolarisConfiguration.<Boolean>builder()
          .key("CLEANUP_ON_CATALOG_DROP")
          .catalogConfig("cleanup.on.catalog.drop")
          .description("If set to true, clean up data when a catalog is dropped")
          .defaultValue(false)
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<Boolean> DROP_WITH_PURGE_ENABLED =
      PolarisConfiguration.<Boolean>builder()
          .key("DROP_WITH_PURGE_ENABLED")
          .catalogConfig("drop-with-purge.enabled")
          .description(
              "If set to true, allows tables to be dropped with the purge parameter set to true.")
          .defaultValue(true)
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<Integer> STORAGE_CREDENTIAL_DURATION_SECONDS =
      PolarisConfiguration.<Integer>builder()
          .key("STORAGE_CREDENTIAL_DURATION_SECONDS")
          .description(
              "The duration of time that vended storage credentials are valid for. Support for"
                  + " longer (or shorter) durations is dependent on the storage provider. GCS"
                  + " current does not respect this value.")
          .defaultValue(60 * 60) // 1 hour
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<Integer> STORAGE_CREDENTIAL_CACHE_DURATION_SECONDS =
      PolarisConfiguration.<Integer>builder()
          .key("STORAGE_CREDENTIAL_CACHE_DURATION_SECONDS")
          .description(
              "How long to store storage credentials in the local cache. This should be less than "
                  + STORAGE_CREDENTIAL_DURATION_SECONDS.key)
          .defaultValue(30 * 60) // 30 minutes
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<Integer> MAX_METADATA_REFRESH_RETRIES =
      PolarisConfiguration.<Integer>builder()
          .key("MAX_METADATA_REFRESH_RETRIES")
          .description(
              "How many times to retry refreshing metadata when the previous error was retryable")
          .defaultValue(2)
          .buildFeatureConfiguration();
}
