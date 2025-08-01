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
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.connection.ConnectionType;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.persistence.cache.EntityWeigher;

/**
 * Configurations for features within Polaris. These configurations are intended to be customized
 * and many expose user-facing catalog-level configurations. These configurations are stable over
 * time.
 *
 * @param <T>
 */
public class FeatureConfiguration<T> extends PolarisConfiguration<T> {
  protected FeatureConfiguration(
      String key,
      String description,
      T defaultValue,
      Optional<String> catalogConfig,
      Optional<String> catalogConfigUnsafe) {
    super(key, description, defaultValue, catalogConfig, catalogConfigUnsafe);
  }

  /**
   * Helper for the common scenario of gating a feature with a boolean FeatureConfiguration, where
   * we want to throw an UnsupportedOperationException if it's not enabled.
   */
  public static void enforceFeatureEnabledOrThrow(
      CallContext callContext, FeatureConfiguration<Boolean> featureConfig) {
    boolean enabled = callContext.getRealmConfig().getConfig(featureConfig);
    if (!enabled) {
      throw new UnsupportedOperationException("Feature not enabled: " + featureConfig.key());
    }
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

  @SuppressWarnings("deprecation")
  public static final FeatureConfiguration<Boolean> ALLOW_TABLE_LOCATION_OVERLAP =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_TABLE_LOCATION_OVERLAP")
          .catalogConfig("polaris.config.allow.overlapping.table.location")
          .catalogConfigUnsafe("allow.overlapping.table.location")
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

  @SuppressWarnings("deprecation")
  public static final FeatureConfiguration<Boolean> ALLOW_UNSTRUCTURED_TABLE_LOCATION =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_UNSTRUCTURED_TABLE_LOCATION")
          .catalogConfig("polaris.config.allow.unstructured.table.location")
          .catalogConfigUnsafe("allow.unstructured.table.location")
          .description("If set to true, allows unstructured table locations.")
          .defaultValue(false)
          .buildFeatureConfiguration();

  @SuppressWarnings("deprecation")
  public static final FeatureConfiguration<Boolean> ALLOW_EXTERNAL_TABLE_LOCATION =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_EXTERNAL_TABLE_LOCATION")
          .catalogConfig("polaris.config.allow.external.table.location")
          .catalogConfigUnsafe("allow.external.table.location")
          .description(
              "If set to true, allows tables to have external locations outside the default structure.")
          .defaultValue(false)
          .buildFeatureConfiguration();

  @SuppressWarnings("deprecation")
  public static final FeatureConfiguration<Boolean> ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING")
          .catalogConfig("polaris.config.enable.credential.vending")
          .catalogConfigUnsafe("enable.credential.vending")
          .description("If set to true, allow credential vending for external catalogs.")
          .defaultValue(true)
          .buildFeatureConfiguration();

  @SuppressWarnings("deprecation")
  public static final FeatureConfiguration<List<String>> SUPPORTED_CATALOG_STORAGE_TYPES =
      PolarisConfiguration.<List<String>>builder()
          .key("SUPPORTED_CATALOG_STORAGE_TYPES")
          .catalogConfig("polaris.config.supported.storage.types")
          .catalogConfigUnsafe("supported.storage.types")
          .description("The list of supported storage types for a catalog")
          .defaultValue(
              List.of(
                  StorageConfigInfo.StorageTypeEnum.S3.name(),
                  StorageConfigInfo.StorageTypeEnum.AZURE.name(),
                  StorageConfigInfo.StorageTypeEnum.GCS.name()))
          .buildFeatureConfiguration();

  @SuppressWarnings("deprecation")
  public static final FeatureConfiguration<Boolean> CLEANUP_ON_NAMESPACE_DROP =
      PolarisConfiguration.<Boolean>builder()
          .key("CLEANUP_ON_NAMESPACE_DROP")
          .catalogConfig("polaris.config.cleanup.on.namespace.drop")
          .catalogConfigUnsafe("cleanup.on.namespace.drop")
          .description("If set to true, clean up data when a namespace is dropped")
          .defaultValue(false)
          .buildFeatureConfiguration();

  @SuppressWarnings("deprecation")
  public static final FeatureConfiguration<Boolean> CLEANUP_ON_CATALOG_DROP =
      PolarisConfiguration.<Boolean>builder()
          .key("CLEANUP_ON_CATALOG_DROP")
          .catalogConfig("polaris.config.cleanup.on.catalog.drop")
          .catalogConfigUnsafe("cleanup.on.catalog.drop")
          .description("If set to true, clean up data when a catalog is dropped")
          .defaultValue(false)
          .buildFeatureConfiguration();

  @SuppressWarnings("deprecation")
  public static final FeatureConfiguration<Boolean> DROP_WITH_PURGE_ENABLED =
      PolarisConfiguration.<Boolean>builder()
          .key("DROP_WITH_PURGE_ENABLED")
          .catalogConfig("polaris.config.drop-with-purge.enabled")
          .catalogConfigUnsafe("drop-with-purge.enabled")
          .description(
              "If set to true, allows tables to be dropped with the purge parameter set to true.")
          .defaultValue(false)
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
                  + STORAGE_CREDENTIAL_DURATION_SECONDS.key())
          .defaultValue(30 * 60) // 30 minutes
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<Integer> MAX_METADATA_REFRESH_RETRIES =
      PolarisConfiguration.<Integer>builder()
          .key("MAX_METADATA_REFRESH_RETRIES")
          .description(
              "How many times to retry refreshing metadata when the previous error was retryable")
          .defaultValue(2)
          .buildFeatureConfiguration();

  public static final PolarisConfiguration<Boolean> LIST_PAGINATION_ENABLED =
      PolarisConfiguration.<Boolean>builder()
          .key("LIST_PAGINATION_ENABLED")
          .catalogConfig("polaris.config.list-pagination-enabled")
          .description("If set to true, pagination for APIs like listTables is enabled.")
          .defaultValue(false)
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<Boolean> ENABLE_GENERIC_TABLES =
      PolarisConfiguration.<Boolean>builder()
          .key("ENABLE_GENERIC_TABLES")
          .description("If true, the generic-tables endpoints are enabled")
          .defaultValue(true)
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<Long> ENTITY_CACHE_WEIGHER_TARGET =
      PolarisConfiguration.<Long>builder()
          .key("ENTITY_CACHE_WEIGHER_TARGET")
          .description(
              "The maximum weight for the entity cache. This is a heuristic value without any particular"
                  + " unit of measurement. It roughly correlates with the total heap size of cached values. Fine-tuning"
                  + " requires experimentation in the specific deployment environment")
          .defaultValue(100 * EntityWeigher.WEIGHT_PER_MB)
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<Boolean> ENABLE_CATALOG_FEDERATION =
      PolarisConfiguration.<Boolean>builder()
          .key("ENABLE_CATALOG_FEDERATION")
          .description(
              "If true, allows creating and using ExternalCatalogs containing ConnectionConfigInfos"
                  + " to perform federation to remote catalogs.")
          .defaultValue(false)
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<Boolean> ENABLE_POLICY_STORE =
      PolarisConfiguration.<Boolean>builder()
          .key("ENABLE_POLICY_STORE")
          .description("If true, the policy-store endpoints are enabled")
          .defaultValue(true)
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<List<String>> SUPPORTED_CATALOG_CONNECTION_TYPES =
      PolarisConfiguration.<List<String>>builder()
          .key("SUPPORTED_CATALOG_CONNECTION_TYPES")
          .description("The list of supported catalog connection types for federation")
          .defaultValue(List.of(ConnectionType.ICEBERG_REST.name()))
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<List<String>>
      SUPPORTED_EXTERNAL_CATALOG_AUTHENTICATION_TYPES =
          PolarisConfiguration.<List<String>>builder()
              .key("SUPPORTED_EXTERNAL_CATALOG_AUTHENTICATION_TYPES")
              .description("The list of supported authentication types for catalog federation")
              .defaultValue(
                  List.of(
                      AuthenticationParameters.AuthenticationTypeEnum.OAUTH.name(),
                      AuthenticationParameters.AuthenticationTypeEnum.BEARER.name()))
              .buildFeatureConfiguration();

  public static final FeatureConfiguration<Integer> ICEBERG_COMMIT_MAX_RETRIES =
      PolarisConfiguration.<Integer>builder()
          .key("ICEBERG_COMMIT_MAX_RETRIES")
          .catalogConfig("polaris.config.iceberg-commit-max-retries")
          .description("The max number of times to try committing to an Iceberg table")
          .defaultValue(4)
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<Boolean> ALLOW_SPECIFYING_FILE_IO_IMPL =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_SPECIFYING_FILE_IO_IMPL")
          .description(
              "Config key for whether to allow setting the FILE_IO_IMPL using catalog properties. "
                  + "Must only be enabled in dev/test environments, should not be in production systems.")
          .defaultValue(false)
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<Boolean> ALLOW_INSECURE_STORAGE_TYPES =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_INSECURE_STORAGE_TYPES")
          .description(
              "Allow usage of FileIO implementations that are considered insecure. "
                  + "Enabling this setting may expose the service to possibly severe security risks! "
                  + "This should only be set to 'true' for tests!")
          .defaultValue(false)
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<Boolean> ICEBERG_ROLLBACK_COMPACTION_ON_CONFLICTS =
      PolarisConfiguration.<Boolean>builder()
          .key("ICEBERG_ROLLBACK_COMPACTION_ON_CONFLICTS")
          .catalogConfig("polaris.config.rollback.compaction.on-conflicts.enabled")
          .description(
              "Rollback replace snapshots created by compaction which have "
                  + "polaris.internal.conflict-resolution.by-operation-type.replace property set to rollback "
                  + "in their snapshot summary")
          .defaultValue(false)
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<Boolean> ADD_TRAILING_SLASH_TO_LOCATION =
      PolarisConfiguration.<Boolean>builder()
          .key("ADD_TRAILING_SLASH_TO_LOCATION")
          .catalogConfig("polaris.config.add-trailing-slash-to-location")
          .description(
              "When set, the base location for a table or namespace will have `/` added as a suffix if not present")
          .defaultValue(true)
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<Boolean> OPTIMIZED_SIBLING_CHECK =
      PolarisConfiguration.<Boolean>builder()
          .key("OPTIMIZED_SIBLING_CHECK")
          .description(
              "When set, an index is used to perform the sibling check between tables, views, and namespaces. New "
                  + "locations will be checked against previous ones based on components, so the new location "
                  + "/foo/bar/ will check for a sibling at /, /foo/ and /foo/bar/%. In order for this check to "
                  + "be correct, locations should end with a slash. See ADD_TRAILING_SLASH_TO_LOCATION for a way "
                  + "to enforce this when new locations are added. Only supported by the JDBC metastore.")
          .defaultValue(false)
          .buildFeatureConfiguration();

  public static final FeatureConfiguration<Boolean> DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED =
      PolarisConfiguration.<Boolean>builder()
          .key("DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED")
          .catalogConfig("polaris.config.default-table-location-object-storage-prefix.enabled")
          .description(
              "When enabled, Iceberg tables and views created without a location specified will have a prefix "
                  + "applied to the location within the catalog's base location, rather than a location directly "
                  + "inside the parent namespace. Note that this requires ALLOW_EXTERNAL_TABLE_LOCATION to be "
                  + "enabled, but with OPTIMIZED_SIBLING_CHECK enabled "
                  + "it is still possible to enforce the uniqueness of table locations within a catalog.")
          .defaultValue(false)
          .buildFeatureConfiguration();
}
