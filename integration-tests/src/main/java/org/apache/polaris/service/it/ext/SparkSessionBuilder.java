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
package org.apache.polaris.service.it.ext;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.spark.sql.SparkSession;

/**
 * A fluent builder for configuring SparkSession instances with Polaris catalogs.
 *
 * <p>Example usage:
 *
 * <pre>
 * SparkSession session = SparkSessionBuilder
 *     .withTestDefaults()
 *     .addCatalog("catalog1", CatalogType.ICEBERG, endpoints, token)
 *     .addCatalog("catalog2", CatalogType.ICEBERG, endpoints, token)
 *     .createSession();
 * </pre>
 */
public class SparkSessionBuilder {

  public enum CatalogType {
    ICEBERG("org.apache.iceberg.spark.SparkCatalog"),
    POLARIS("org.apache.polaris.spark.SparkCatalog");

    private final String implementationClass;

    CatalogType(String implementationClass) {
      this.implementationClass = implementationClass;
    }

    public String getImplementationClass() {
      return implementationClass;
    }
  }

  public enum ExtensionType {
    ICEBERG_ONLY("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
    ICEBERG_AND_DELTA(
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension");

    private final String extensionClasses;

    ExtensionType(String extensionClasses) {
      this.extensionClasses = extensionClasses;
    }

    public String getExtensionClasses() {
      return extensionClasses;
    }
  }

  private static class ConfigPair {
    final String key;
    final String value;

    ConfigPair(String key, String value) {
      this.key = key;
      this.value = value;
    }
  }

  /** Configuration for a single catalog. */
  private static class CatalogConfig {
    final String catalogName;
    final CatalogType catalogType;
    final PolarisApiEndpoints endpoints;
    final String token;
    final List<ConfigPair> catalogSpecificConfigs;

    CatalogConfig(
        String catalogName,
        CatalogType catalogType,
        PolarisApiEndpoints endpoints,
        String token,
        List<ConfigPair> catalogSpecificConfigs) {
      this.catalogName = catalogName;
      this.catalogType = catalogType;
      this.endpoints = endpoints;
      this.token = token;
      this.catalogSpecificConfigs =
          catalogSpecificConfigs != null ? catalogSpecificConfigs : new ArrayList<>();
    }
  }

  private final SparkSession.Builder builder;
  private final List<CatalogConfig> catalogs = new ArrayList<>();
  private final List<ConfigPair> additionalConfigs = new ArrayList<>();

  private ExtensionType extensionType = ExtensionType.ICEBERG_ONLY;
  private URI warehouseDir;
  private boolean includeDeltaCatalogConfig = false;

  private SparkSessionBuilder(SparkSession.Builder builder) {
    this.builder = builder;
  }

  /**
   * Create a SparkSessionBuilder with common test defaults: local master and disabled UI.
   *
   * @return new builder instance with test defaults
   */
  public static SparkSessionBuilder withTestDefaults() {
    return new SparkSessionBuilder(SparkSession.builder()).withLocalMaster().withDisabledUI();
  }

  public SparkSessionBuilder master(String master) {
    this.builder.master(master);
    return this;
  }

  public SparkSessionBuilder appName(String name) {
    this.builder.appName(name);
    return this;
  }

  public SparkSessionBuilder withLocalMaster(int cores) {
    return master(String.format("local[%d]", cores));
  }

  public SparkSessionBuilder withLocalMaster() {
    return withLocalMaster(1);
  }

  public SparkSessionBuilder withDisabledUI() {
    return withConfig("spark.ui.showConsoleProgress", "false")
        .withConfig("spark.ui.enabled", "false");
  }

  public SparkSessionBuilder withWarehouse(URI warehouseDir) {
    this.warehouseDir = warehouseDir;
    return this;
  }

  public SparkSessionBuilder withExtensions(ExtensionType extensionType) {
    this.extensionType = extensionType;
    return this;
  }

  public SparkSessionBuilder withDeltaCatalogConfig() {
    this.includeDeltaCatalogConfig = true;
    return this;
  }

  public SparkSessionBuilder withS3MockContainer() {
    return withS3FileSystem("foo", "bar");
  }

  public SparkSessionBuilder withS3FileSystem(String accessKey, String secretKey) {
    return withConfig("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .withConfig(
            "spark.hadoop.fs.s3.aws.credentials.provider",
            "org.apache.hadoop.fs.s3.TemporaryAWSCredentialsProvider")
        .withConfig("spark.hadoop.fs.s3.access.key", accessKey)
        .withConfig("spark.hadoop.fs.s3.secret.key", secretKey);
  }

  public SparkSessionBuilder addCatalog(
      String catalogName, CatalogType catalogType, PolarisApiEndpoints endpoints, String token) {
    this.catalogs.add(
        new CatalogConfig(catalogName, catalogType, endpoints, token, new ArrayList<>()));
    return this;
  }

  public SparkSessionBuilder withConfig(String key, String value) {
    this.additionalConfigs.add(new ConfigPair(key, value));
    return this;
  }

  public SparkSession createSession() {
    return build().getOrCreate();
  }

  /**
   * Build the underlying SparkSession.Builder with all configurations applied. Use this if you need
   * to do additional configuration before creating the session.
   *
   * @return configured SparkSession.Builder
   */
  public SparkSession.Builder build() {
    SparkSession.Builder configuredBuilder = builder;

    // Apply core configurations
    configuredBuilder = applyExtensions(configuredBuilder);
    configuredBuilder = applyDeltaConfig(configuredBuilder);
    configuredBuilder = applyWarehouseConfig(configuredBuilder);

    // Apply catalog configurations
    configuredBuilder = applyCatalogConfigurations(configuredBuilder);

    // Apply additional configurations
    configuredBuilder = applyAdditionalConfigurations(configuredBuilder);

    return configuredBuilder;
  }

  private SparkSession.Builder applyExtensions(SparkSession.Builder builder) {
    return builder.config("spark.sql.extensions", extensionType.getExtensionClasses());
  }

  private SparkSession.Builder applyDeltaConfig(SparkSession.Builder builder) {
    if (includeDeltaCatalogConfig) {
      builder =
          builder.config(
              "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog");
    }
    return builder;
  }

  private SparkSession.Builder applyWarehouseConfig(SparkSession.Builder builder) {
    if (warehouseDir != null) {
      builder = builder.config("spark.sql.warehouse.dir", warehouseDir.toString());
    }
    return builder;
  }

  private SparkSession.Builder applyCatalogConfigurations(SparkSession.Builder builder) {
    for (CatalogConfig catalog : catalogs) {
      builder = applySingleCatalogConfig(builder, catalog);
    }
    return builder;
  }

  private SparkSession.Builder applySingleCatalogConfig(
      SparkSession.Builder builder, CatalogConfig catalog) {
    // Basic catalog configuration
    builder =
        builder
            .config(
                String.format("spark.sql.catalog.%s", catalog.catalogName),
                catalog.catalogType.getImplementationClass())
            .config(String.format("spark.sql.catalog.%s.type", catalog.catalogName), "rest")
            .config(
                String.format("spark.sql.catalog.%s.warehouse", catalog.catalogName),
                catalog.catalogName)
            .config(
                String.format("spark.sql.catalog.%s.scope", catalog.catalogName),
                "PRINCIPAL_ROLE:ALL");

    // Add endpoint configuration
    if (catalog.endpoints != null) {
      builder =
          builder
              .config(
                  String.format("spark.sql.catalog.%s.uri", catalog.catalogName),
                  catalog.endpoints.catalogApiEndpoint().toString())
              .config(
                  String.format("spark.sql.catalog.%s.header.realm", catalog.catalogName),
                  catalog.endpoints.realmId());
    }

    // Add token configuration
    if (catalog.token != null) {
      builder =
          builder.config(
              String.format("spark.sql.catalog.%s.token", catalog.catalogName), catalog.token);
    }

    // Add catalog-specific configurations
    for (ConfigPair config : catalog.catalogSpecificConfigs) {
      builder = builder.config(config.key, config.value);
    }

    return builder;
  }

  private SparkSession.Builder applyAdditionalConfigurations(SparkSession.Builder builder) {
    for (ConfigPair config : additionalConfigs) {
      builder = builder.config(config.key, config.value);
    }
    return builder;
  }
}
