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

import com.google.common.base.Preconditions;
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
    final String catalogType;
    final PolarisApiEndpoints endpoints;
    final String token;
    final List<ConfigPair> catalogSpecificConfigs;

    CatalogConfig(
        String catalogName,
        String catalogType,
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

  private String extensions = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions";
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
    // local master
    var sparkSessionBuilder = SparkSession.builder();
    sparkSessionBuilder.master(String.format("local[%d]", 1));
    // disable UI
    sparkSessionBuilder.config("spark.ui.showConsoleProgress", "false");
    sparkSessionBuilder.config("spark.ui.enabled", "false");

    return new SparkSessionBuilder(sparkSessionBuilder);
  }

  public SparkSessionBuilder withS3MockContainer() {
    return withConfig("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .withConfig(
            "spark.hadoop.fs.s3.aws.credentials.provider",
            "org.apache.hadoop.fs.s3.TemporaryAWSCredentialsProvider")
        .withConfig("spark.hadoop.fs.s3.access.key", "foo")
        .withConfig("spark.hadoop.fs.s3.secret.key", "bar");
  }

  public SparkSessionBuilder withWarehouse(URI warehouseDir) {
    this.warehouseDir = warehouseDir;
    return this;
  }

  public SparkSessionBuilder withExtensions(String extensions) {
    this.extensions = extensions;
    return this;
  }

  public SparkSessionBuilder addCatalog(
      String catalogName, String catalogType, PolarisApiEndpoints endpoints, String token) {
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
    builder.config("spark.sql.extensions", extensions);

    // Apply catalog configurations
    configuredBuilder = applyCatalogConfigurations(configuredBuilder);

    // Apply additional configurations
    configuredBuilder = applyAdditionalConfigurations(configuredBuilder);

    return configuredBuilder;
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
      applySingleCatalogConfig(builder, catalog);
    }
    return builder;
  }

  private SparkSession.Builder applySingleCatalogConfig(
      SparkSession.Builder builder, CatalogConfig catalog) {
    // Basic catalog configuration
    builder
        .config(String.format("spark.sql.catalog.%s", catalog.catalogName), catalog.catalogType)
        .config(String.format("spark.sql.catalog.%s.type", catalog.catalogName), "rest")
        .config(
            String.format("spark.sql.catalog.%s.warehouse", catalog.catalogName),
            catalog.catalogName)
        .config(
            String.format("spark.sql.catalog.%s.scope", catalog.catalogName), "PRINCIPAL_ROLE:ALL");

    // Add endpoint configuration
    Preconditions.checkNotNull(catalog.endpoints, "endpoints is required");
    builder
        .config(
            String.format("spark.sql.catalog.%s.uri", catalog.catalogName),
            catalog.endpoints.catalogApiEndpoint().toString())
        .config(
            String.format("spark.sql.catalog.%s.header.realm", catalog.catalogName),
            catalog.endpoints.realmId());

    // Add token configuration
    if (catalog.token != null) {
      builder.config(
          String.format("spark.sql.catalog.%s.token", catalog.catalogName), catalog.token);
    }

    // Add catalog-specific configurations
    for (ConfigPair config : catalog.catalogSpecificConfigs) {
      builder.config(config.key, config.value);
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
