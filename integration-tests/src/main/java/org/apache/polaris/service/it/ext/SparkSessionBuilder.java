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
 * <p>This builder creates a SparkSession with sensible test defaults and allows easy configuration
 * of multiple Polaris catalogs. The resulting SparkSession will be configured for local execution
 * with S3Mock support for testing.
 *
 * <p>Example usage:
 *
 * <pre>
 * SparkSession session = SparkSessionBuilder
 *     .buildWithTestDefaults()
 *     .withExtensions("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
 *     .withWarehouse(warehouseUri)
 *     .addCatalog("catalog1", "org.apache.iceberg.spark.SparkCatalog", endpoints, token)
 *     .addCatalog("catalog2", "org.apache.polaris.spark.SparkCatalog", endpoints, token)
 *     .createSession();
 * </pre>
 *
 * <p>The final SparkSession will be configured with:
 *
 * <ul>
 *   <li>Local master execution (local[1])
 *   <li>Disabled Spark UI for clean test output
 *   <li>S3A filesystem with mock credentials (foo/bar)
 *   <li>Multiple Polaris catalogs with REST endpoints
 *   <li>Iceberg extensions for table format support
 *   <li>Custom warehouse directory location
 * </ul>
 *
 * <p>Each catalog will be configured as:
 *
 * <pre>
 * spark.sql.catalog.{catalogName} = {catalogType}
 * spark.sql.catalog.{catalogName}.type = rest
 * spark.sql.catalog.{catalogName}.uri = {polarisEndpoint}
 * spark.sql.catalog.{catalogName}.token = {authToken}
 * spark.sql.catalog.{catalogName}.warehouse = {catalogName}
 * spark.sql.catalog.{catalogName}.scope = PRINCIPAL_ROLE:ALL
 * </pre>
 */
public class SparkSessionBuilder {
  private final SparkSession.Builder builder;
  private final List<CatalogConfig> catalogs = new ArrayList<>();
  private final List<ConfigPair> additionalConfigs = new ArrayList<>();

  private String extensions = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions";
  private URI warehouseDir;

  private SparkSessionBuilder(SparkSession.Builder builder) {
    this.builder = builder;
  }

  /**
   * Create a SparkSessionBuilder with common test defaults
   *
   * @return new builder instance with test defaults
   */
  public static SparkSessionBuilder buildWithTestDefaults() {
    // local master
    var builder = SparkSession.builder();
    builder.master(String.format("local[%d]", 1));
    // disable UI
    builder.config("spark.ui.showConsoleProgress", "false");
    builder.config("spark.ui.enabled", "false");

    var sparkSessionbuilder = new SparkSessionBuilder(builder);
    sparkSessionbuilder.withS3MockContainer();
    return sparkSessionbuilder;
  }

  private void withS3MockContainer() {
    withConfig("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
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
      String catalogName, String catalogImplClass, PolarisApiEndpoints endpoints, String token) {
    this.catalogs.add(new CatalogConfig(catalogName, catalogImplClass, endpoints, token));
    return this;
  }

  public SparkSessionBuilder withConfig(String key, String value) {
    this.additionalConfigs.add(new ConfigPair(key, value));
    return this;
  }

  public SparkSession getOrCreate() {
    if (extensions != null) {
      builder.config("spark.sql.extensions", extensions);
    }

    if (warehouseDir != null) {
      builder.config("spark.sql.warehouse.dir", warehouseDir.toString());
    }

    // Apply catalog configurations
    applyCatalogConfigurations();

    // Apply additional configurations
    applyAdditionalConfigurations();

    return builder.getOrCreate();
  }

  private void applyCatalogConfigurations() {
    for (CatalogConfig catalog : catalogs) {
      applySingleCatalogConfig(catalog);
    }
  }

  private void applySingleCatalogConfig(CatalogConfig catalog) {
    // Basic catalog configuration
    builder
        .config(
            String.format("spark.sql.catalog.%s", catalog.catalogName), catalog.catalogImplClass)
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
  }

  private void applyAdditionalConfigurations() {
    for (ConfigPair config : additionalConfigs) {
      builder.config(config.key, config.value);
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
    final String catalogImplClass;
    final PolarisApiEndpoints endpoints;
    final String token;

    CatalogConfig(
        String catalogName, String catalogImplClass, PolarisApiEndpoints endpoints, String token) {
      this.catalogName = catalogName;
      this.catalogImplClass = catalogImplClass;
      this.endpoints = endpoints;
      this.token = token;
    }
  }
}
