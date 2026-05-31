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
package org.apache.polaris.spark.quarkus.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@QuarkusIntegrationTest
@ExtendWith(PolarisIntegrationTestExtension.class)
public class BundleJarSanityIT {

  /**
   * This test verifies that the Polaris Spark bundle jar can be loaded in a fresh JVM with only
   * Spark dependencies, and be able to interact with the Polaris server.
   */
  @Test
  void testBundleJarLoading(
      @TempDir Path tempDir, PolarisApiEndpoints endpoints, ClientCredentials credentials)
      throws Exception {
    String bundleJarPath = System.getProperty("polaris.spark.bundle.jar");
    assertThat(bundleJarPath)
        .withFailMessage("polaris.spark.bundle.jar property not set")
        .isNotNull();

    File bundleJar = new File(bundleJarPath);
    assertThat(bundleJar).exists();

    try (PolarisManagementClient client = PolarisManagementClient.managementClient(endpoints)) {
      String catalogName = client.newEntityName("bundle_test_catalog");
      ManagementApi managementApi = client.managementApi(credentials);

      String catalogBaseLocation = tempDir.resolve("catalog").toUri().toString();
      FileStorageConfigInfo storageConfig =
          FileStorageConfigInfo.builder()
              .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
              .setAllowedLocations(List.of(catalogBaseLocation))
              .build();
      Catalog catalog =
          PolarisCatalog.builder()
              .setType(Catalog.TypeEnum.INTERNAL)
              .setName(catalogName)
              .setProperties(new CatalogProperties(catalogBaseLocation))
              .setStorageConfigInfo(storageConfig)
              .build();

      managementApi.createCatalog(catalog);
      try {
        runIsolatedSparkCheck(
            bundleJar, tempDir, catalogName, endpoints, client.obtainToken(credentials));
      } finally {
        managementApi.deleteCatalog(catalogName);
      }
    }
  }

  private void runIsolatedSparkCheck(
      File bundleJar, Path tempDir, String catalogName, PolarisApiEndpoints endpoints, String token)
      throws Exception {
    // Filter the current classpath: drop polaris-spark / polaris-core so the bundle jar
    // is the sole source of those classes; keep external jars (spark-sql, iceberg, etc.).
    String[] parts = System.getProperty("java.class.path").split(File.pathSeparator);
    List<String> isolatedClasspathParts = new ArrayList<>();
    isolatedClasspathParts.add(bundleJar.getAbsolutePath());
    for (String part : parts) {
      if (part.endsWith(".jar")
          && !part.contains("polaris-spark")
          && !part.contains("polaris-core")) {
        isolatedClasspathParts.add(part);
      }
    }
    String isolatedClasspath = String.join(File.pathSeparator, isolatedClasspathParts);
    Path warehouseDir = tempDir.resolve("warehouse");

    // Spawn a new JVM to run the Spark check
    String testClassesDir =
        BundleJarSanityIT.class.getProtectionDomain().getCodeSource().getLocation().getPath();
    ProcessBuilder pb = new ProcessBuilder();
    pb.command(
        "java",
        "--add-exports",
        "java.base/sun.nio.ch=ALL-UNNAMED",
        "-Dspark.master=local",
        "-Dspark.sql.catalog.polaris=org.apache.polaris.spark.SparkCatalog",
        "-Dspark.sql.catalog.polaris.type=rest",
        "-Dspark.sql.catalog.polaris.uri=" + endpoints.catalogApiEndpoint(),
        "-Dspark.sql.catalog.polaris.warehouse=" + catalogName,
        "-Dspark.sql.catalog.polaris.token=" + token,
        "-Dspark.sql.warehouse.dir=" + warehouseDir.toAbsolutePath(),
        "-cp",
        isolatedClasspath + File.pathSeparator + testClassesDir,
        "org.apache.polaris.spark.quarkus.it.BundleSanityChecker");

    pb.redirectErrorStream(true);
    Process process = pb.start();

    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println("[Isolated Spark] " + line);
      }
    }

    int exitCode = process.waitFor();
    assertThat(exitCode).withFailMessage("Isolated Spark process failed").isEqualTo(0);
  }
}
