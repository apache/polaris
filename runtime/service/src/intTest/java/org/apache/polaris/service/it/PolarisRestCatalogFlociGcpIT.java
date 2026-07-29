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
package org.apache.polaris.service.it;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.iceberg.CatalogProperties.TABLE_DEFAULT_PREFIX;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.polaris.service.it.env.PolarisClient.polarisClient;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.GcpStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.IcebergHelper;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.test.floci.gcp.FlociGcp;
import org.apache.polaris.test.floci.gcp.FlociGcpAccess;
import org.apache.polaris.test.floci.gcp.FlociGcpTestResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@QuarkusIntegrationTest
@QuarkusTestResource(
    value = FlociGcpTestResource.class,
    initArgs = {
      @ResourceArg(name = "bucket", value = "floci-gcp-test-polaris"),
      @ResourceArg(name = "projectId", value = "polaris-floci-gcp"),
      @ResourceArg(name = "oauth2Token", value = "polaris-floci-gcp-token")
    })
@ExtendWith(PolarisIntegrationTestExtension.class)
public class PolarisRestCatalogFlociGcpIT {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", org.apache.iceberg.types.Types.IntegerType.get(), "doc"),
          optional(2, "data", org.apache.iceberg.types.Types.StringType.get()));

  @FlociGcp static FlociGcpAccess flociGcpAccess;

  private static PolarisClient client;
  private static ManagementApi managementApi;
  private static PolarisApiEndpoints endpoints;
  private static String adminToken;

  private PrincipalWithCredentials principalCredentials;
  private String catalogName;

  @BeforeAll
  static void setup(PolarisApiEndpoints apiEndpoints, ClientCredentials adminCredentials) {
    endpoints = apiEndpoints;
    client = polarisClient(endpoints);
    adminToken = client.obtainToken(adminCredentials);
    managementApi = client.managementApi(adminToken);
  }

  @AfterAll
  static void close() throws Exception {
    client.close();
  }

  @BeforeEach
  void before(TestInfo testInfo) {
    var principalName = client.newEntityName("floci-gcp-user");
    var principalRoleName = client.newEntityName("floci-gcp-admin");
    principalCredentials = managementApi.createPrincipalWithRole(principalName, principalRoleName);
    catalogName = client.newEntityName(testInfo.getTestMethod().orElseThrow().getName());

    managementApi.createCatalog(principalRoleName, catalog());
  }

  @AfterEach
  void cleanUp() {
    client.cleanUp(adminToken);
  }

  @Test
  void createsTableAndWritesData() throws IOException {
    try (var restCatalog = restCatalog()) {
      var namespace = Namespace.of("test_ns");
      restCatalog.createNamespace(namespace);

      var id = TableIdentifier.of(namespace, "t1");
      var table = restCatalog.createTable(id, SCHEMA);
      assertThat(restCatalog.tableExists(id)).isTrue();

      @SuppressWarnings("resource")
      var io = table.io();
      var location = URI.create(table.locationProvider().newDataLocation("test-file.txt"));
      var outputFile = io.newOutputFile(location.toString());
      var fileContent = "Hello Floci GCP".getBytes(UTF_8);
      try (var outputStream = outputFile.create()) {
        outputStream.write(fileContent);
      }

      try (var inputStream = io.newInputFile(outputFile.location()).newStream()) {
        assertThat(inputStream.readAllBytes()).isEqualTo(fileContent);
      }

      var dataFile =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withPath(outputFile.location())
              .withFormat(FileFormat.PARQUET)
              .withFileSizeInBytes(fileContent.length)
              .withRecordCount(1)
              .build();
      table.newAppend().appendFile(dataFile).commit();

      assertThat(restCatalog.loadTable(id).currentSnapshot()).isNotNull();
    }
  }

  private Catalog catalog() {
    var baseLocation = flociGcpAccess.bucketUri("polaris/" + catalogName).toString();
    var catalogProps = CatalogProperties.builder(baseLocation);
    catalogProps.addProperty(FeatureConfiguration.DROP_WITH_PURGE_ENABLED.catalogConfig(), "true");
    flociGcpAccess
        .icebergProperties()
        .forEach((key, value) -> catalogProps.addProperty(TABLE_DEFAULT_PREFIX + key, value));

    return PolarisCatalog.builder()
        .setType(Catalog.TypeEnum.INTERNAL)
        .setName(catalogName)
        .setStorageConfigInfo(
            GcpStorageConfigInfo.builder()
                .setGcsServiceAccount(flociGcpAccess.projectId())
                .setStorageType(StorageConfigInfo.StorageTypeEnum.GCS)
                .setAllowedLocations(List.of(baseLocation))
                .build())
        .setProperties(catalogProps.build())
        .build();
  }

  private RESTCatalog restCatalog() {
    var authToken = client.obtainToken(principalCredentials);
    return IcebergHelper.restCatalog(endpoints, catalogName, restCatalogProperties(), authToken);
  }

  private Map<String, String> restCatalogProperties() {
    return ImmutableMap.<String, String>builder()
        .putAll(flociGcpAccess.icebergProperties())
        .putAll(
            flociGcpAccess.icebergProperties().entrySet().stream()
                .collect(
                    ImmutableMap.toImmutableMap(
                        entry -> TABLE_DEFAULT_PREFIX + entry.getKey(), Map.Entry::getValue)))
        .buildKeepingLast();
  }
}
