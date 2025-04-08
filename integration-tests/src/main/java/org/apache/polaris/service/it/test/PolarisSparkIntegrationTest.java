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
package org.apache.polaris.service.it.test;

import static org.apache.polaris.service.it.env.PolarisClient.polarisClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import com.google.common.collect.ImmutableMap;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.rest.requests.ImmutableRegisterTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.it.env.CatalogApi;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.IntegrationTestsHelper;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.service.it.ext.PolarisSparkIntegrationTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

/**
 * @implSpec This test expects the server to be configured with the following features enabled:
 *     <ul>
 *       <li>{@link
 *           org.apache.polaris.core.config.FeatureConfiguration#SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION}:
 *           {@code true}
 *       <li>{@link
 *           org.apache.polaris.core.config.FeatureConfiguration#ALLOW_OVERLAPPING_CATALOG_URLS}:
 *           {@code true}
 *     </ul>
 */
public class PolarisSparkIntegrationTest extends PolarisSparkIntegrationTestBase {

  @Test
  public void testCreateTable() {
    long namespaceCount = onSpark("SHOW NAMESPACES").count();
    assertThat(namespaceCount).isEqualTo(0L);

    onSpark("CREATE NAMESPACE ns1");
    onSpark("USE ns1");
    onSpark("CREATE TABLE tb1 (col1 integer, col2 string)");
    onSpark("INSERT INTO tb1 VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    long recordCount = onSpark("SELECT * FROM tb1").count();
    assertThat(recordCount).isEqualTo(3);
  }

  @Test
  public void testCreateAndUpdateExternalTable() {
    long namespaceCount = onSpark("SHOW NAMESPACES").count();
    assertThat(namespaceCount).isEqualTo(0L);

    onSpark("CREATE NAMESPACE ns1");
    onSpark("USE ns1");
    onSpark("CREATE TABLE tb1 (col1 integer, col2 string)");
    onSpark("INSERT INTO tb1 VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    long recordCount = onSpark("SELECT * FROM tb1").count();
    assertThat(recordCount).isEqualTo(3);

    onSpark("USE " + externalCatalogName);
    List<Row> existingNamespaces = onSpark("SHOW NAMESPACES").collectAsList();
    assertThat(existingNamespaces).isEmpty();

    onSpark("CREATE NAMESPACE externalns1");
    onSpark("USE externalns1");
    List<Row> existingTables = onSpark("SHOW TABLES").collectAsList();
    assertThat(existingTables).isEmpty();

    LoadTableResponse tableResponse = loadTable(catalogName, "ns1", "tb1");
    try (Response registerResponse =
        catalogApi
            .request("v1/{cat}/namespaces/externalns1/register", Map.of("cat", externalCatalogName))
            .post(
                Entity.json(
                    ImmutableRegisterTableRequest.builder()
                        .name("mytb1")
                        .metadataLocation(tableResponse.metadataLocation())
                        .build()))) {
      assertThat(registerResponse).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
    }

    long tableCount = onSpark("SHOW TABLES").count();
    assertThat(tableCount).isEqualTo(1);
    List<Row> tables = onSpark("SHOW TABLES").collectAsList();
    assertThat(tables).hasSize(1).extracting(row -> row.getString(1)).containsExactly("mytb1");
    long rowCount = onSpark("SELECT * FROM mytb1").count();
    assertThat(rowCount).isEqualTo(3);
    assertThatThrownBy(() -> onSpark("INSERT INTO mytb1 VALUES (20, 'new_text')"))
        .isInstanceOf(Exception.class);

    onSpark("INSERT INTO " + catalogName + ".ns1.tb1 VALUES (20, 'new_text')");
    tableResponse = loadTable(catalogName, "ns1", "tb1");
    Map<String, Object> updateNotification =
        ImmutableMap.<String, Object>builder()
            .put("table-name", "mytb1")
            .put("timestamp", "" + Instant.now().toEpochMilli())
            .put("table-uuid", tableResponse.tableMetadata().uuid())
            .put("metadata-location", tableResponse.metadataLocation())
            .put("metadata", tableResponse.tableMetadata())
            .build();
    Map<String, Object> notificationRequest =
        ImmutableMap.<String, Object>builder()
            .put("payload", updateNotification)
            .put("notification-type", "UPDATE")
            .build();
    try (Response notifyResponse =
        catalogApi
            .request(
                "v1/{cat}/namespaces/externalns1/tables/mytb1/notifications",
                Map.of("cat", externalCatalogName))
            .post(Entity.json(notificationRequest))) {
      assertThat(notifyResponse)
          .extracting(Response::getStatus)
          .isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }
    // refresh the table so it queries for the latest metadata.json
    onSpark("REFRESH TABLE mytb1");
    rowCount = onSpark("SELECT * FROM mytb1").count();
    assertThat(rowCount).isEqualTo(4);
  }

  @Test
  public void testCreateView() {
    long namespaceCount = onSpark("SHOW NAMESPACES").count();
    assertThat(namespaceCount).isEqualTo(0L);

    onSpark("CREATE NAMESPACE ns1");
    onSpark("USE ns1");
    onSpark("CREATE TABLE tb1 (col1 integer, col2 string)");
    onSpark("INSERT INTO tb1 VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    onSpark("CREATE VIEW view1 AS SELECT * FROM tb1");
    long recordCount = onSpark("SELECT * FROM view1").count();
    assertThat(recordCount).isEqualTo(3);
  }

  private LoadTableResponse loadTable(String catalog, String namespace, String table) {
    try (Response response =
        catalogApi
            .request(
                "v1/{cat}/namespaces/{ns}/tables/{table}",
                Map.of("cat", catalog, "ns", namespace, "table", table))
            .get()) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      return response.readEntity(LoadTableResponse.class);
    }
  }
}
