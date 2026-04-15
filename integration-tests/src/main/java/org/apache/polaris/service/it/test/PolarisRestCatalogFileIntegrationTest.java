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

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.it.env.IntegrationTestsHelper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Runs PolarisRestCatalogIntegrationBase test on the local filesystem. */
public class PolarisRestCatalogFileIntegrationTest extends PolarisRestCatalogIntegrationBase {

  public static String baseLocation;

  @BeforeAll
  static void setup(@TempDir Path tempDir) {
    URI tempDirURI = IntegrationTestsHelper.getTemporaryDirectory(tempDir);
    baseLocation = stripTrailingSlash(tempDirURI.toString());
  }

  @Override
  protected StorageConfigInfo getStorageConfigInfo() {
    return FileStorageConfigInfo.builder()
        .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
        .setAllowedLocations(List.of(baseLocation, "file://"))
        .build();
  }

  private static String stripTrailingSlash(String uri) {
    return uri.endsWith("/") && uri.length() > "file:///".length()
        ? uri.substring(0, uri.length() - 1)
        : uri;
  }

  @Disabled("File storage does not support credential vending")
  @Test
  @Override
  public void testLoadCredentialsEndpoint() {}

  /**
   * This test is declared here because namespace and table names with special characters are
   * currently not possible in Polaris with other storage integrations.
   */
  @Test
  public void testNamespaceAndTableEncodeDecode() {

    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();

    Namespace ns1 = Namespace.of("ns+1/%20");
    catalog.createNamespace(ns1);

    assertThat(catalog.listNamespaces()).contains(ns1);
    assertThat(catalog.loadNamespaceMetadata(ns1)).isNotNull();

    Namespace ns2 = Namespace.of("ns+1/%20", "ns+2/%2B");
    catalog.createNamespace(ns2);

    assertThat(catalog.listNamespaces(ns1)).contains(ns2);
    assertThat(catalog.loadNamespaceMetadata(ns2)).isNotNull();

    TableIdentifier t1 = TableIdentifier.of(ns2, "tbl+1/%20%2B");
    catalog.buildTable(t1, SCHEMA).create();

    assertThat(catalog.listTables(ns2)).contains(t1);
    Table table = catalog.loadTable(t1);
    assertThat(table).isNotNull().isInstanceOf(BaseTable.class);
  }
}
