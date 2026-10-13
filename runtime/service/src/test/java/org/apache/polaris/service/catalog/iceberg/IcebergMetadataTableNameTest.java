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
package org.apache.polaris.service.catalog.iceberg;

import static org.apache.polaris.service.admin.PolarisAuthzTestBase.SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.ws.rs.core.Response;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A table may be named after an Iceberg metadata table type (such as {@code history}), since a
 * metadata table is only addressed below an existing table.
 */
public class IcebergMetadataTableNameTest {

  private static final String CATALOG = "test-catalog";
  private static final String NAMESPACE = "ns";

  private TestServices services;

  @BeforeEach
  void setUp() {
    services =
        TestServices.builder()
            .fileIOFactorySupplier(
                () ->
                    (FileIOFactory)
                        (accessConfig, ioImplClassName, properties) -> new InMemoryFileIO())
            .build();

    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(CATALOG)
            .setProperties(new CatalogProperties("s3://bucket/base/" + CATALOG))
            .setStorageConfigInfo(
                AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                    .setRoleArn("arn:aws:iam::123456789012:role/catalog_role")
                    .setAllowedLocations(List.of("s3://bucket/base/" + CATALOG + "/"))
                    .build())
            .build();
    try (Response response =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalog),
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }

    try (Response response =
        services
            .restApi()
            .createNamespace(
                CATALOG,
                CreateNamespaceRequest.builder().withNamespace(Namespace.of(NAMESPACE)).build(),
                null,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"history", "snapshots", "entries"})
  void createTableNamedAfterMetadataTable(String name) {
    CreateTableRequest request =
        CreateTableRequest.builder().withName(name).withSchema(SCHEMA).build();

    assertThat(createTable(request)).isEqualTo(Response.Status.OK.getStatusCode());
    assertThatThrownBy(() -> createTable(request)).isInstanceOf(AlreadyExistsException.class);
  }

  @ParameterizedTest
  @ValueSource(strings = {"history", "snapshots", "entries"})
  void stageCreateTableNamedAfterMetadataTable(String name) {
    CreateTableRequest request =
        CreateTableRequest.builder().withName(name).withSchema(SCHEMA).stageCreate().build();

    assertThat(createTable(request)).isEqualTo(Response.Status.OK.getStatusCode());
  }

  private int createTable(CreateTableRequest request) {
    try (Response response =
        services
            .restApi()
            .createTable(
                CATALOG,
                NAMESPACE,
                request,
                null,
                null,
                services.realmContext(),
                services.securityContext())) {
      return response.getStatus();
    }
  }
}
