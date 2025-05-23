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
package org.apache.polaris.service.quarkus.catalog.io;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.azure.core.exception.AzureException;
import com.google.cloud.storage.StorageException;
import jakarta.ws.rs.core.Response;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.catalog.io.MeasuredFileIOFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.services.s3.model.S3Exception;

/** Validates the propagation of FileIO-level exceptions to the REST API layer. */
public class FileIOExceptionsTest {
  private static final Schema SCHEMA =
      new Schema(required(3, "id", Types.IntegerType.get(), "unique ID"));

  private static final String catalog = "test-catalog";

  private static TestServices services;
  private static MeasuredFileIOFactory ioFactory;

  @BeforeAll
  public static void beforeAll(@TempDir Path tempDir) {
    services =
        TestServices.builder()
            .config(
                Map.of(
                    "ALLOW_INSECURE_STORAGE_TYPES",
                    true,
                    "SUPPORTED_CATALOG_STORAGE_TYPES",
                    List.of("FILE", "S3")))
            .build();
    ioFactory = (MeasuredFileIOFactory) services.fileIOFactory();

    String catalogBaseLocation = tempDir.toAbsolutePath().toUri().toString();
    if (catalogBaseLocation.endsWith("/")) {
      catalogBaseLocation = catalogBaseLocation.substring(0, catalogBaseLocation.length() - 1);
    }

    FileStorageConfigInfo storageConfigInfo =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of(catalogBaseLocation))
            .build();
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("test-catalog")
            .setProperties(
                org.apache.polaris.core.admin.model.CatalogProperties.builder(catalogBaseLocation)
                    .build())
            .setStorageConfigInfo(storageConfigInfo)
            .build();

    try (Response res =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalog),
                services.realmContext(),
                services.securityContext())) {
      assertThat(res.getStatus()).isEqualTo(201);
    }

    try (Response res =
        services
            .restApi()
            .createNamespace(
                FileIOExceptionsTest.catalog,
                CreateNamespaceRequest.builder().withNamespace(Namespace.of("ns1")).build(),
                services.realmContext(),
                services.securityContext())) {
      assertThat(res.getStatus()).isEqualTo(200);
    }
  }

  @BeforeEach
  void reset() {
    ioFactory.loadFileIOExceptionSupplier = Optional.empty();
    ioFactory.newInputFileExceptionSupplier = Optional.empty();
    ioFactory.newOutputFileExceptionSupplier = Optional.empty();
  }

  private static void requestCreateTable() {
    CreateTableRequest request =
        CreateTableRequest.builder().withName("t1").withSchema(SCHEMA).build();
    Response res =
        services
            .restApi()
            .createTable(
                catalog, "ns1", request, null, services.realmContext(), services.securityContext());
    res.close();
  }

  private static void requestDropTable() {
    Response res =
        services
            .restApi()
            .dropTable(
                catalog, "ns1", "t1", false, services.realmContext(), services.securityContext());
    res.close();
  }

  private static void requestLoadTable() {
    Response res =
        services
            .restApi()
            .loadTable(
                catalog,
                "ns1",
                "t1",
                null,
                null,
                "ALL",
                services.realmContext(),
                services.securityContext());
    res.close();
  }

  static Stream<RuntimeException> exceptions() {
    return Stream.of(
        new AzureException("Forbidden"),
        S3Exception.builder().statusCode(403).message("Forbidden").build(),
        new StorageException(403, "Forbidden"));
  }

  @ParameterizedTest
  @MethodSource("exceptions")
  void testLoadFileIOExceptionPropagation(RuntimeException ex) {
    ioFactory.loadFileIOExceptionSupplier = Optional.of(() -> ex);
    assertThatThrownBy(FileIOExceptionsTest::requestCreateTable).isSameAs(ex);
  }

  @ParameterizedTest
  @MethodSource("exceptions")
  void testNewInputFileExceptionPropagation(RuntimeException ex) {
    ioFactory.newInputFileExceptionSupplier = Optional.of(() -> ex);
    assertThatCode(FileIOExceptionsTest::requestCreateTable).doesNotThrowAnyException();
    assertThatThrownBy(FileIOExceptionsTest::requestLoadTable).isSameAs(ex);
    assertThatCode(FileIOExceptionsTest::requestDropTable).doesNotThrowAnyException();
  }

  @ParameterizedTest
  @MethodSource("exceptions")
  void testNewOutputFileExceptionPropagation(RuntimeException ex) {
    ioFactory.newOutputFileExceptionSupplier = Optional.of(() -> ex);
    assertThatThrownBy(FileIOExceptionsTest::requestCreateTable).isSameAs(ex);
  }
}
