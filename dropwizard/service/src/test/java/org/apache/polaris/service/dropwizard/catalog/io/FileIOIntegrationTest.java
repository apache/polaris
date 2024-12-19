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
package org.apache.polaris.service.dropwizard.catalog.io;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.azure.core.exception.AzureException;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.Iterators;
import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.dropwizard.catalog.TestUtil;
import org.apache.polaris.service.dropwizard.test.PolarisIntegrationTestFixture;
import org.apache.polaris.service.dropwizard.test.PolarisIntegrationTestHelper;
import org.apache.polaris.service.dropwizard.test.TestEnvironment;
import org.apache.polaris.service.dropwizard.test.TestEnvironmentExtension;
import org.apache.polaris.service.exception.IcebergExceptionMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.services.s3.model.S3Exception;

/** Collection of File IO integration tests */
@QuarkusTest
@TestInstance(Lifecycle.PER_CLASS)
@ExtendWith(TestEnvironmentExtension.class)
public class FileIOIntegrationTest {

  private static final String catalogBaseLocation = "file:/tmp/buckets/my-bucket/path/to/data";

  @Inject PolarisIntegrationTestHelper helper;

  private PolarisIntegrationTestFixture fixture;
  private TestFileIOFactory ioFactory;
  private RESTCatalog restCatalog;
  private Table table;

  @BeforeAll
  public void createFixture(TestEnvironment testEnv, TestInfo testInfo) {
    fixture = helper.createFixture(testEnv, testInfo);
    ioFactory = new TestFileIOFactory();
    QuarkusMock.installMockForType(ioFactory, FileIOFactory.class);

    FileStorageConfigInfo storageConfigInfo =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of(catalogBaseLocation))
            .build();
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("mycatalog")
            .setProperties(
                org.apache.polaris.core.admin.model.CatalogProperties.builder(catalogBaseLocation)
                    .build())
            .setStorageConfigInfo(storageConfigInfo)
            .build();

    restCatalog = TestUtil.createSnowmanManagedCatalog(testEnv, fixture, catalog, Map.of());

    Namespace namespace = Namespace.of("myns");
    restCatalog.createNamespace(namespace);

    String tableName = "mytable";
    table =
        restCatalog
            .buildTable(
                TableIdentifier.of(namespace, tableName),
                new Schema(required(3, "id", Types.IntegerType.get(), "mydoc")))
            .create();
  }

  @AfterAll
  public void destroyFixture() {
    fixture.destroy();
  }

  @Test
  void testGetLengthExceptionSupplier() {
    InMemoryFileIO inMemoryFileIO = new InMemoryFileIO();

    String path = "x/y/z";
    inMemoryFileIO.addFile(path, new byte[0]);

    String errorMsg = "getLength not available";
    FileIO io =
        new TestFileIO(
            inMemoryFileIO,
            Optional.empty(),
            Optional.empty(),
            Optional.of(() -> new RuntimeException(errorMsg)));

    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> io.newInputFile(path).getLength());
    assertTrue(
        exception.getMessage().contains(errorMsg),
        String.format("expected '%s' to contain '%s'", exception.getMessage(), errorMsg));
  }

  private void testIOExceptionExceptionTypes(int uniqueId, IOExceptionTypeTestConfig<?> config) {
    ioFactory.loadFileIOExceptionSupplier = config.loadFileIOExceptionSupplier;
    ioFactory.newInputFileExceptionSupplier = config.newInputFileExceptionSupplier;
    ioFactory.newOutputFileExceptionSupplier = config.newOutputFileExceptionSupplier;

    assertThrows(config.expectedException, () -> config.workload.run(uniqueId));
  }

  @TestFactory
  Stream<DynamicTest> testIOExceptionExceptionTypes() {
    Iterator<String> accessDeniedHint =
        Iterators.cycle(IcebergExceptionMapper.getAccessDeniedHints());
    AtomicInteger uniqueId = new AtomicInteger(0);
    return Stream.of(
            IOExceptionTypeTestConfig.allVariants(
                ForbiddenException.class,
                () ->
                    S3Exception.builder().statusCode(403).message(accessDeniedHint.next()).build(),
                this::workloadCreateTable),
            IOExceptionTypeTestConfig.allVariants(
                ForbiddenException.class,
                () -> new AzureException(accessDeniedHint.next()),
                this::workloadCreateTable),
            IOExceptionTypeTestConfig.allVariants(
                ForbiddenException.class,
                () -> new StorageException(403, accessDeniedHint.next()),
                this::workloadCreateTable),
            IOExceptionTypeTestConfig.allVariants(
                ForbiddenException.class,
                () ->
                    S3Exception.builder().statusCode(403).message(accessDeniedHint.next()).build(),
                this::workloadUpdateTableProperties),
            IOExceptionTypeTestConfig.allVariants(
                ForbiddenException.class,
                () -> new AzureException(accessDeniedHint.next()),
                this::workloadUpdateTableProperties),
            IOExceptionTypeTestConfig.allVariants(
                ForbiddenException.class,
                () -> new StorageException(403, accessDeniedHint.next()),
                this::workloadUpdateTableProperties))
        .flatMap(Collection::stream)
        .map(
            config -> {
              int id = uniqueId.getAndIncrement();
              return DynamicTest.dynamicTest(
                  "testIOExceptionExceptionTypes[%d]".formatted(id),
                  () -> testIOExceptionExceptionTypes(id, config));
            });
  }

  private void workloadUpdateTableProperties(int uniqueId) {
    table.updateProperties().set("foo", "bar" + uniqueId).commit();
  }

  private void workloadCreateTable(int uniqueId) {
    Namespace namespace = Namespace.of("myns" + uniqueId);
    restCatalog.createNamespace(namespace);
    restCatalog
        .buildTable(
            TableIdentifier.of(namespace, "mytable"),
            new Schema(required(3, "id", Types.IntegerType.get(), "mydoc")))
        .create();
  }

  /**
   * Test configuration for asserting that the given workload throws the expectedException when a
   * particular IO operation fails with the given exception specified by the Suppliers.
   */
  record IOExceptionTypeTestConfig<T extends Throwable>(
      Class<T> expectedException,
      Optional<Supplier<RuntimeException>> loadFileIOExceptionSupplier,
      Optional<Supplier<RuntimeException>> newInputFileExceptionSupplier,
      Optional<Supplier<RuntimeException>> newOutputFileExceptionSupplier,
      Workload workload) {

    interface Workload {
      void run(int uniqueId);
    }

    /**
     * Returns a collection of test configs where the given exception Supplier happens at each
     * possible step of the IO
     */
    static <T extends Throwable> Collection<IOExceptionTypeTestConfig<T>> allVariants(
        Class<T> exceptionType, Supplier<RuntimeException> exceptionSupplier, Workload workload) {
      return List.of(
          new IOExceptionTypeTestConfig<>(
              exceptionType,
              Optional.of(exceptionSupplier),
              Optional.empty(),
              Optional.empty(),
              workload),
          new IOExceptionTypeTestConfig<>(
              exceptionType,
              Optional.empty(),
              Optional.of(exceptionSupplier),
              Optional.empty(),
              workload),
          new IOExceptionTypeTestConfig<>(
              exceptionType,
              Optional.empty(),
              Optional.empty(),
              Optional.of(exceptionSupplier),
              workload));
    }
  }
}
