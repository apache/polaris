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
package org.apache.polaris.service.catalog.io;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.azure.core.exception.AzureException;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.Iterators;
import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
import org.apache.polaris.service.PolarisApplication;
import org.apache.polaris.service.catalog.TestUtil;
import org.apache.polaris.service.config.PolarisApplicationConfig;
import org.apache.polaris.service.exception.IcebergExceptionMapper;
import org.apache.polaris.service.test.PolarisConnectionExtension;
import org.apache.polaris.service.test.PolarisRealm;
import org.apache.polaris.service.test.SnowmanCredentialsExtension;
import org.apache.polaris.service.test.TestEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.services.s3.model.S3Exception;

/** Collection of File IO integration tests */
@ExtendWith({
  DropwizardExtensionsSupport.class,
  TestEnvironmentExtension.class,
  PolarisConnectionExtension.class,
  SnowmanCredentialsExtension.class
})
public class FileIOIntegrationTest {
  private static final DropwizardAppExtension<PolarisApplicationConfig> EXT =
      new DropwizardAppExtension<>(
          PolarisApplication.class,
          ResourceHelpers.resourceFilePath("polaris-server-integrationtest.yml"),
          ConfigOverride.config(
              "server.applicationConnectors[0].port",
              "0"), // Bind to random port to support parallelism
          ConfigOverride.config("server.adminConnectors[0].port", "0"),
          ConfigOverride.config("io.factoryType", "test"));

  private static final String catalogBaseLocation = "file:/tmp/buckets/my-bucket/path/to/data";
  private static TestFileIOFactory ioFactory;
  private static RESTCatalog restCatalog;
  private static Table table;

  @BeforeAll
  public static void beforeAll(
      PolarisConnectionExtension.PolarisToken adminToken,
      SnowmanCredentialsExtension.SnowmanCredentials snowmanCredentials,
      @PolarisRealm String realm) {
    ioFactory = (TestFileIOFactory) EXT.getConfiguration().getFileIOFactory();

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

    restCatalog =
        TestUtil.createSnowmanManagedCatalog(
            EXT, adminToken, snowmanCredentials, realm, catalog, Map.of());

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

  @ParameterizedTest
  @MethodSource("getIOExceptionTypeTestConfigs")
  void testIOExceptionExceptionTypes(int uniqueId, IOExceptionTypeTestConfig<?> config) {
    ioFactory.loadFileIOExceptionSupplier = config.loadFileIOExceptionSupplier;
    ioFactory.newInputFileExceptionSupplier = config.newInputFileExceptionSupplier;
    ioFactory.newOutputFileExceptionSupplier = config.newOutputFileExceptionSupplier;

    assertThrows(config.expectedException, () -> config.workload.run(uniqueId));
  }

  private static Stream<Arguments> getIOExceptionTypeTestConfigs() {
    Iterator<String> accessDeniedHint =
        Iterators.cycle(IcebergExceptionMapper.getAccessDeniedHints());
    List<IOExceptionTypeTestConfig<?>> configs =
        Stream.of(
                IOExceptionTypeTestConfig.allVariants(
                    ForbiddenException.class,
                    () ->
                        S3Exception.builder()
                            .statusCode(403)
                            .message(accessDeniedHint.next())
                            .build(),
                    FileIOIntegrationTest::workloadCreateTable),
                IOExceptionTypeTestConfig.allVariants(
                    ForbiddenException.class,
                    () -> new AzureException(accessDeniedHint.next()),
                    FileIOIntegrationTest::workloadCreateTable),
                IOExceptionTypeTestConfig.allVariants(
                    ForbiddenException.class,
                    () -> new StorageException(403, accessDeniedHint.next()),
                    FileIOIntegrationTest::workloadCreateTable),
                IOExceptionTypeTestConfig.allVariants(
                    ForbiddenException.class,
                    () ->
                        S3Exception.builder()
                            .statusCode(403)
                            .message(accessDeniedHint.next())
                            .build(),
                    FileIOIntegrationTest::workloadUpdateTableProperties),
                IOExceptionTypeTestConfig.allVariants(
                    ForbiddenException.class,
                    () -> new AzureException(accessDeniedHint.next()),
                    FileIOIntegrationTest::workloadUpdateTableProperties),
                IOExceptionTypeTestConfig.allVariants(
                    ForbiddenException.class,
                    () -> new StorageException(403, accessDeniedHint.next()),
                    FileIOIntegrationTest::workloadUpdateTableProperties))
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

    return IntStream.range(0, configs.size()).mapToObj(i -> Arguments.of(i, configs.get(i)));
  }

  private static void workloadUpdateTableProperties(int uniqueId) {
    table.updateProperties().set("foo", "bar" + uniqueId).commit();
  }

  private static void workloadCreateTable(int uniqueId) {
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
