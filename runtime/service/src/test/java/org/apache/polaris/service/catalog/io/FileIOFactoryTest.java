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
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Nonnull;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.catalog.PolarisPassthroughResolutionView;
import org.apache.polaris.service.catalog.iceberg.IcebergCatalog;
import org.apache.polaris.service.task.TaskFileIOSupplier;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

public class FileIOFactoryTest {

  public static final String CATALOG_NAME = "polaris-catalog";
  public static final Namespace NS = Namespace.of("newdb");
  public static final TableIdentifier TABLE = TableIdentifier.of(NS, "table");
  public static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get(), "unique ID 🤪"),
          required(4, "data", Types.StringType.get()));
  public static final String TEST_ACCESS_KEY = "test_access_key";
  public static final String SECRET_ACCESS_KEY = "secret_access_key";
  public static final String SESSION_TOKEN = "session_token";

  private CallContext callContext;
  private RealmContext realmContext;
  private StsClient stsClient;
  private TestServices testServices;

  @BeforeEach
  public void before(TestInfo testInfo) {
    String realmName =
        "realm_%s_%s"
            .formatted(
                testInfo.getTestMethod().map(Method::getName).orElse("test"), System.nanoTime());
    realmContext = () -> realmName;

    // Mock get subscoped creds
    stsClient = Mockito.mock(StsClient.class);
    when(stsClient.assumeRole(isA(AssumeRoleRequest.class)))
        .thenReturn(
            AssumeRoleResponse.builder()
                .credentials(
                    Credentials.builder()
                        .accessKeyId(TEST_ACCESS_KEY)
                        .secretAccessKey(SECRET_ACCESS_KEY)
                        .sessionToken(SESSION_TOKEN)
                        .build())
                .build());

    // Spy FileIOFactory and check if the credentials are passed to the FileIO
    TestServices.FileIOFactorySupplier fileIOFactorySupplier =
        (storageCredentialCache, metaStoreManagerFactory) ->
            Mockito.spy(
                new DefaultFileIOFactory(storageCredentialCache, metaStoreManagerFactory) {
                  @Override
                  FileIO loadFileIOInternal(
                      @Nonnull String ioImplClassName, @Nonnull Map<String, String> properties) {
                    // properties should contain credentials
                    Assertions.assertThat(properties)
                        .containsEntry(S3FileIOProperties.ACCESS_KEY_ID, TEST_ACCESS_KEY)
                        .containsEntry(S3FileIOProperties.SECRET_ACCESS_KEY, SECRET_ACCESS_KEY)
                        .containsEntry(S3FileIOProperties.SESSION_TOKEN, SESSION_TOKEN);
                    return super.loadFileIOInternal(ioImplClassName, properties);
                  }
                });

    testServices =
        TestServices.builder()
            .config(
                Map.of(
                    "ALLOW_SPECIFYING_FILE_IO_IMPL",
                    true,
                    "ALLOW_INSECURE_STORAGE_TYPES",
                    true,
                    "SUPPORTED_CATALOG_STORAGE_TYPES",
                    List.of("FILE", "S3"),
                    "DROP_WITH_PURGE_ENABLED",
                    true))
            .realmContext(realmContext)
            .stsClient(stsClient)
            .fileIOFactorySupplier(fileIOFactorySupplier)
            .build();

    callContext = testServices.newCallContext();
  }

  @AfterEach
  public void after() {}

  @ParameterizedTest
  @ValueSource(strings = {"s3a", "s3"})
  public void testLoadFileIOForTableLike(String scheme) {
    IcebergCatalog catalog = createCatalog(testServices, scheme);
    catalog.createNamespace(NS);
    catalog.createTable(TABLE, SCHEMA);

    // 1. BasePolarisCatalog:doCommit: for writing the table during the creation
    Mockito.verify(testServices.fileIOFactory(), Mockito.times(1))
        .loadFileIO(
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any());
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3a", "s3"})
  public void testLoadFileIOForCleanupTask(String scheme) {
    IcebergCatalog catalog = createCatalog(testServices, scheme);
    catalog.createNamespace(NS);
    catalog.createTable(TABLE, SCHEMA);
    catalog.dropTable(TABLE, true);

    List<PolarisBaseEntity> tasks =
        testServices
            .metaStoreManagerFactory()
            .getOrCreateMetaStoreManager(realmContext)
            .loadTasks(callContext.getPolarisCallContext(), "testExecutor", PageToken.fromLimit(1))
            .getEntities();
    Assertions.assertThat(tasks).hasSize(1);
    TaskEntity taskEntity = TaskEntity.of(tasks.get(0));
    FileIO fileIO =
        new TaskFileIOSupplier(testServices.fileIOFactory()).apply(taskEntity, TABLE, callContext);
    Assertions.assertThat(fileIO).isNotNull().isInstanceOf(ExceptionMappingFileIO.class);
    Assertions.assertThat(((ExceptionMappingFileIO) fileIO).getInnerIo())
        .isInstanceOf(InMemoryFileIO.class);

    // 1. BasePolarisCatalog:doCommit: for writing the table during the creation
    // 2. BasePolarisCatalog:doRefresh: for reading the table during the drop
    // 3. TaskFileIOSupplier:apply: for clean up metadata files and merge files
    Mockito.verify(testServices.fileIOFactory(), Mockito.times(3))
        .loadFileIO(
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any());
  }

  IcebergCatalog createCatalog(TestServices services, String scheme) {
    String storageLocation = scheme + "://my-bucket/path/to/data";
    AwsStorageConfigInfo awsStorageConfigInfo =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(storageLocation))
            .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
            .build();

    // Create Catalog Entity
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(CATALOG_NAME)
            .setProperties(new CatalogProperties(scheme + "://tmp/path/to/data"))
            .setStorageConfigInfo(awsStorageConfigInfo)
            .build();
    services
        .catalogsApi()
        .createCatalog(
            new CreateCatalogRequest(catalog), services.realmContext(), services.securityContext());

    PolarisPassthroughResolutionView passthroughView =
        new PolarisPassthroughResolutionView(
            callContext,
            services.resolutionManifestFactory(),
            services.securityContext(),
            CATALOG_NAME);
    IcebergCatalog polarisCatalog =
        new IcebergCatalog(
            services.storageCredentialCache(),
            services.resolverFactory(),
            services.metaStoreManagerFactory().getOrCreateMetaStoreManager(realmContext),
            callContext,
            passthroughView,
            services.securityContext(),
            services.taskExecutor(),
            services.fileIOFactory(),
            services.polarisEventListener());
    polarisCatalog.initialize(
        CATALOG_NAME,
        ImmutableMap.of(
            org.apache.iceberg.CatalogProperties.FILE_IO_IMPL,
            "org.apache.iceberg.inmemory.InMemoryFileIO"));
    return polarisCatalog;
  }
}
