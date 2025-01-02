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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.azure.core.exception.AzureException;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.StorageException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.security.Principal;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.service.admin.PolarisServiceImpl;
import org.apache.polaris.service.admin.api.PolarisCatalogsApi;
import org.apache.polaris.service.catalog.IcebergCatalogAdapter;
import org.apache.polaris.service.catalog.api.IcebergRestCatalogApi;
import org.apache.polaris.service.catalog.api.IcebergRestCatalogApiService;
import org.apache.polaris.service.config.RealmEntityManagerFactory;
import org.apache.polaris.service.context.CallContextCatalogFactory;
import org.apache.polaris.service.context.PolarisCallContextCatalogFactory;
import org.apache.polaris.service.persistence.InMemoryPolarisMetaStoreManagerFactory;
import org.apache.polaris.service.storage.PolarisStorageIntegrationProviderImpl;
import org.apache.polaris.service.task.TaskExecutor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import software.amazon.awssdk.services.s3.model.S3Exception;

/** Validates the propagation of FileIO-level exceptions to the REST API layer. */
public class FileIOExceptionsTest {
  private static final Schema SCHEMA =
      new Schema(required(3, "id", Types.IntegerType.get(), "unique ID"));

  private static final String catalog = "test-catalog";
  private static final String catalogBaseLocation = "file:/tmp/buckets/my-bucket/path/to/data";
  private static final RealmContext realmContext = () -> "test-realm";

  private static SecurityContext securityContext;
  private static TestFileIOFactory ioFactory;
  private static IcebergRestCatalogApi api;

  @BeforeAll
  public static void beforeAll() {
    ioFactory = new TestFileIOFactory();

    InMemoryPolarisMetaStoreManagerFactory metaStoreManagerFactory =
        new InMemoryPolarisMetaStoreManagerFactory();
    metaStoreManagerFactory.setStorageIntegrationProvider(
        new PolarisStorageIntegrationProviderImpl(
            Mockito::mock, () -> GoogleCredentials.create(new AccessToken("abc", new Date()))));

    PolarisMetaStoreManager metaStoreManager =
        metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);

    EntityCache cache = new EntityCache(metaStoreManager);
    RealmEntityManagerFactory realmEntityManagerFactory =
        new RealmEntityManagerFactory(metaStoreManagerFactory, () -> cache) {};
    CallContextCatalogFactory callContextFactory =
        new PolarisCallContextCatalogFactory(
            realmEntityManagerFactory,
            metaStoreManagerFactory,
            Mockito.mock(TaskExecutor.class),
            ioFactory);
    PolarisAuthorizer authorizer = Mockito.mock(PolarisAuthorizer.class);
    IcebergRestCatalogApiService service =
        new IcebergCatalogAdapter(
            callContextFactory, realmEntityManagerFactory, metaStoreManagerFactory, authorizer);
    api = new IcebergRestCatalogApi(service);

    PolarisMetaStoreSession session =
        metaStoreManagerFactory.getOrCreateSessionSupplier(realmContext).get();
    PolarisCallContext context =
        new PolarisCallContext(session, Mockito.mock(PolarisDiagnostics.class));
    PolarisMetaStoreManager.CreatePrincipalResult createdPrincipal =
        metaStoreManager.createPrincipal(
            context,
            new PrincipalEntity.Builder()
                .setName("test-principal")
                .setCreateTimestamp(Instant.now().toEpochMilli())
                .setCredentialRotationRequiredState()
                .build());

    AuthenticatedPolarisPrincipal principal =
        new AuthenticatedPolarisPrincipal(
            PolarisEntity.of(createdPrincipal.getPrincipal()), Set.of());

    securityContext =
        new SecurityContext() {
          @Override
          public Principal getUserPrincipal() {
            return principal;
          }

          @Override
          public boolean isUserInRole(String s) {
            return false;
          }

          @Override
          public boolean isSecure() {
            return true;
          }

          @Override
          public String getAuthenticationScheme() {
            return "";
          }
        };

    PolarisCatalogsApi catalogsApi =
        new PolarisCatalogsApi(
            new PolarisServiceImpl(realmEntityManagerFactory, metaStoreManagerFactory, authorizer));

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
        catalogsApi.createCatalog(
            new CreateCatalogRequest(catalog), realmContext, securityContext)) {
      assertThat(res.getStatus()).isEqualTo(201);
    }

    try (Response res =
        api.createNamespace(
            FileIOExceptionsTest.catalog,
            CreateNamespaceRequest.builder().withNamespace(Namespace.of("ns1")).build(),
            realmContext,
            securityContext)) {
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
    Response res = api.createTable(catalog, "ns1", request, null, realmContext, securityContext);
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
    assertThatThrownBy(FileIOExceptionsTest::requestCreateTable).isSameAs(ex);
  }

  @ParameterizedTest
  @MethodSource("exceptions")
  void testNewOutputFileExceptionPropagation(RuntimeException ex) {
    ioFactory.newOutputFileExceptionSupplier = Optional.of(() -> ex);
    assertThatThrownBy(FileIOExceptionsTest::requestCreateTable).isSameAs(ex);
  }
}
