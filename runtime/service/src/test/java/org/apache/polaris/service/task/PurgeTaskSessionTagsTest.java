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
package org.apache.polaris.service.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.common.annotation.Identifier;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisPrincipalAttributes;
import org.apache.polaris.core.collection.ImmutableAttributeMap;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolverFactory;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.core.storage.cache.StorageCredentialCacheKey;
import org.apache.polaris.service.Profiles;
import org.apache.polaris.service.admin.PolarisAdminService;
import org.apache.polaris.service.admin.PolarisAdminServiceTestSupport;
import org.apache.polaris.service.catalog.PolarisPassthroughResolutionView;
import org.apache.polaris.service.catalog.iceberg.LocalIcebergCatalog;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.context.catalog.PolarisPrincipalHolder;
import org.apache.polaris.service.context.catalog.RealmContextHolder;
import org.apache.polaris.service.events.PolarisEventDispatcher;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.storage.aws.StsClientsPool;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;
import software.amazon.awssdk.services.sts.model.Tag;

/**
 * Verifies that the cleanup tasks scheduled by a drop-with-purge vend storage credentials with the
 * same catalog-derived STS session tags and role session name as request-path credential vending.
 *
 * <p>Only the STS client pool and the credential cache are mocked; credential vending, the task
 * executor and the table, manifest and batch cleanup task handlers are the real beans.
 */
@QuarkusTest
@TestProfile(PurgeTaskSessionTagsTest.Profile.class)
class PurgeTaskSessionTagsTest {

  public static class Profile extends Profiles.DefaultProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("polaris.features.\"DROP_WITH_PURGE_ENABLED\"", "true")
          .put(
              "polaris.features.\"SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL\"",
              "[\"catalog\",\"namespace\",\"table\"]")
          .put("polaris.features.\"SESSION_NAME_FIELDS_IN_SUBSCOPED_CREDENTIAL\"", "[\"catalog\"]")
          .build();
    }
  }

  private static final String CATALOG_NAME = "test_catalog";
  private static final String STORAGE_LOCATION = "s3://my-bucket/path/to/data";
  private static final TableIdentifier TABLE = TableIdentifier.of(Namespace.of("ns"), "tbl");
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  @Inject PolarisMetaStoreManager metaStoreManager;
  @Inject CallContext callContext;
  @Inject RealmConfig realmConfig;
  @Inject RealmContextHolder realmContextHolder;
  @Inject ResolutionManifestFactory resolutionManifestFactory;
  @Inject ResolverFactory resolverFactory;
  @Inject ServiceIdentityProvider serviceIdentityProvider;
  @Inject UserSecretsManager userSecretsManager;
  @Inject PolarisAuthorizer authorizer;
  @Inject ReservedProperties reservedProperties;
  @Inject PolarisPrincipalHolder principalHolder;
  @Inject PolarisDiagnostics diagnostics;
  @Inject StorageAccessConfigProvider storageAccessConfigProvider;
  @Inject FileIOFactory fileIOFactory;
  @Inject TaskExecutor taskExecutor;
  @Inject PolarisEventDispatcher eventDispatcher;
  @Inject PolarisEventMetadataFactory eventMetadataFactory;

  @Inject
  @Identifier("task-error-handler")
  TaskErrorHandler taskErrorHandler;

  private record StsCall(String thread, AssumeRoleRequest request) {}

  private final List<StsCall> stsCalls = new CopyOnWriteArrayList<>();
  private LocalIcebergCatalog catalog;

  @BeforeEach
  void setUp() {
    String realmName = "realm_" + System.nanoTime();
    realmContextHolder.set(() -> realmName);
    PrincipalEntity rootEntity =
        metaStoreManager.findRootPrincipal(callContext.getPolarisCallContext()).orElseThrow();
    PolarisPrincipal root =
        PolarisPrincipal.of(
            rootEntity.getName(),
            ImmutableAttributeMap.builder()
                .put(PolarisPrincipalAttributes.PRINCIPAL_ENTITY_ATTRIBUTE_KEY, rootEntity)
                .build(),
            Set.of("service_admin"));
    principalHolder.set(root);

    StsClient stsClient = Mockito.mock(StsClient.class);
    when(stsClient.assumeRole(isA(AssumeRoleRequest.class)))
        .thenAnswer(
            inv -> {
              stsCalls.add(new StsCall(Thread.currentThread().getName(), inv.getArgument(0)));
              return AssumeRoleResponse.builder()
                  .credentials(
                      Credentials.builder()
                          .accessKeyId("ak")
                          .secretAccessKey("sk")
                          .sessionToken("st")
                          .build())
                  .build();
            });
    StsClientsPool stsClientsPool = Mockito.mock(StsClientsPool.class);
    when(stsClientsPool.stsClient(any())).thenReturn(stsClient);
    QuarkusMock.installMockForType(stsClientsPool, StsClientsPool.class);

    // Bypass the credential cache so that every cleanup task calls STS: with correct session
    // tags, the tasks' cache keys are identical and would otherwise share one AssumeRole call.
    // A zero cache duration is not enough, since concurrent loads of one key are coalesced.
    StorageCredentialCache credentialCache = Mockito.mock(StorageCredentialCache.class);
    when(credentialCache.getOrLoad(any()))
        .thenAnswer(inv -> inv.<StorageCredentialCacheKey>getArgument(0).load());
    QuarkusMock.installMockForType(credentialCache, StorageCredentialCache.class);

    AwsStorageConfigInfo storageConfig =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/test-role")
            .setExternalId("externalId")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(STORAGE_LOCATION))
            .build();
    PolarisAdminService adminService =
        PolarisAdminServiceTestSupport.newAdminService(
            callContext,
            resolutionManifestFactory,
            metaStoreManager,
            userSecretsManager,
            serviceIdentityProvider,
            root,
            authorizer,
            reservedProperties);
    adminService.createCatalog(
        new CreateCatalogRequest(
            new CatalogEntity.Builder()
                .setName(CATALOG_NAME)
                .setDefaultBaseLocation(STORAGE_LOCATION)
                .addProperty(CatalogProperties.FILE_IO_IMPL, InMemoryFileIO.class.getName())
                .setStorageConfigurationInfo(realmConfig, storageConfig)
                .build()
                .asCatalog(serviceIdentityProvider)));

    catalog =
        new LocalIcebergCatalog(
            diagnostics,
            resolverFactory,
            metaStoreManager,
            callContext,
            new PolarisPassthroughResolutionView(resolutionManifestFactory, root, CATALOG_NAME),
            root,
            taskExecutor,
            storageAccessConfigProvider,
            fileIOFactory,
            eventDispatcher,
            eventMetadataFactory);
    catalog.initialize(
        CATALOG_NAME, Map.of(CatalogProperties.FILE_IO_IMPL, InMemoryFileIO.class.getName()));
  }

  @Test
  void testPurgeTasksVendCredentialsWithCatalogSessionTags() {
    catalog.createNamespace(TABLE.namespace());
    Table table = catalog.buildTable(TABLE, SCHEMA).create();
    // One data file gives the table a manifest and a manifest list, so the table cleanup task
    // schedules both a manifest file cleanup task and a batch file cleanup task.
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(table.location() + "/data/f1.parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    table.newFastAppend().appendFile(dataFile).commit();
    table.refresh();

    List<String> filesToPurge = new ArrayList<>();
    filesToPurge.add(((BaseTable) table).operations().current().metadataFileLocation());
    filesToPurge.add(table.currentSnapshot().manifestListLocation());
    table.currentSnapshot().allManifests(table.io()).forEach(m -> filesToPurge.add(m.path()));
    InMemoryFileIO fileIO = new InMemoryFileIO();
    assertThat(filesToPurge).allMatch(fileIO::fileExists);

    stsCalls.clear();
    String testThread = Thread.currentThread().getName();
    assertThat(catalog.dropTable(TABLE, true)).isTrue();

    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .until(() -> filesToPurge.stream().noneMatch(fileIO::fileExists));
    taskErrorHandler.assertNoTaskExceptions();

    // The table, manifest file and batch file cleanup tasks each vend credentials on a task
    // executor thread. Checking this keeps the assertions below from passing without any task
    // credentials to inspect.
    assertThat(stsCalls)
        .filteredOn(call -> !call.thread().equals(testThread))
        .hasSizeGreaterThanOrEqualTo(3);
    assertThat(stsCalls)
        .as("(catalog, namespace, table session tags, role session name) of drop + purge calls")
        .extracting(
            call -> tagValue(call.request(), CredentialVendingContext.TAG_KEY_CATALOG),
            call -> tagValue(call.request(), CredentialVendingContext.TAG_KEY_NAMESPACE),
            call -> tagValue(call.request(), CredentialVendingContext.TAG_KEY_TABLE),
            call -> call.request().roleSessionName())
        .containsOnly(tuple(CATALOG_NAME, "ns", "tbl", "p-" + CATALOG_NAME));
  }

  private static String tagValue(AssumeRoleRequest request, String key) {
    return request.tags().stream()
        .filter(tag -> tag.key().equals(key))
        .map(Tag::value)
        .findFirst()
        .orElse(null);
  }
}
