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
package org.apache.polaris.service.quarkus.catalog;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisConfiguration;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizerImpl;
import org.apache.polaris.core.auth.PolarisSecretsManager;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.GenericTableEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.transactional.TransactionalPersistence;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.aws.AwsCredentialsStorageIntegration;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.service.admin.PolarisAdminService;
import org.apache.polaris.service.catalog.PolarisPassthroughResolutionView;
import org.apache.polaris.service.catalog.generic.PolarisGenericTableCatalog;
import org.apache.polaris.service.catalog.iceberg.PolarisIcebergCatalog;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.storage.PolarisStorageIntegrationProviderImpl;
import org.apache.polaris.service.task.TaskExecutor;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

@QuarkusTest
@TestProfile(PolarisGenericTableCatalogTest.Profile.class)
public class PolarisGenericTableCatalogTest {
  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of();
    }
  }

  protected static final Namespace NS = Namespace.of("newdb");
  protected static final TableIdentifier TABLE = TableIdentifier.of(NS, "table");
  public static final String CATALOG_NAME = "polaris-catalog";
  public static final String TEST_ACCESS_KEY = "test_access_key";
  public static final String SECRET_ACCESS_KEY = "secret_access_key";
  public static final String SESSION_TOKEN = "session_token";

  @Inject MetaStoreManagerFactory managerFactory;
  @Inject PolarisConfigurationStore configurationStore;
  @Inject PolarisStorageIntegrationProvider storageIntegrationProvider;
  @Inject PolarisDiagnostics diagServices;

  private PolarisGenericTableCatalog genericTableCatalog;
  private PolarisIcebergCatalog icebergCatalog;
  private CallContext callContext;
  private AwsStorageConfigInfo storageConfigModel;
  private String realmName;
  private PolarisMetaStoreManager metaStoreManager;
  private PolarisCallContext polarisContext;
  private PolarisAdminService adminService;
  private PolarisEntityManager entityManager;
  private FileIOFactory fileIOFactory;
  private AuthenticatedPolarisPrincipal authenticatedRoot;
  private PolarisEntity catalogEntity;
  private SecurityContext securityContext;

  @BeforeAll
  public static void setUpMocks() {
    PolarisStorageIntegrationProviderImpl mock =
        Mockito.mock(PolarisStorageIntegrationProviderImpl.class);
    QuarkusMock.installMockForType(mock, PolarisStorageIntegrationProviderImpl.class);
  }

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void before(TestInfo testInfo) {
    realmName =
        "realm_%s_%s"
            .formatted(
                testInfo.getTestMethod().map(Method::getName).orElse("test"), System.nanoTime());
    RealmContext realmContext = () -> realmName;
    metaStoreManager = managerFactory.getOrCreateMetaStoreManager(realmContext);
    polarisContext =
        new PolarisCallContext(
            managerFactory.getOrCreateSessionSupplier(realmContext).get(),
            diagServices,
            configurationStore,
            Clock.systemDefaultZone());
    entityManager =
        new PolarisEntityManager(
            metaStoreManager, new StorageCredentialCache(), new EntityCache(metaStoreManager));

    callContext = CallContext.of(realmContext, polarisContext);

    PrincipalEntity rootEntity =
        new PrincipalEntity(
            PolarisEntity.of(
                metaStoreManager
                    .readEntityByName(
                        polarisContext,
                        null,
                        PolarisEntityType.PRINCIPAL,
                        PolarisEntitySubType.NULL_SUBTYPE,
                        "root")
                    .getEntity()));

    authenticatedRoot = new AuthenticatedPolarisPrincipal(rootEntity, Set.of());

    securityContext = Mockito.mock(SecurityContext.class);
    when(securityContext.getUserPrincipal()).thenReturn(authenticatedRoot);
    when(securityContext.isUserInRole(isA(String.class))).thenReturn(true);
    adminService =
        new PolarisAdminService(
            callContext,
            entityManager,
            metaStoreManager,
            securityContext,
            new PolarisAuthorizerImpl(new PolarisConfigurationStore() {}));

    String storageLocation = "s3://my-bucket/path/to/data";
    storageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
            .setExternalId("externalId")
            .setUserArn("aws::a:user:arn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(storageLocation, "s3://externally-owned-bucket"))
            .build();
    catalogEntity =
        adminService.createCatalog(
            new CatalogEntity.Builder()
                .setName(CATALOG_NAME)
                .setDefaultBaseLocation(storageLocation)
                .setReplaceNewLocationPrefixWithCatalogDefault("file:")
                .addProperty(
                    PolarisConfiguration.ALLOW_EXTERNAL_TABLE_LOCATION.catalogConfig(), "true")
                .addProperty(
                    PolarisConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(), "true")
                .setStorageConfigurationInfo(storageConfigModel, storageLocation)
                .build());

    PolarisPassthroughResolutionView passthroughView =
        new PolarisPassthroughResolutionView(
            callContext, entityManager, securityContext, CATALOG_NAME);
    TaskExecutor taskExecutor = Mockito.mock();

    StsClient stsClient = Mockito.mock(StsClient.class);
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
    PolarisStorageIntegration<AwsStorageConfigurationInfo> storageIntegration =
        new AwsCredentialsStorageIntegration(stsClient);
    when(storageIntegrationProvider.getStorageIntegrationForConfig(
            isA(AwsStorageConfigurationInfo.class)))
        .thenReturn((PolarisStorageIntegration) storageIntegration);

    this.genericTableCatalog =
        new PolarisGenericTableCatalog(
            entityManager,
            metaStoreManager,
            callContext,
            passthroughView,
            securityContext,
            taskExecutor,
            fileIOFactory);
    this.icebergCatalog =
        new PolarisIcebergCatalog(
            entityManager,
            metaStoreManager,
            callContext,
            passthroughView,
            securityContext,
            taskExecutor,
            fileIOFactory);
  }

  @AfterEach
  public void after() throws IOException {
    metaStoreManager.purge(polarisContext);
  }

  private MetaStoreManagerFactory createMockMetaStoreManagerFactory() {
    return new MetaStoreManagerFactory() {
      @Override
      public PolarisMetaStoreManager getOrCreateMetaStoreManager(RealmContext realmContext) {
        return metaStoreManager;
      }

      @Override
      public Supplier<TransactionalPersistence> getOrCreateSessionSupplier(
          RealmContext realmContext) {
        return () -> polarisContext.getMetaStore();
      }

      @Override
      public StorageCredentialCache getOrCreateStorageCredentialCache(RealmContext realmContext) {
        return new StorageCredentialCache();
      }

      @Override
      public EntityCache getOrCreateEntityCache(RealmContext realmContext) {
        return new EntityCache(metaStoreManager);
      }

      @Override
      public Map<String, PolarisSecretsManager.PrincipalSecretsResult> bootstrapRealms(
          Iterable<String> realms, RootCredentialsSet rootCredentialsSet) {
        throw new NotImplementedException("Bootstrapping realms is not supported");
      }

      @Override
      public Map<String, BaseResult> purgeRealms(Iterable<String> realms) {
        throw new NotImplementedException("Purging realms is not supported");
      }
    };
  }

  @Test
  public void testCreateGenericTableDoesNotThrow() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);
    Assertions.assertThatCode(
            () ->
                genericTableCatalog.createGenericTable(
                    TableIdentifier.of("ns", "t1"), "test-format", Map.of()))
        .doesNotThrowAnyException();
  }

  @Test
  public void testGenericTableRoundTrip() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);

    String tableName = "t1";
    Map<String, String> properties = Map.of("a", "b", "c", "d");
    String format = "round-trip-format";

    genericTableCatalog.createGenericTable(TableIdentifier.of("ns", tableName), format, properties);

    // todo remove debugging
    PolarisPassthroughResolutionView passthroughView =
        new PolarisPassthroughResolutionView(
            callContext, entityManager, securityContext, CATALOG_NAME);
    PolarisResolvedPathWrapper dbg = passthroughView.getPassthroughResolvedPath(namespace);
    System.out.println(dbg);
    // todo end of debugging

    GenericTableEntity resultEntity =
        genericTableCatalog.loadGenericTable(TableIdentifier.of("ns", tableName));

    Assertions.assertThat(resultEntity.getFormat()).isEqualTo(format);
    Assertions.assertThat(resultEntity.getProperties()).isEqualTo(properties);
    Assertions.assertThat(resultEntity.getName()).isEqualTo(tableName);
  }
}
