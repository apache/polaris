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

import com.google.common.collect.ImmutableMap;
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
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizerImpl;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.core.persistence.transactional.TransactionalPersistence;
import org.apache.polaris.core.policy.PredefinedPolicyTypes;
import org.apache.polaris.core.policy.exceptions.NoSuchPolicyException;
import org.apache.polaris.core.policy.exceptions.PolicyVersionMismatchException;
import org.apache.polaris.core.policy.validator.InvalidPolicyException;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.aws.AwsCredentialsStorageIntegration;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.service.admin.PolarisAdminService;
import org.apache.polaris.service.catalog.PolarisPassthroughResolutionView;
import org.apache.polaris.service.catalog.iceberg.IcebergCatalog;
import org.apache.polaris.service.catalog.io.DefaultFileIOFactory;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.policy.PolicyCatalog;
import org.apache.polaris.service.config.RealmEntityManagerFactory;
import org.apache.polaris.service.storage.PolarisStorageIntegrationProviderImpl;
import org.apache.polaris.service.task.TaskExecutor;
import org.apache.polaris.service.types.Policy;
import org.apache.polaris.service.types.PolicyIdentifier;
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
@TestProfile(PolicyCatalogTest.Profile.class)
public class PolicyCatalogTest {
  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.features.defaults.\"ALLOW_SPECIFYING_FILE_IO_IMPL\"",
          "true",
          "polaris.features.defaults.\"INITIALIZE_DEFAULT_CATALOG_FILEIO_FOR_TEST\"",
          "true",
          "polaris.features.defaults.\"SUPPORTED_CATALOG_STORAGE_TYPES\"",
          "[\"FILE\"]");
    }
  }

  protected static final Namespace NS = Namespace.of("newdb");
  protected static final TableIdentifier TABLE = TableIdentifier.of(NS, "table");
  public static final String CATALOG_NAME = "polaris-catalog";
  public static final String TEST_ACCESS_KEY = "test_access_key";
  public static final String SECRET_ACCESS_KEY = "secret_access_key";
  public static final String SESSION_TOKEN = "session_token";

  private static final Namespace NS1 = Namespace.of("ns1");
  private static final PolicyIdentifier POLICY1 = new PolicyIdentifier(NS1, "p1");
  private static final PolicyIdentifier POLICY2 = new PolicyIdentifier(NS1, "p2");
  private static final PolicyIdentifier POLICY3 = new PolicyIdentifier(NS1, "p3");
  private static final PolicyIdentifier POLICY4 = new PolicyIdentifier(NS1, "p4");

  @Inject MetaStoreManagerFactory managerFactory;
  @Inject PolarisConfigurationStore configurationStore;
  @Inject PolarisStorageIntegrationProvider storageIntegrationProvider;
  @Inject PolarisDiagnostics diagServices;

  private PolicyCatalog policyCatalog;
  private IcebergCatalog icebergCatalog;
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
                    FeatureConfiguration.ALLOW_EXTERNAL_TABLE_LOCATION.catalogConfig(), "true")
                .addProperty(
                    FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(), "true")
                .setStorageConfigurationInfo(storageConfigModel, storageLocation)
                .build());

    PolarisPassthroughResolutionView passthroughView =
        new PolarisPassthroughResolutionView(
            callContext, entityManager, securityContext, CATALOG_NAME);
    TaskExecutor taskExecutor = Mockito.mock();
    RealmEntityManagerFactory realmEntityManagerFactory =
        new RealmEntityManagerFactory(createMockMetaStoreManagerFactory());
    this.fileIOFactory =
        new DefaultFileIOFactory(realmEntityManagerFactory, managerFactory, configurationStore);

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

    this.policyCatalog = new PolicyCatalog(metaStoreManager, callContext, passthroughView);
    this.icebergCatalog =
        new IcebergCatalog(
            entityManager,
            metaStoreManager,
            callContext,
            passthroughView,
            securityContext,
            taskExecutor,
            fileIOFactory);
    this.icebergCatalog.initialize(
        CATALOG_NAME,
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));
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
        return () -> ((TransactionalPersistence) polarisContext.getMetaStore());
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
      public Map<String, PrincipalSecretsResult> bootstrapRealms(
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
  public void testCreatePolicyDoesNotThrow() {
    icebergCatalog.createNamespace(NS1);
    Assertions.assertThatCode(
            () ->
                policyCatalog.createPolicy(
                    POLICY1,
                    PredefinedPolicyTypes.DATA_COMPACTION.getName(),
                    "test",
                    "{\"enable\": false}"))
        .doesNotThrowAnyException();
  }

  @Test
  public void testCreatePolicyAlreadyExists() {
    icebergCatalog.createNamespace(NS1);
    policyCatalog.createPolicy(
        POLICY1, PredefinedPolicyTypes.DATA_COMPACTION.getName(), "test", "{\"enable\": false}");

    Assertions.assertThatThrownBy(
            () ->
                policyCatalog.createPolicy(
                    POLICY1,
                    PredefinedPolicyTypes.DATA_COMPACTION.getName(),
                    "test",
                    "{\"enable\": false}"))
        .isInstanceOf(AlreadyExistsException.class);

    Assertions.assertThatThrownBy(
            () ->
                policyCatalog.createPolicy(
                    POLICY1,
                    PredefinedPolicyTypes.SNAPSHOT_RETENTION.getName(),
                    "test",
                    "{\"enable\": false}"))
        .isInstanceOf(AlreadyExistsException.class);
  }

  @Test
  public void testListPolicies() {
    icebergCatalog.createNamespace(NS1);
    policyCatalog.createPolicy(
        POLICY1, PredefinedPolicyTypes.DATA_COMPACTION.getName(), "test", "{\"enable\": false}");

    policyCatalog.createPolicy(
        POLICY2,
        PredefinedPolicyTypes.METADATA_COMPACTION.getName(),
        "test",
        "{\"enable\": false}");

    policyCatalog.createPolicy(
        POLICY3, PredefinedPolicyTypes.SNAPSHOT_RETENTION.getName(), "test", "{\"enable\": false}");

    policyCatalog.createPolicy(
        POLICY4,
        PredefinedPolicyTypes.ORPHAN_FILE_REMOVAL.getName(),
        "test",
        "{\"enable\": false}");

    List<PolicyIdentifier> listResult = policyCatalog.listPolicies(NS1, null);
    Assertions.assertThat(listResult).hasSize(4);
    Assertions.assertThat(listResult).containsExactlyInAnyOrder(POLICY1, POLICY2, POLICY3, POLICY4);
  }

  @Test
  public void testListPoliciesFilterByPolicyType() {
    icebergCatalog.createNamespace(NS1);
    policyCatalog.createPolicy(
        POLICY1, PredefinedPolicyTypes.DATA_COMPACTION.getName(), "test", "{\"enable\": false}");

    policyCatalog.createPolicy(
        POLICY2,
        PredefinedPolicyTypes.METADATA_COMPACTION.getName(),
        "test",
        "{\"enable\": false}");

    policyCatalog.createPolicy(
        POLICY3, PredefinedPolicyTypes.SNAPSHOT_RETENTION.getName(), "test", "{\"enable\": false}");

    policyCatalog.createPolicy(
        POLICY4,
        PredefinedPolicyTypes.ORPHAN_FILE_REMOVAL.getName(),
        "test",
        "{\"enable\": false}");

    List<PolicyIdentifier> listResult =
        policyCatalog.listPolicies(NS1, PredefinedPolicyTypes.ORPHAN_FILE_REMOVAL);
    Assertions.assertThat(listResult).hasSize(1);
    Assertions.assertThat(listResult).containsExactlyInAnyOrder(POLICY4);
  }

  @Test
  public void testLoadPolicy() {
    icebergCatalog.createNamespace(NS1);
    policyCatalog.createPolicy(
        POLICY1, PredefinedPolicyTypes.DATA_COMPACTION.getName(), "test", "{\"enable\": false}");

    Policy policy = policyCatalog.loadPolicy(POLICY1);
    Assertions.assertThat(policy.getVersion()).isEqualTo(0);
    Assertions.assertThat(policy.getPolicyType())
        .isEqualTo(PredefinedPolicyTypes.DATA_COMPACTION.getName());
    Assertions.assertThat(policy.getContent()).isEqualTo("{\"enable\": false}");
    Assertions.assertThat(policy.getName()).isEqualTo("p1");
    Assertions.assertThat(policy.getDescription()).isEqualTo("test");
  }

  @Test
  public void testCreatePolicyWithInvalidContent() {
    icebergCatalog.createNamespace(NS1);

    Assertions.assertThatThrownBy(
            () ->
                policyCatalog.createPolicy(
                    POLICY1, PredefinedPolicyTypes.DATA_COMPACTION.getName(), "test", "invalid"))
        .isInstanceOf(InvalidPolicyException.class);
  }

  @Test
  public void testLoadPolicyNotExist() {
    icebergCatalog.createNamespace(NS1);

    Assertions.assertThatThrownBy(() -> policyCatalog.loadPolicy(POLICY1))
        .isInstanceOf(NoSuchPolicyException.class);
  }

  @Test
  public void testUpdatePolicy() {
    icebergCatalog.createNamespace(NS1);
    policyCatalog.createPolicy(
        POLICY1, PredefinedPolicyTypes.DATA_COMPACTION.getName(), "test", "{\"enable\": false}");

    policyCatalog.updatePolicy(POLICY1, "updated", "{\"enable\": true}", 0);

    Policy policy = policyCatalog.loadPolicy(POLICY1);
    Assertions.assertThat(policy.getVersion()).isEqualTo(1);
    Assertions.assertThat(policy.getPolicyType())
        .isEqualTo(PredefinedPolicyTypes.DATA_COMPACTION.getName());
    Assertions.assertThat(policy.getContent()).isEqualTo("{\"enable\": true}");
    Assertions.assertThat(policy.getName()).isEqualTo("p1");
    Assertions.assertThat(policy.getDescription()).isEqualTo("updated");
  }

  @Test
  public void testUpdatePolicyWithWrongVersion() {
    icebergCatalog.createNamespace(NS1);
    policyCatalog.createPolicy(
        POLICY1, PredefinedPolicyTypes.DATA_COMPACTION.getName(), "test", "{\"enable\": false}");

    Assertions.assertThatThrownBy(
            () -> policyCatalog.updatePolicy(POLICY1, "updated", "{\"enable\": true}", 1))
        .isInstanceOf(PolicyVersionMismatchException.class);
  }

  @Test
  public void testUpdatePolicyWithInvalidContent() {
    icebergCatalog.createNamespace(NS1);
    policyCatalog.createPolicy(
        POLICY1, PredefinedPolicyTypes.DATA_COMPACTION.getName(), "test", "{\"enable\": false}");

    Assertions.assertThatThrownBy(
            () -> policyCatalog.updatePolicy(POLICY1, "updated", "invalid", 0))
        .isInstanceOf(InvalidPolicyException.class);
  }

  @Test
  public void testUpdatePolicyNotExist() {
    icebergCatalog.createNamespace(NS1);

    Assertions.assertThatThrownBy(
            () -> policyCatalog.updatePolicy(POLICY1, "updated", "{\"enable\": true}", 0))
        .isInstanceOf(NoSuchPolicyException.class);
  }

  @Test
  public void testDropPolicy() {
    icebergCatalog.createNamespace(NS1);
    policyCatalog.createPolicy(
        POLICY1, PredefinedPolicyTypes.DATA_COMPACTION.getName(), "test", "{\"enable\": false}");

    Assertions.assertThat(policyCatalog.dropPolicy(POLICY1, false)).isTrue();
    Assertions.assertThatThrownBy(() -> policyCatalog.loadPolicy(POLICY1))
        .isInstanceOf(NoSuchPolicyException.class);
  }

  @Test
  public void testDropPolicyNotExist() {
    icebergCatalog.createNamespace(NS1);

    Assertions.assertThatThrownBy(() -> policyCatalog.dropPolicy(POLICY1, false))
        .isInstanceOf(NoSuchPolicyException.class);
  }
}
