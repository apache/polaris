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
package org.apache.polaris.service.catalog.policy;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.polaris.core.policy.PredefinedPolicyTypes.DATA_COMPACTION;
import static org.apache.polaris.core.policy.PredefinedPolicyTypes.METADATA_COMPACTION;
import static org.apache.polaris.core.policy.PredefinedPolicyTypes.ORPHAN_FILE_REMOVAL;
import static org.apache.polaris.core.policy.PredefinedPolicyTypes.TEST_NON_INHERITABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusMock;
import jakarta.inject.Inject;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisAuthorizerImpl;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolicyMappingAlreadyExistsException;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolverFactory;
import org.apache.polaris.core.policy.PredefinedPolicyTypes;
import org.apache.polaris.core.policy.exceptions.NoSuchPolicyException;
import org.apache.polaris.core.policy.exceptions.PolicyInUseException;
import org.apache.polaris.core.policy.exceptions.PolicyVersionMismatchException;
import org.apache.polaris.core.policy.validator.InvalidPolicyException;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.aws.AwsCredentialsStorageIntegration;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.service.admin.PolarisAdminService;
import org.apache.polaris.service.catalog.PolarisPassthroughResolutionView;
import org.apache.polaris.service.catalog.iceberg.IcebergCatalog;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.context.catalog.PolarisPrincipalHolder;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.events.listeners.NoOpPolarisEventListener;
import org.apache.polaris.service.storage.PolarisStorageIntegrationProviderImpl;
import org.apache.polaris.service.task.TaskExecutor;
import org.apache.polaris.service.types.ApplicablePolicy;
import org.apache.polaris.service.types.Policy;
import org.apache.polaris.service.types.PolicyAttachmentTarget;
import org.apache.polaris.service.types.PolicyIdentifier;
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

public abstract class AbstractPolicyCatalogTest {

  private static final Namespace NS = Namespace.of("ns1");
  private static final TableIdentifier TABLE = TableIdentifier.of(NS, "table");
  private static final String CATALOG_NAME = "polaris-catalog";
  private static final String TEST_ACCESS_KEY = "test_access_key";
  private static final String SECRET_ACCESS_KEY = "secret_access_key";
  private static final String SESSION_TOKEN = "session_token";
  private static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get(), "unique ID"),
          required(4, "data", Types.StringType.get()));

  private static final PolicyIdentifier POLICY1 = new PolicyIdentifier(NS, "p1");
  private static final PolicyIdentifier POLICY2 = new PolicyIdentifier(NS, "p2");
  private static final PolicyIdentifier POLICY3 = new PolicyIdentifier(NS, "p3");
  private static final PolicyIdentifier POLICY4 = new PolicyIdentifier(NS, "p4");
  private static final PolicyAttachmentTarget POLICY_ATTACH_TARGET_NS =
      new PolicyAttachmentTarget(PolicyAttachmentTarget.TypeEnum.NAMESPACE, List.of(NS.levels()));
  private static final PolicyAttachmentTarget POLICY_ATTACH_TARGET_TBL =
      new PolicyAttachmentTarget(
          PolicyAttachmentTarget.TypeEnum.TABLE_LIKE, List.of(TABLE.toString().split("\\.")));

  @Inject ServiceIdentityProvider serviceIdentityProvider;
  @Inject StorageCredentialCache storageCredentialCache;
  @Inject PolarisStorageIntegrationProvider storageIntegrationProvider;
  @Inject PolarisDiagnostics diagServices;
  @Inject ResolverFactory resolverFactory;
  @Inject ResolutionManifestFactory resolutionManifestFactory;
  @Inject PolarisEventMetadataFactory eventMetadataFactory;
  @Inject PolarisMetaStoreManager metaStoreManager;
  @Inject UserSecretsManager userSecretsManager;
  @Inject CallContext callContext;
  @Inject RealmConfig realmConfig;
  @Inject StorageAccessConfigProvider storageAccessConfigProvider;
  @Inject FileIOFactory fileIOFactory;
  @Inject PolarisPrincipalHolder polarisPrincipalHolder;

  private PolicyCatalog policyCatalog;
  private IcebergCatalog icebergCatalog;
  private AwsStorageConfigInfo storageConfigModel;
  private String realmName;
  private PolarisCallContext polarisContext;
  private PolarisAdminService adminService;
  private PolarisPrincipal authenticatedRoot;
  private PolarisEntity catalogEntity;

  @BeforeAll
  public static void setUpMocks() {
    PolarisStorageIntegrationProviderImpl mock =
        Mockito.mock(PolarisStorageIntegrationProviderImpl.class);
    QuarkusMock.installMockForType(mock, PolarisStorageIntegrationProviderImpl.class);
  }

  protected void bootstrapRealm(String realmName) {}

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void before(TestInfo testInfo) {
    storageCredentialCache.invalidateAll();

    realmName =
        "realm_%s_%s"
            .formatted(
                testInfo.getTestMethod().map(Method::getName).orElse("test"), System.nanoTime());
    bootstrapRealm(realmName);

    RealmContext realmContext = () -> realmName;
    QuarkusMock.installMockForType(realmContext, RealmContext.class);
    polarisContext = callContext.getPolarisCallContext();

    PrincipalEntity rootPrincipal =
        metaStoreManager.findRootPrincipal(polarisContext).orElseThrow();
    authenticatedRoot = PolarisPrincipal.of(rootPrincipal, Set.of());
    polarisPrincipalHolder.set(authenticatedRoot);

    PolarisAuthorizer authorizer = new PolarisAuthorizerImpl(realmConfig);
    ReservedProperties reservedProperties = ReservedProperties.NONE;

    adminService =
        new PolarisAdminService(
            polarisContext,
            resolutionManifestFactory,
            metaStoreManager,
            userSecretsManager,
            serviceIdentityProvider,
            authenticatedRoot,
            authorizer,
            reservedProperties);

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
            new CreateCatalogRequest(
                new CatalogEntity.Builder()
                    .setName(CATALOG_NAME)
                    .setDefaultBaseLocation(storageLocation)
                    .setReplaceNewLocationPrefixWithCatalogDefault("file:")
                    .addProperty(
                        FeatureConfiguration.ALLOW_EXTERNAL_TABLE_LOCATION.catalogConfig(), "true")
                    .addProperty(
                        FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(),
                        "true")
                    .setStorageConfigurationInfo(realmConfig, storageConfigModel, storageLocation)
                    .build()
                    .asCatalog(serviceIdentityProvider)));

    PolarisPassthroughResolutionView passthroughView =
        new PolarisPassthroughResolutionView(
            resolutionManifestFactory, authenticatedRoot, CATALOG_NAME);
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
        new AwsCredentialsStorageIntegration(
            (AwsStorageConfigurationInfo)
                CatalogEntity.of(catalogEntity).getStorageConfigurationInfo(),
            stsClient);
    when(storageIntegrationProvider.getStorageIntegrationForConfig(
            isA(AwsStorageConfigurationInfo.class)))
        .thenReturn((PolarisStorageIntegration) storageIntegration);

    this.policyCatalog = new PolicyCatalog(metaStoreManager, polarisContext, passthroughView);
    this.icebergCatalog =
        new IcebergCatalog(
            diagServices,
            resolverFactory,
            metaStoreManager,
            polarisContext,
            passthroughView,
            authenticatedRoot,
            taskExecutor,
            storageAccessConfigProvider,
            fileIOFactory,
            new NoOpPolarisEventListener(),
            eventMetadataFactory);
    this.icebergCatalog.initialize(
        CATALOG_NAME,
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));
  }

  @AfterEach
  public void after() throws IOException {
    metaStoreManager.purge(polarisContext);
  }

  @Test
  public void testCreatePolicyDoesNotThrow() {
    icebergCatalog.createNamespace(NS);
    assertThatCode(
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
    icebergCatalog.createNamespace(NS);
    policyCatalog.createPolicy(
        POLICY1, PredefinedPolicyTypes.DATA_COMPACTION.getName(), "test", "{\"enable\": false}");
    assertThatThrownBy(
            () ->
                policyCatalog.createPolicy(
                    POLICY1,
                    PredefinedPolicyTypes.DATA_COMPACTION.getName(),
                    "test",
                    "{\"enable\": false}"))
        .isInstanceOf(AlreadyExistsException.class);

    assertThatThrownBy(
            () ->
                policyCatalog.createPolicy(
                    POLICY1,
                    PredefinedPolicyTypes.SNAPSHOT_EXPIRY.getName(),
                    "test",
                    "{\"enable\": false}"))
        .isInstanceOf(AlreadyExistsException.class);
  }

  @Test
  public void testListPolicies() {
    icebergCatalog.createNamespace(NS);
    policyCatalog.createPolicy(
        POLICY1, PredefinedPolicyTypes.DATA_COMPACTION.getName(), "test", "{\"enable\": false}");

    policyCatalog.createPolicy(
        POLICY2,
        PredefinedPolicyTypes.METADATA_COMPACTION.getName(),
        "test",
        "{\"enable\": false}");

    policyCatalog.createPolicy(
        POLICY3, PredefinedPolicyTypes.SNAPSHOT_EXPIRY.getName(), "test", "{\"enable\": false}");

    policyCatalog.createPolicy(
        POLICY4, ORPHAN_FILE_REMOVAL.getName(), "test", "{\"enable\": false}");

    List<PolicyIdentifier> listResult = policyCatalog.listPolicies(NS, null);
    assertThat(listResult).hasSize(4);
    assertThat(listResult).containsExactlyInAnyOrder(POLICY1, POLICY2, POLICY3, POLICY4);
  }

  @Test
  public void testListPoliciesFilterByPolicyType() {
    icebergCatalog.createNamespace(NS);
    policyCatalog.createPolicy(
        POLICY1, PredefinedPolicyTypes.DATA_COMPACTION.getName(), "test", "{\"enable\": false}");

    policyCatalog.createPolicy(
        POLICY2,
        PredefinedPolicyTypes.METADATA_COMPACTION.getName(),
        "test",
        "{\"enable\": false}");

    policyCatalog.createPolicy(
        POLICY3, PredefinedPolicyTypes.SNAPSHOT_EXPIRY.getName(), "test", "{\"enable\": false}");

    policyCatalog.createPolicy(
        POLICY4, ORPHAN_FILE_REMOVAL.getName(), "test", "{\"enable\": false}");

    List<PolicyIdentifier> listResult = policyCatalog.listPolicies(NS, ORPHAN_FILE_REMOVAL);
    assertThat(listResult).hasSize(1);
    assertThat(listResult).containsExactlyInAnyOrder(POLICY4);
  }

  @Test
  public void testLoadPolicy() {
    icebergCatalog.createNamespace(NS);
    policyCatalog.createPolicy(
        POLICY1, PredefinedPolicyTypes.DATA_COMPACTION.getName(), "test", "{\"enable\": false}");

    Policy policy = policyCatalog.loadPolicy(POLICY1);
    assertThat(policy.getVersion()).isEqualTo(0);
    assertThat(policy.getPolicyType()).isEqualTo(PredefinedPolicyTypes.DATA_COMPACTION.getName());
    assertThat(policy.getContent()).isEqualTo("{\"enable\": false}");
    assertThat(policy.getName()).isEqualTo("p1");
    assertThat(policy.getDescription()).isEqualTo("test");
  }

  @Test
  public void testCreatePolicyWithInvalidContent() {
    icebergCatalog.createNamespace(NS);

    assertThatThrownBy(
            () ->
                policyCatalog.createPolicy(
                    POLICY1, PredefinedPolicyTypes.DATA_COMPACTION.getName(), "test", "invalid"))
        .isInstanceOf(InvalidPolicyException.class);
  }

  @Test
  public void testLoadPolicyNotExist() {
    icebergCatalog.createNamespace(NS);

    assertThatThrownBy(() -> policyCatalog.loadPolicy(POLICY1))
        .isInstanceOf(NoSuchPolicyException.class);
  }

  @Test
  public void testUpdatePolicy() {
    icebergCatalog.createNamespace(NS);
    policyCatalog.createPolicy(
        POLICY1, PredefinedPolicyTypes.DATA_COMPACTION.getName(), "test", "{\"enable\": false}");
    policyCatalog.updatePolicy(POLICY1, "updated", "{\"enable\": true}", 0);

    Policy policy = policyCatalog.loadPolicy(POLICY1);
    assertThat(policy.getVersion()).isEqualTo(1);
    assertThat(policy.getPolicyType()).isEqualTo(PredefinedPolicyTypes.DATA_COMPACTION.getName());
    assertThat(policy.getContent()).isEqualTo("{\"enable\": true}");
    assertThat(policy.getName()).isEqualTo("p1");
    assertThat(policy.getDescription()).isEqualTo("updated");
  }

  @Test
  public void testUpdatePolicyWithWrongVersion() {
    icebergCatalog.createNamespace(NS);
    policyCatalog.createPolicy(
        POLICY1, PredefinedPolicyTypes.DATA_COMPACTION.getName(), "test", "{\"enable\": false}");

    assertThatThrownBy(
            () -> policyCatalog.updatePolicy(POLICY1, "updated", "{\"enable\": true}", 1))
        .isInstanceOf(PolicyVersionMismatchException.class);
  }

  @Test
  public void testUpdatePolicyWithInvalidContent() {
    icebergCatalog.createNamespace(NS);
    policyCatalog.createPolicy(
        POLICY1, PredefinedPolicyTypes.DATA_COMPACTION.getName(), "test", "{\"enable\": false}");

    assertThatThrownBy(() -> policyCatalog.updatePolicy(POLICY1, "updated", "invalid", 0))
        .isInstanceOf(InvalidPolicyException.class);
  }

  @Test
  public void testUpdatePolicyNotExist() {
    icebergCatalog.createNamespace(NS);

    assertThatThrownBy(
            () -> policyCatalog.updatePolicy(POLICY1, "updated", "{\"enable\": true}", 0))
        .isInstanceOf(NoSuchPolicyException.class);
  }

  @Test
  public void testDropPolicy() {
    icebergCatalog.createNamespace(NS);
    policyCatalog.createPolicy(
        POLICY1, PredefinedPolicyTypes.DATA_COMPACTION.getName(), "test", "{\"enable\": false}");

    policyCatalog.dropPolicy(POLICY1, false);
    assertThatThrownBy(() -> policyCatalog.loadPolicy(POLICY1))
        .isInstanceOf(NoSuchPolicyException.class);
  }

  @Test
  public void testDropPolicyInUse() {
    icebergCatalog.createNamespace(NS);
    policyCatalog.createPolicy(
        POLICY1, PredefinedPolicyTypes.DATA_COMPACTION.getName(), "test", "{\"enable\": false}");
    var target = new PolicyAttachmentTarget(PolicyAttachmentTarget.TypeEnum.CATALOG, List.of());
    policyCatalog.attachPolicy(POLICY1, target, null);

    assertThatThrownBy(() -> policyCatalog.dropPolicy(POLICY1, false))
        .isInstanceOf(PolicyInUseException.class);

    // The policy is still attached to the catalog
    List<ApplicablePolicy> applicablePolicies =
        policyCatalog.getApplicablePolicies(null, null, null);
    assertThat(applicablePolicies.size()).isEqualTo(1);

    // Drop the policy with detach-all flag
    policyCatalog.dropPolicy(POLICY1, true);

    // The policy should be detached from the catalog and dropped
    applicablePolicies = policyCatalog.getApplicablePolicies(null, null, null);
    assertThat(applicablePolicies.size()).isEqualTo(0);
  }

  @Test
  public void testDropPolicyNotExist() {
    icebergCatalog.createNamespace(NS);

    assertThatThrownBy(() -> policyCatalog.dropPolicy(POLICY1, false))
        .isInstanceOf(NoSuchPolicyException.class);
  }

  @Test
  public void testAttachPolicy() {
    icebergCatalog.createNamespace(NS);
    policyCatalog.createPolicy(POLICY1, DATA_COMPACTION.getName(), "test", "{\"enable\": false}");

    var target = new PolicyAttachmentTarget(PolicyAttachmentTarget.TypeEnum.CATALOG, List.of());
    policyCatalog.attachPolicy(POLICY1, target, null);
    assertThat(policyCatalog.getApplicablePolicies(null, null, null).size()).isEqualTo(1);
  }

  @Test
  public void testAttachPolicyConflict() {
    icebergCatalog.createNamespace(NS);
    policyCatalog.createPolicy(POLICY1, DATA_COMPACTION.getName(), "test", "{\"enable\": false}");
    policyCatalog.createPolicy(POLICY2, DATA_COMPACTION.getName(), "test", "{\"enable\": true}");

    var target = new PolicyAttachmentTarget(PolicyAttachmentTarget.TypeEnum.CATALOG, List.of());
    policyCatalog.attachPolicy(POLICY1, target, null);
    // Attempt to attach a conflicting second policy and expect an exception
    assertThatThrownBy(() -> policyCatalog.attachPolicy(POLICY2, target, null))
        .isInstanceOf(PolicyMappingAlreadyExistsException.class)
        .hasMessage(
            String.format(
                "The policy mapping of same type (%s) for %s already exists",
                DATA_COMPACTION.getName(), CATALOG_NAME));
  }

  @Test
  public void testDetachPolicy() {
    icebergCatalog.createNamespace(NS);
    policyCatalog.createPolicy(POLICY1, DATA_COMPACTION.getName(), "test", "{\"enable\": false}");

    policyCatalog.attachPolicy(POLICY1, POLICY_ATTACH_TARGET_NS, null);
    assertThat(policyCatalog.getApplicablePolicies(NS, null, null).size()).isEqualTo(1);
    policyCatalog.detachPolicy(POLICY1, POLICY_ATTACH_TARGET_NS);
    assertThat(policyCatalog.getApplicablePolicies(NS, null, null).size()).isEqualTo(0);
  }

  @Test
  public void testPolicyOverwrite() {
    icebergCatalog.createNamespace(NS);
    policyCatalog.createPolicy(POLICY1, DATA_COMPACTION.getName(), "test", "{\"enable\": false}");
    policyCatalog.createPolicy(POLICY2, DATA_COMPACTION.getName(), "test", "{\"enable\": true}");

    // attach to catalog
    var target = new PolicyAttachmentTarget(PolicyAttachmentTarget.TypeEnum.CATALOG, List.of());
    policyCatalog.attachPolicy(POLICY1, target, null);

    // attach to namespace
    policyCatalog.attachPolicy(POLICY2, POLICY_ATTACH_TARGET_NS, null);
    var applicablePolicies = policyCatalog.getApplicablePolicies(NS, null, null);
    assertThat(applicablePolicies.size()).isEqualTo(1);
    assertThat(applicablePolicies.getFirst().getName())
        .isEqualTo(POLICY2.name())
        .as("Namespace level policy overwrite the catalog level policy with the same type");
  }

  @Test
  public void testPolicyInheritance() {
    icebergCatalog.createNamespace(NS);
    var p1 =
        policyCatalog.createPolicy(
            POLICY1, METADATA_COMPACTION.getName(), "test", "{\"enable\": false}");
    var p2 =
        policyCatalog.createPolicy(
            POLICY2, DATA_COMPACTION.getName(), "test", "{\"enable\": true}");

    // attach a policy to catalog
    var target = new PolicyAttachmentTarget(PolicyAttachmentTarget.TypeEnum.CATALOG, List.of());
    policyCatalog.attachPolicy(POLICY1, target, null);

    // attach a different type of policy to namespace
    policyCatalog.attachPolicy(POLICY2, POLICY_ATTACH_TARGET_NS, null);
    var applicablePolicies = policyCatalog.getApplicablePolicies(NS, null, null);
    assertThat(applicablePolicies.size()).isEqualTo(2);
    assertThat(applicablePolicies.contains(policyToApplicablePolicy(p1, true, NS))).isTrue();
    assertThat(applicablePolicies.contains(policyToApplicablePolicy(p2, false, NS))).isTrue();

    // attach policies to a table
    icebergCatalog.createTable(TABLE, SCHEMA);
    applicablePolicies = policyCatalog.getApplicablePolicies(NS, TABLE.name(), null);
    assertThat(applicablePolicies.size()).isEqualTo(2);
    // attach a third type of policy to a table
    policyCatalog.createPolicy(
        POLICY3, ORPHAN_FILE_REMOVAL.getName(), "test", "{\"enable\": false}");
    policyCatalog.attachPolicy(POLICY3, POLICY_ATTACH_TARGET_TBL, null);
    applicablePolicies = policyCatalog.getApplicablePolicies(NS, TABLE.name(), null);
    assertThat(applicablePolicies.size()).isEqualTo(3);
    // create policy 4 with one of types from its parent
    var p4 =
        policyCatalog.createPolicy(
            POLICY4, DATA_COMPACTION.getName(), "test", "{\"enable\": false}");
    policyCatalog.attachPolicy(POLICY4, POLICY_ATTACH_TARGET_TBL, null);
    applicablePolicies = policyCatalog.getApplicablePolicies(NS, TABLE.name(), null);
    // p2 should be overwritten by p4, as they are the same type
    assertThat(applicablePolicies.contains(policyToApplicablePolicy(p4, false, NS))).isTrue();
    assertThat(applicablePolicies.contains(policyToApplicablePolicy(p2, true, NS))).isFalse();
  }

  @Test
  public void testGetApplicablePoliciesFilterOnType() {
    icebergCatalog.createNamespace(NS);
    policyCatalog.createPolicy(
        POLICY1, METADATA_COMPACTION.getName(), "test", "{\"enable\": false}");
    var p2 =
        policyCatalog.createPolicy(
            POLICY2, DATA_COMPACTION.getName(), "test", "{\"enable\": true}");

    // attach a policy to catalog
    var target = new PolicyAttachmentTarget(PolicyAttachmentTarget.TypeEnum.CATALOG, List.of());
    policyCatalog.attachPolicy(POLICY1, target, null);

    // attach a different type of policy to namespace
    policyCatalog.attachPolicy(POLICY2, POLICY_ATTACH_TARGET_NS, null);
    var applicablePolicies = policyCatalog.getApplicablePolicies(NS, null, DATA_COMPACTION);
    // only p2 is with the type fetched
    assertThat(applicablePolicies.contains(policyToApplicablePolicy(p2, false, NS))).isTrue();
  }

  private static ApplicablePolicy policyToApplicablePolicy(
      Policy policy, boolean inherited, Namespace parent) {
    return new ApplicablePolicy(
        policy.getPolicyType(),
        policy.getInheritable(),
        policy.getName(),
        policy.getDescription(),
        policy.getContent(),
        policy.getVersion(),
        inherited,
        Arrays.asList(parent.levels()));
  }

  @Test
  public void testCreateNonInheritablePolicy() {
    icebergCatalog.createNamespace(NS);
    Policy policy =
        policyCatalog.createPolicy(
            POLICY1, TEST_NON_INHERITABLE.getName(), "test", "{\"enable\": false}");

    assertThat(policy).isNotNull();
    assertThat(policy.getName()).isEqualTo(POLICY1.name());
    assertThat(policy.getPolicyType()).isEqualTo(TEST_NON_INHERITABLE.getName());
    assertThat(policy.getInheritable()).isFalse();
  }

  @Test
  public void testNonInheritablePolicyDoesNotPropagateToNamespace() {
    icebergCatalog.createNamespace(NS);
    var policy =
        policyCatalog.createPolicy(
            POLICY1, TEST_NON_INHERITABLE.getName(), "test", "{\"enable\": false}");

    // Attach non-inheritable policy to catalog
    var catalogTarget =
        new PolicyAttachmentTarget(PolicyAttachmentTarget.TypeEnum.CATALOG, List.of());
    policyCatalog.attachPolicy(POLICY1, catalogTarget, null);

    // Non-inheritable policy should not appear in namespace applicable policies
    var applicablePolicies = policyCatalog.getApplicablePolicies(NS, null, null);
    assertThat(applicablePolicies).isEmpty();
  }

  @Test
  public void testNonInheritablePolicyDoesNotPropagateToTable() {
    icebergCatalog.createNamespace(NS);
    icebergCatalog.createTable(TABLE, SCHEMA);

    var policy =
        policyCatalog.createPolicy(
            POLICY1, TEST_NON_INHERITABLE.getName(), "test", "{\"enable\": false}");

    // Attach non-inheritable policy to catalog
    var catalogTarget =
        new PolicyAttachmentTarget(PolicyAttachmentTarget.TypeEnum.CATALOG, List.of());
    policyCatalog.attachPolicy(POLICY1, catalogTarget, null);

    // Non-inheritable policy should not appear in table applicable policies
    var applicablePolicies = policyCatalog.getApplicablePolicies(NS, TABLE.name(), null);
    assertThat(applicablePolicies).isEmpty();

    // Attach non-inheritable policy to namespace
    policyCatalog.attachPolicy(POLICY1, POLICY_ATTACH_TARGET_NS, null);

    // Non-inheritable policy from namespace should not appear in table applicable policies
    applicablePolicies = policyCatalog.getApplicablePolicies(NS, TABLE.name(), null);
    assertThat(applicablePolicies).isEmpty();
  }

  @Test
  public void testNonInheritablePolicyAppliesToDirectAttachmentOnly() {
    icebergCatalog.createNamespace(NS);
    icebergCatalog.createTable(TABLE, SCHEMA);

    var policy =
        policyCatalog.createPolicy(
            POLICY1, TEST_NON_INHERITABLE.getName(), "test", "{\"enable\": false}");

    // Attach non-inheritable policy directly to table
    policyCatalog.attachPolicy(POLICY1, POLICY_ATTACH_TARGET_TBL, null);

    // Non-inheritable policy should appear when querying the table directly
    var applicablePolicies = policyCatalog.getApplicablePolicies(NS, TABLE.name(), null);
    assertThat(applicablePolicies)
        .hasSize(1)
        .containsExactly(policyToApplicablePolicy(policy, false, NS));
  }

  @Test
  public void testMixedInheritableAndNonInheritablePolicies() {
    icebergCatalog.createNamespace(NS);
    icebergCatalog.createTable(TABLE, SCHEMA);

    // Create inheritable policy
    var inheritablePolicy =
        policyCatalog.createPolicy(
            POLICY1, DATA_COMPACTION.getName(), "inheritable", "{\"enable\": true}");

    // Create non-inheritable policy
    var nonInheritablePolicy =
        policyCatalog.createPolicy(
            POLICY2, TEST_NON_INHERITABLE.getName(), "non-inheritable", "{\"enable\": false}");

    // Attach both policies to catalog
    var catalogTarget =
        new PolicyAttachmentTarget(PolicyAttachmentTarget.TypeEnum.CATALOG, List.of());
    policyCatalog.attachPolicy(POLICY1, catalogTarget, null);
    policyCatalog.attachPolicy(POLICY2, catalogTarget, null);

    // Only inheritable policy should propagate to namespace
    var nsApplicablePolicies = policyCatalog.getApplicablePolicies(NS, null, null);
    assertThat(nsApplicablePolicies)
        .hasSize(1)
        .containsExactly(policyToApplicablePolicy(inheritablePolicy, true, NS));

    // Only inheritable policy should propagate to table
    var tableApplicablePolicies = policyCatalog.getApplicablePolicies(NS, TABLE.name(), null);
    assertThat(tableApplicablePolicies)
        .hasSize(1)
        .containsExactly(policyToApplicablePolicy(inheritablePolicy, true, NS));
  }

  @Test
  public void testNonInheritablePolicyFilteredByType() {
    icebergCatalog.createNamespace(NS);
    icebergCatalog.createTable(TABLE, SCHEMA);

    var policy =
        policyCatalog.createPolicy(
            POLICY1, TEST_NON_INHERITABLE.getName(), "test", "{\"enable\": false}");

    // Attach non-inheritable policy to table
    policyCatalog.attachPolicy(POLICY1, POLICY_ATTACH_TARGET_TBL, null);

    // Should find the policy when filtering by its type
    var applicablePolicies =
        policyCatalog.getApplicablePolicies(NS, TABLE.name(), TEST_NON_INHERITABLE);
    assertThat(applicablePolicies)
        .hasSize(1)
        .containsExactly(policyToApplicablePolicy(policy, false, NS));

    // Should not find the policy when filtering by a different type
    applicablePolicies = policyCatalog.getApplicablePolicies(NS, TABLE.name(), DATA_COMPACTION);
    assertThat(applicablePolicies).isEmpty();
  }

  @Test
  public void testMultipleNonInheritablePoliciesOnDifferentLevels() {
    icebergCatalog.createNamespace(NS);
    icebergCatalog.createTable(TABLE, SCHEMA);

    var catalogPolicy =
        policyCatalog.createPolicy(
            POLICY1, TEST_NON_INHERITABLE.getName(), "catalog-policy", "{\"enable\": false}");
    var namespacePolicy =
        policyCatalog.createPolicy(
            POLICY2, TEST_NON_INHERITABLE.getName(), "namespace-policy", "{\"enable\": true}");
    var tablePolicy =
        policyCatalog.createPolicy(
            POLICY3, TEST_NON_INHERITABLE.getName(), "table-policy", "{\"enable\": false}");

    // Attach policies to different levels
    var catalogTarget =
        new PolicyAttachmentTarget(PolicyAttachmentTarget.TypeEnum.CATALOG, List.of());
    policyCatalog.attachPolicy(POLICY1, catalogTarget, null);
    policyCatalog.attachPolicy(POLICY2, POLICY_ATTACH_TARGET_NS, null);
    policyCatalog.attachPolicy(POLICY3, POLICY_ATTACH_TARGET_TBL, null);

    // Catalog level: only catalog policy
    var catalogApplicable = policyCatalog.getApplicablePolicies(null, null, null);
    assertThat(catalogApplicable)
        .hasSize(1)
        .containsExactly(policyToApplicablePolicy(catalogPolicy, false, NS));

    // Namespace level: only namespace policy (not catalog policy)
    var namespaceApplicable = policyCatalog.getApplicablePolicies(NS, null, null);
    assertThat(namespaceApplicable)
        .hasSize(1)
        .containsExactly(policyToApplicablePolicy(namespacePolicy, false, NS));

    // Table level: only table policy (not catalog or namespace policies)
    var tableApplicable = policyCatalog.getApplicablePolicies(NS, TABLE.name(), null);
    assertThat(tableApplicable)
        .hasSize(1)
        .containsExactly(policyToApplicablePolicy(tablePolicy, false, NS));
  }
}
