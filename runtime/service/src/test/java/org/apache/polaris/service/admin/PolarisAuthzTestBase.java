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
package org.apache.polaris.service.admin;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.polaris.core.entity.CatalogEntity.DEFAULT_BASE_LOCATION_KEY;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusMock;
import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.IcebergRestConnectionConfigInfo;
import org.apache.polaris.core.admin.model.OAuthClientCredentialsParameters;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.PrincipalWithCredentialsCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisAuthorizerImpl;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.CatalogRoleEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.PrivilegeResult;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolverFactory;
import org.apache.polaris.core.policy.PredefinedPolicyTypes;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.core.secrets.UserSecretsManagerFactory;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.service.catalog.PolarisPassthroughResolutionView;
import org.apache.polaris.service.catalog.Profiles;
import org.apache.polaris.service.catalog.generic.PolarisGenericTableCatalog;
import org.apache.polaris.service.catalog.iceberg.CatalogHandlerUtils;
import org.apache.polaris.service.catalog.iceberg.IcebergCatalog;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.catalog.policy.PolicyCatalog;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.context.catalog.CallContextCatalogFactory;
import org.apache.polaris.service.context.catalog.PolarisCallContextCatalogFactory;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.apache.polaris.service.storage.PolarisStorageIntegrationProviderImpl;
import org.apache.polaris.service.task.TaskExecutor;
import org.apache.polaris.service.types.PolicyIdentifier;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sts.StsClient;

/** Base class for shared test setup logic used by various Polaris authz-related tests. */
public abstract class PolarisAuthzTestBase {

  public static class Profile extends Profiles.DefaultProfile {

    @Override
    public Set<Class<?>> getEnabledAlternatives() {
      return Set.of(TestPolarisCallContextCatalogFactory.class);
    }

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("polaris.features.\"ALLOW_EXTERNAL_METADATA_FILE_LOCATION\"", "true")
          .put("polaris.features.\"ENABLE_GENERIC_TABLES\"", "true")
          .put(
              "polaris.features.\"ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING\"",
              "true")
          .put("polaris.features.\"DROP_WITH_PURGE_ENABLED\"", "true")
          .put("polaris.behavior-changes.\"ALLOW_NAMESPACE_CUSTOM_LOCATION\"", "true")
          .put("polaris.features.\"ENABLE_CATALOG_FEDERATION\"", "true")
          .build();
    }
  }

  protected static final String CATALOG_NAME = "polaris-catalog";
  protected static final String FEDERATED_CATALOG_NAME = "federated-polaris-catalog";
  protected static final String PRINCIPAL_NAME = "snowman";

  // catalog_role1 will be assigned only to principal_role1 and
  // catalog_role2 will be assigned only to principal_role2
  protected static final String PRINCIPAL_ROLE1 = "principal_role1";
  protected static final String PRINCIPAL_ROLE2 = "principal_role2";
  protected static final String CATALOG_ROLE1 = "catalog_role1";
  protected static final String CATALOG_ROLE2 = "catalog_role2";
  protected static final String CATALOG_ROLE_SHARED = "catalog_role_shared";

  protected static final Namespace NS1 = Namespace.of("ns1");
  protected static final Namespace NS2 = Namespace.of("ns2");
  protected static final Namespace NS1A = Namespace.of("ns1", "ns1a");
  protected static final Namespace NS1AA = Namespace.of("ns1", "ns1a", "ns1aa");
  protected static final Namespace NS1B = Namespace.of("ns1", "ns1b");

  // One table directly under ns1
  protected static final TableIdentifier TABLE_NS1_1 = TableIdentifier.of(NS1, "layer1_table");

  // A generic table directly under ns1
  protected static final TableIdentifier TABLE_NS1_1_GENERIC =
      TableIdentifier.of(NS1, "layer1_table_generic");

  // A policy directly under ns1
  protected static final PolicyIdentifier POLICY_NS1_1 = new PolicyIdentifier(NS1, "layer1_policy");

  // Two tables under ns1a
  protected static final TableIdentifier TABLE_NS1A_1 = TableIdentifier.of(NS1A, "table1");
  protected static final TableIdentifier TABLE_NS1A_2 = TableIdentifier.of(NS1A, "table2");

  // One table under ns1b with same name as one under ns1a
  protected static final TableIdentifier TABLE_NS1B_1 = TableIdentifier.of(NS1B, "table1");

  // One table directly under ns2
  protected static final TableIdentifier TABLE_NS2_1 = TableIdentifier.of(NS2, "table1");

  // One view directly under ns1
  protected static final TableIdentifier VIEW_NS1_1 = TableIdentifier.of(NS1, "layer1_view");

  // Two views under ns1a
  protected static final TableIdentifier VIEW_NS1A_1 = TableIdentifier.of(NS1A, "view1");
  protected static final TableIdentifier VIEW_NS1A_2 = TableIdentifier.of(NS1A, "view2");

  // One view under ns1b with same name as one under ns1a
  protected static final TableIdentifier VIEW_NS1B_1 = TableIdentifier.of(NS1B, "view1");

  // One view directly under ns2
  protected static final TableIdentifier VIEW_NS2_1 = TableIdentifier.of(NS2, "view1");

  protected static final String VIEW_QUERY = "select * from ns1.layer1_table";

  public static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get(), "unique ID 🤪"),
          required(4, "data", Types.StringType.get()));
  protected final ReservedProperties reservedProperties = ReservedProperties.NONE;

  @Inject protected MetaStoreManagerFactory managerFactory;
  @Inject protected ResolutionManifestFactory resolutionManifestFactory;
  @Inject protected CallContextCatalogFactory callContextCatalogFactory;
  @Inject protected UserSecretsManagerFactory userSecretsManagerFactory;
  @Inject protected ServiceIdentityProvider serviceIdentityProvider;
  @Inject protected PolarisCredentialManager credentialManager;
  @Inject protected PolarisDiagnostics diagServices;
  @Inject protected FileIOFactory fileIOFactory;
  @Inject protected PolarisEventListener polarisEventListener;
  @Inject protected CatalogHandlerUtils catalogHandlerUtils;
  @Inject protected PolarisConfigurationStore configurationStore;
  @Inject protected StorageCredentialCache storageCredentialCache;
  @Inject protected ResolverFactory resolverFactory;
  @Inject protected StorageAccessConfigProvider storageAccessConfigProvider;

  protected IcebergCatalog baseCatalog;
  protected PolarisGenericTableCatalog genericTableCatalog;
  protected PolicyCatalog policyCatalog;
  protected PolarisAdminService adminService;
  protected PolarisMetaStoreManager metaStoreManager;
  protected UserSecretsManager userSecretsManager;
  protected PolarisBaseEntity catalogEntity;
  protected PolarisBaseEntity federatedCatalogEntity;
  protected PrincipalEntity principalEntity;
  protected CallContext callContext;
  protected RealmConfig realmConfig;
  protected PolarisPrincipal authenticatedRoot;
  protected PolarisAuthorizer polarisAuthorizer;

  protected PolarisCallContext polarisContext;

  @BeforeAll
  public static void setUpMocks() {
    StsClient stsClient = Mockito.mock(StsClient.class);
    PolarisStorageIntegrationProviderImpl mock =
        new PolarisStorageIntegrationProviderImpl(
            destination -> stsClient,
            Optional.empty(),
            () -> GoogleCredentials.create(new AccessToken("abc", new Date())));
    QuarkusMock.installMockForType(mock, PolarisStorageIntegrationProviderImpl.class);
  }

  @BeforeEach
  public void before(TestInfo testInfo) {
    storageCredentialCache.invalidateAll();

    RealmContext realmContext = testInfo::getDisplayName;
    QuarkusMock.installMockForType(realmContext, RealmContext.class);
    ContainerRequestContext containerRequestContext = Mockito.mock(ContainerRequestContext.class);
    Mockito.when(containerRequestContext.getProperty(Mockito.anyString()))
        .thenReturn("request-id-1");
    QuarkusMock.installMockForType(containerRequestContext, ContainerRequestContext.class);
    metaStoreManager = managerFactory.getOrCreateMetaStoreManager(realmContext);
    userSecretsManager = userSecretsManagerFactory.getOrCreateUserSecretsManager(realmContext);

    polarisContext =
        new PolarisCallContext(
            realmContext, managerFactory.getOrCreateSession(realmContext), configurationStore);

    callContext = polarisContext;
    realmConfig = polarisContext.getRealmConfig();

    polarisAuthorizer = new PolarisAuthorizerImpl(realmConfig);

    PrincipalEntity rootPrincipal =
        metaStoreManager.findRootPrincipal(polarisContext).orElseThrow();
    this.authenticatedRoot = PolarisPrincipal.of(rootPrincipal, Set.of());

    this.adminService =
        new PolarisAdminService(
            diagServices,
            callContext,
            resolutionManifestFactory,
            metaStoreManager,
            userSecretsManager,
            serviceIdentityProvider,
            securityContext(authenticatedRoot),
            polarisAuthorizer,
            reservedProperties);

    String storageLocation = "file:///tmp/authz";
    String storageLocationForFederatedCatalog = "file:///tmp/authz_federated";
    FileStorageConfigInfo storageConfigModel =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of(storageLocation, "file:///tmp/authz"))
            .build();
    FileStorageConfigInfo storageConfigModelForFederatedCatalog =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(
                List.of(storageLocationForFederatedCatalog, "file:///tmp/authz_federated"))
            .build();
    ConnectionConfigInfo connectionConfigInfo =
        IcebergRestConnectionConfigInfo.builder(
                ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST)
            .setUri("https://myorg-my_account.snowflakecomputing.com/polaris/api/catalog")
            .setRemoteCatalogName("my-remote-catalog")
            .setAuthenticationParameters(
                OAuthClientCredentialsParameters.builder(
                        AuthenticationParameters.AuthenticationTypeEnum.OAUTH)
                    .setClientId("my-client-id")
                    .setClientSecret("my-client-secret")
                    .setScopes(List.of("PRINCIPAL_ROLE:ALL"))
                    .build())
            .build();
    CatalogEntity externalCatalogEntity =
        new CatalogEntity.Builder()
            .setName(FEDERATED_CATALOG_NAME)
            .setCatalogType("EXTERNAL")
            .setDefaultBaseLocation(storageLocationForFederatedCatalog)
            .setStorageConfigurationInfo(
                realmConfig,
                storageConfigModelForFederatedCatalog,
                storageLocationForFederatedCatalog)
            .addProperty("polaris.config.enable-sub-catalog-rbac-for-federated-catalogs", "true")
            .build();
    ExternalCatalog externalCatalog =
        ExternalCatalog.builder()
            .setName(externalCatalogEntity.getName())
            .setType(ExternalCatalog.TypeEnum.EXTERNAL)
            .setProperties(
                org.apache.polaris.core.admin.model.CatalogProperties.builder(
                        externalCatalogEntity.getPropertiesAsMap().get(DEFAULT_BASE_LOCATION_KEY))
                    .putAll(externalCatalogEntity.getPropertiesAsMap())
                    .build())
            .setCreateTimestamp(externalCatalogEntity.getCreateTimestamp())
            .setLastUpdateTimestamp(externalCatalogEntity.getLastUpdateTimestamp())
            .setEntityVersion(externalCatalogEntity.getEntityVersion())
            .setStorageConfigInfo(storageConfigModelForFederatedCatalog)
            .setConnectionConfigInfo(connectionConfigInfo)
            .build();

    catalogEntity =
        adminService.createCatalog(
            new CreateCatalogRequest(
                new CatalogEntity.Builder()
                    .setName(CATALOG_NAME)
                    .setCatalogType("INTERNAL")
                    .setDefaultBaseLocation(storageLocation)
                    .setStorageConfigurationInfo(realmConfig, storageConfigModel, storageLocation)
                    .build()
                    .asCatalog(serviceIdentityProvider)));
    federatedCatalogEntity = adminService.createCatalog(new CreateCatalogRequest(externalCatalog));

    initBaseCatalog();

    PrincipalWithCredentials principal =
        adminService.createPrincipal(new PrincipalEntity.Builder().setName(PRINCIPAL_NAME).build());
    principalEntity =
        rotateAndRefreshPrincipal(
            metaStoreManager, PRINCIPAL_NAME, principal.getCredentials(), polarisContext);

    // Pre-create the principal roles and catalog roles without any grants on securables, but
    // assign both principal roles to the principal, then CATALOG_ROLE1 to PRINCIPAL_ROLE1,
    // CATALOG_ROLE2 to PRINCIPAL_ROLE2, and CATALOG_ROLE_SHARED to both.
    adminService.createPrincipalRole(
        new PrincipalRoleEntity.Builder().setName(PRINCIPAL_ROLE1).build());
    adminService.createPrincipalRole(
        new PrincipalRoleEntity.Builder().setName(PRINCIPAL_ROLE2).build());
    adminService.createCatalogRole(
        CATALOG_NAME, new CatalogRoleEntity.Builder().setName(CATALOG_ROLE1).build());
    adminService.createCatalogRole(
        CATALOG_NAME, new CatalogRoleEntity.Builder().setName(CATALOG_ROLE2).build());
    adminService.createCatalogRole(
        CATALOG_NAME, new CatalogRoleEntity.Builder().setName(CATALOG_ROLE_SHARED).build());
    adminService.createCatalogRole(
        FEDERATED_CATALOG_NAME, new CatalogRoleEntity.Builder().setName(CATALOG_ROLE1).build());
    adminService.createCatalogRole(
        FEDERATED_CATALOG_NAME, new CatalogRoleEntity.Builder().setName(CATALOG_ROLE2).build());

    assertSuccess(adminService.assignPrincipalRole(PRINCIPAL_NAME, PRINCIPAL_ROLE1));
    assertSuccess(adminService.assignPrincipalRole(PRINCIPAL_NAME, PRINCIPAL_ROLE2));

    assertSuccess(
        adminService.assignCatalogRoleToPrincipalRole(
            PRINCIPAL_ROLE1, CATALOG_NAME, CATALOG_ROLE1));
    assertSuccess(
        adminService.assignCatalogRoleToPrincipalRole(
            PRINCIPAL_ROLE2, CATALOG_NAME, CATALOG_ROLE2));
    assertSuccess(
        adminService.assignCatalogRoleToPrincipalRole(
            PRINCIPAL_ROLE1, CATALOG_NAME, CATALOG_ROLE_SHARED));
    assertSuccess(
        adminService.assignCatalogRoleToPrincipalRole(
            PRINCIPAL_ROLE2, CATALOG_NAME, CATALOG_ROLE_SHARED));
    assertSuccess(
        adminService.assignCatalogRoleToPrincipalRole(
            PRINCIPAL_ROLE1, FEDERATED_CATALOG_NAME, CATALOG_ROLE1));
    assertSuccess(
        adminService.assignCatalogRoleToPrincipalRole(
            PRINCIPAL_ROLE2, FEDERATED_CATALOG_NAME, CATALOG_ROLE2));

    // Do some shared setup with non-authz-aware baseCatalog.
    baseCatalog.createNamespace(NS1);
    baseCatalog.createNamespace(NS2);
    baseCatalog.createNamespace(NS1A);
    baseCatalog.createNamespace(NS1AA);
    baseCatalog.createNamespace(NS1B);

    baseCatalog.buildTable(TABLE_NS1_1, SCHEMA).create();
    baseCatalog.buildTable(TABLE_NS1A_1, SCHEMA).create();
    baseCatalog.buildTable(TABLE_NS1A_2, SCHEMA).create();
    baseCatalog.buildTable(TABLE_NS1B_1, SCHEMA).create();
    baseCatalog.buildTable(TABLE_NS2_1, SCHEMA).create();

    genericTableCatalog.createGenericTable(
        TABLE_NS1_1_GENERIC, "format", "file:///tmp/test_table", "doc", Map.of());

    policyCatalog.createPolicy(
        POLICY_NS1_1,
        PredefinedPolicyTypes.DATA_COMPACTION.getName(),
        "test_policy",
        "{\"enable\": false}");

    baseCatalog
        .buildView(VIEW_NS1_1)
        .withSchema(SCHEMA)
        .withDefaultNamespace(NS1)
        .withQuery("spark", VIEW_QUERY)
        .create();
    baseCatalog
        .buildView(VIEW_NS1A_1)
        .withSchema(SCHEMA)
        .withDefaultNamespace(NS1)
        .withQuery("spark", VIEW_QUERY)
        .create();
    baseCatalog
        .buildView(VIEW_NS1A_2)
        .withSchema(SCHEMA)
        .withDefaultNamespace(NS1)
        .withQuery("spark", VIEW_QUERY)
        .create();
    baseCatalog
        .buildView(VIEW_NS1B_1)
        .withSchema(SCHEMA)
        .withDefaultNamespace(NS1)
        .withQuery("spark", VIEW_QUERY)
        .create();
    baseCatalog
        .buildView(VIEW_NS2_1)
        .withSchema(SCHEMA)
        .withDefaultNamespace(NS1)
        .withQuery("spark", VIEW_QUERY)
        .create();
  }

  @AfterEach
  public void after() {
    try {
      if (this.baseCatalog != null) {
        try {
          this.baseCatalog.close();
          this.baseCatalog = null;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    } finally {
      metaStoreManager.purge(polarisContext);
    }
  }

  protected static void assertSuccess(BaseResult result) {
    Assertions.assertThat(result.isSuccess()).isTrue();
  }

  protected @Nonnull SecurityContext securityContext(PolarisPrincipal p) {
    SecurityContext securityContext = Mockito.mock(SecurityContext.class);
    Mockito.when(securityContext.getUserPrincipal()).thenReturn(p);
    Set<String> principalRoleNames = loadPrincipalRolesNames(p);
    Mockito.when(securityContext.isUserInRole(Mockito.anyString()))
        .thenAnswer(invocation -> principalRoleNames.contains(invocation.getArgument(0)));
    return securityContext;
  }

  protected @Nonnull Set<String> loadPrincipalRolesNames(PolarisPrincipal p) {
    PolarisBaseEntity principal =
        metaStoreManager
            .findPrincipalByName(callContext.getPolarisCallContext(), p.getName())
            .orElseThrow();
    return metaStoreManager
        .loadGrantsToGrantee(callContext.getPolarisCallContext(), principal)
        .getGrantRecords()
        .stream()
        .filter(gr -> gr.getPrivilegeCode() == PolarisPrivilege.PRINCIPAL_ROLE_USAGE.getCode())
        .map(
            gr ->
                metaStoreManager.loadEntity(
                    callContext.getPolarisCallContext(),
                    0L,
                    gr.getSecurableId(),
                    PolarisEntityType.PRINCIPAL_ROLE))
        .map(EntityResult::getEntity)
        .map(PolarisBaseEntity::getName)
        .collect(Collectors.toSet());
  }

  protected @Nonnull PrincipalEntity rotateAndRefreshPrincipal(
      PolarisMetaStoreManager metaStoreManager,
      String principalName,
      PrincipalWithCredentialsCredentials credentials,
      PolarisCallContext polarisContext) {
    PrincipalEntity principal =
        metaStoreManager.findPrincipalByName(polarisContext, principalName).orElseThrow();
    metaStoreManager.rotatePrincipalSecrets(
        callContext.getPolarisCallContext(),
        credentials.getClientId(),
        principal.getId(),
        false,
        credentials.getClientSecret()); // This should actually be the secret's hash
    return metaStoreManager.findPrincipalByName(polarisContext, principalName).orElseThrow();
  }

  /**
   * This baseCatalog is used for setup rather than being the test target under a wrapper instance;
   * we set up this baseCatalog with a PolarisPassthroughResolutionView to allow it to circumvent
   * the "authorized" resolution set of entities used by wrapper instances, allowing it to resolve
   * all entities in the underlying metaStoreManager at once.
   */
  private void initBaseCatalog() {
    if (this.baseCatalog != null) {
      try {
        this.baseCatalog.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    SecurityContext securityContext = Mockito.mock(SecurityContext.class);
    Mockito.when(securityContext.getUserPrincipal()).thenReturn(authenticatedRoot);
    Mockito.when(securityContext.isUserInRole(Mockito.anyString())).thenReturn(true);
    PolarisPassthroughResolutionView passthroughView =
        new PolarisPassthroughResolutionView(
            resolutionManifestFactory, securityContext, CATALOG_NAME);
    this.baseCatalog =
        new IcebergCatalog(
            diagServices,
            resolverFactory,
            metaStoreManager,
            callContext,
            passthroughView,
            securityContext,
            Mockito.mock(),
            fileIOFactory,
            polarisEventListener);
    this.baseCatalog.initialize(
        CATALOG_NAME,
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));
    this.genericTableCatalog =
        new PolarisGenericTableCatalog(metaStoreManager, callContext, passthroughView);
    this.genericTableCatalog.initialize(CATALOG_NAME, ImmutableMap.of());
    this.policyCatalog = new PolicyCatalog(metaStoreManager, callContext, passthroughView);
  }

  @Alternative
  @RequestScoped
  public static class TestPolarisCallContextCatalogFactory
      extends PolarisCallContextCatalogFactory {

    @SuppressWarnings("unused") // Required by CDI
    protected TestPolarisCallContextCatalogFactory() {
      this(null, null, null, null, null, null, null);
    }

    @Inject
    public TestPolarisCallContextCatalogFactory(
        PolarisDiagnostics diagnostics,
        StorageCredentialCache storageCredentialCache,
        ResolverFactory resolverFactory,
        MetaStoreManagerFactory metaStoreManagerFactory,
        TaskExecutor taskExecutor,
        FileIOFactory fileIOFactory,
        PolarisEventListener polarisEventListener) {
      super(
          diagnostics,
          resolverFactory,
          metaStoreManagerFactory,
          taskExecutor,
          fileIOFactory,
          polarisEventListener);
    }

    @Override
    public Catalog createCallContextCatalog(
        CallContext context,
        PolarisPrincipal polarisPrincipal,
        SecurityContext securityContext,
        final PolarisResolutionManifest resolvedManifest) {
      // This depends on the BasePolarisCatalog allowing calling initialize multiple times
      // to override the previous config.
      Catalog catalog =
          super.createCallContextCatalog(
              context, polarisPrincipal, securityContext, resolvedManifest);
      catalog.initialize(
          CATALOG_NAME,
          ImmutableMap.of(
              CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));
      return catalog;
    }
  }

  /**
   * Tests each "sufficient" privilege individually by invoking {@code grantAction} for each set of
   * privileges, running the action being tested, revoking after each test set, and also ensuring
   * that the request fails after each revocation.
   *
   * @param sufficientPrivileges each set of concurrent privileges expected to be sufficient
   *     together.
   * @param action The operation being tested; could also be multiple operations that should all
   *     succeed with the sufficient privilege
   * @param cleanupAction If non-null, additional action to run to "undo" a previous success action
   *     in case the action has side effects. Called before revoking the sufficient privilege;
   *     either the cleanup privileges must be latent, or the cleanup action could be run with
   *     PRINCIPAL_ROLE2 while runnint {@code action} with PRINCIPAL_ROLE1.
   * @param principalName the name expected to appear in forbidden errors
   * @param grantAction the grantPrivilege action to use for each test privilege that will apply the
   *     privilege to whatever context is used in the {@code action}
   * @param revokeAction the revokePrivilege action to clean up after each granted test privilege
   */
  protected void doTestSufficientPrivilegeSets(
      List<Set<PolarisPrivilege>> sufficientPrivileges,
      Runnable action,
      Runnable cleanupAction,
      String principalName,
      Function<PolarisPrivilege, PrivilegeResult> grantAction,
      Function<PolarisPrivilege, PrivilegeResult> revokeAction) {
    for (Set<PolarisPrivilege> privilegeSet : sufficientPrivileges) {
      for (PolarisPrivilege privilege : privilegeSet) {
        // Grant the single privilege at a catalog level to cascade to all objects.
        assertSuccess(grantAction.apply(privilege));
      }

      // Should run without issues.
      try {
        action.run();
      } catch (Throwable t) {
        Assertions.fail(
            String.format(
                "Expected success with sufficientPrivileges '%s', got throwable instead.",
                privilegeSet),
            t);
      }
      if (cleanupAction != null) {
        try {
          cleanupAction.run();
        } catch (Throwable t) {
          Assertions.fail(
              String.format(
                  "Running cleanupAction with sufficientPrivileges '%s', got throwable.",
                  privilegeSet),
              t);
        }
      }

      if (privilegeSet.size() > 1) {
        // Knockout testing - Revoke single privileges and the same action should throw
        // NotAuthorizedException.
        for (PolarisPrivilege privilege : privilegeSet) {
          assertSuccess(revokeAction.apply(privilege));

          try {
            Assertions.assertThatThrownBy(() -> action.run())
                .isInstanceOf(ForbiddenException.class)
                .hasMessageContaining(principalName)
                .hasMessageContaining("is not authorized");
          } catch (Throwable t) {
            Assertions.fail(
                String.format(
                    "Expected failure after revoking sufficientPrivilege '%s' from set '%s'",
                    privilege, privilegeSet),
                t);
          }

          // Grant the single privilege at a catalog level to cascade to all objects.
          assertSuccess(grantAction.apply(privilege));
        }
      }

      // Now remove all the privileges
      for (PolarisPrivilege privilege : privilegeSet) {
        assertSuccess(revokeAction.apply(privilege));
      }
      try {
        Assertions.assertThatThrownBy(() -> action.run())
            .isInstanceOf(ForbiddenException.class)
            .hasMessageContaining(principalName)
            .hasMessageContaining("is not authorized");
      } catch (Throwable t) {
        Assertions.fail(
            String.format(
                "Expected failure after revoking all sufficientPrivileges '%s'", privilegeSet),
            t);
      }
    }
  }

  /**
   * Tests each "insufficient" privilege individually using CATALOG_ROLE1 by granting at the
   * CATALOG_NAME level, ensuring the action fails, then revoking after each test case.
   */
  protected void doTestInsufficientPrivileges(
      List<PolarisPrivilege> insufficientPrivileges,
      String principalName,
      Runnable action,
      Function<PolarisPrivilege, PrivilegeResult> grantAction,
      Function<PolarisPrivilege, PrivilegeResult> revokeAction) {
    doTestInsufficientPrivilegeSets(
        insufficientPrivileges.stream().map(Set::of).toList(),
        principalName,
        action,
        grantAction,
        revokeAction);
  }

  /**
   * Tests each "insufficient" privilege individually using CATALOG_ROLE1 by granting at the
   * CATALOG_NAME level, ensuring the action fails, then revoking after each test case.
   */
  protected void doTestInsufficientPrivilegeSets(
      List<Set<PolarisPrivilege>> insufficientPrivilegeSets,
      String principalName,
      Runnable action,
      Function<PolarisPrivilege, PrivilegeResult> grantAction,
      Function<PolarisPrivilege, PrivilegeResult> revokeAction) {
    for (Set<PolarisPrivilege> privilegeSet : insufficientPrivilegeSets) {
      for (PolarisPrivilege privilege : privilegeSet) {
        // Grant the single privilege at a catalog level to cascade to all objects.
        assertSuccess(grantAction.apply(privilege));

        // Should be insufficient
        try {
          Assertions.assertThatThrownBy(action::run)
              .isInstanceOf(ForbiddenException.class)
              .hasMessageContaining(principalName)
              .hasMessageContaining("is not authorized");
        } catch (Throwable t) {
          Assertions.fail(
              String.format("Expected failure with insufficientPrivilege '%s'", privilege), t);
        }

        // Revoking only matters in case there are some multi-privilege actions being tested with
        // only granting individual privileges in isolation.
        assertSuccess(revokeAction.apply(privilege));
      }
    }
  }
}
