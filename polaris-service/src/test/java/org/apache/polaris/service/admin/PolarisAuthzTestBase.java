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

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Clock;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisConfiguration;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.PrincipalWithCredentialsCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisAuthorizerImpl;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.CatalogRoleEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.service.catalog.BasePolarisCatalog;
import org.apache.polaris.service.catalog.PolarisPassthroughResolutionView;
import org.apache.polaris.service.catalog.io.DefaultFileIOFactory;
import org.apache.polaris.service.config.DefaultConfigurationStore;
import org.apache.polaris.service.config.RealmEntityManagerFactory;
import org.apache.polaris.service.context.PolarisCallContextCatalogFactory;
import org.apache.polaris.service.persistence.InMemoryPolarisMetaStoreManagerFactory;
import org.apache.polaris.service.storage.PolarisStorageIntegrationProviderImpl;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;

/** Base class for shared test setup logic used by various Polaris authz-related tests. */
public abstract class PolarisAuthzTestBase {
  protected static final String CATALOG_NAME = "polaris-catalog";
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
  protected final PolarisAuthorizer polarisAuthorizer =
      new PolarisAuthorizerImpl(
          new DefaultConfigurationStore(
              Map.of(
                  PolarisConfiguration.ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING.key,
                  true)));

  protected BasePolarisCatalog baseCatalog;
  protected PolarisAdminService adminService;
  protected PolarisEntityManager entityManager;
  protected PolarisMetaStoreManager metaStoreManager;
  protected PolarisBaseEntity catalogEntity;
  protected PrincipalEntity principalEntity;
  protected CallContext callContext;
  protected AuthenticatedPolarisPrincipal authenticatedRoot;
  protected InMemoryPolarisMetaStoreManagerFactory metaStoreManagerFactory;

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void before() {
    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    metaStoreManagerFactory = new InMemoryPolarisMetaStoreManagerFactory();
    metaStoreManagerFactory.setStorageIntegrationProvider(
        new PolarisStorageIntegrationProviderImpl(
            Mockito::mock, () -> GoogleCredentials.create(new AccessToken("abc", new Date()))));
    RealmContext realmContext = () -> "realm";
    PolarisMetaStoreManager metaStoreManager =
        metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);

    Map<String, Object> configMap =
        Map.of(
            "ALLOW_SPECIFYING_FILE_IO_IMPL", true, "ALLOW_EXTERNAL_METADATA_FILE_LOCATION", true);
    PolarisCallContext polarisContext =
        new PolarisCallContext(
            metaStoreManagerFactory.getOrCreateSessionSupplier(realmContext).get(),
            diagServices,
            new PolarisConfigurationStore() {
              @Override
              public <T> @Nullable T getConfiguration(PolarisCallContext ctx, String configName) {
                return (T) configMap.get(configName);
              }
            },
            Clock.systemDefaultZone());
    this.entityManager = new PolarisEntityManager(metaStoreManager, new StorageCredentialCache());
    this.metaStoreManager = metaStoreManager;

    callContext = CallContext.of(realmContext, polarisContext);
    CallContext.setCurrentContext(callContext);

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

    this.authenticatedRoot = new AuthenticatedPolarisPrincipal(rootEntity, Set.of());

    this.adminService =
        new PolarisAdminService(
            callContext, entityManager, metaStoreManager, authenticatedRoot, polarisAuthorizer);

    String storageLocation = "file:///tmp/authz";
    FileStorageConfigInfo storageConfigModel =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of(storageLocation, "file:///tmp/authz"))
            .build();
    catalogEntity =
        adminService.createCatalog(
            new CatalogEntity.Builder()
                .setName(CATALOG_NAME)
                .setCatalogType("INTERNAL")
                .setDefaultBaseLocation(storageLocation)
                .setStorageConfigurationInfo(storageConfigModel, storageLocation)
                .build());

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

    adminService.assignPrincipalRole(PRINCIPAL_NAME, PRINCIPAL_ROLE1);
    adminService.assignPrincipalRole(PRINCIPAL_NAME, PRINCIPAL_ROLE2);

    adminService.assignCatalogRoleToPrincipalRole(PRINCIPAL_ROLE1, CATALOG_NAME, CATALOG_ROLE1);
    adminService.assignCatalogRoleToPrincipalRole(PRINCIPAL_ROLE2, CATALOG_NAME, CATALOG_ROLE2);
    adminService.assignCatalogRoleToPrincipalRole(
        PRINCIPAL_ROLE1, CATALOG_NAME, CATALOG_ROLE_SHARED);
    adminService.assignCatalogRoleToPrincipalRole(
        PRINCIPAL_ROLE2, CATALOG_NAME, CATALOG_ROLE_SHARED);

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
    if (this.baseCatalog != null) {
      try {
        this.baseCatalog.close();
        this.baseCatalog = null;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  protected @NotNull PrincipalEntity rotateAndRefreshPrincipal(
      PolarisMetaStoreManager metaStoreManager,
      String principalName,
      PrincipalWithCredentialsCredentials credentials,
      PolarisCallContext polarisContext) {
    PolarisMetaStoreManager.EntityResult lookupEntity =
        metaStoreManager.readEntityByName(
            callContext.getPolarisCallContext(),
            null,
            PolarisEntityType.PRINCIPAL,
            PolarisEntitySubType.NULL_SUBTYPE,
            principalName);
    metaStoreManager.rotatePrincipalSecrets(
        callContext.getPolarisCallContext(),
        credentials.getClientId(),
        lookupEntity.getEntity().getId(),
        false,
        credentials.getClientSecret()); // This should actually be the secret's hash

    return new PrincipalEntity(
        PolarisEntity.of(
            metaStoreManager
                .readEntityByName(
                    polarisContext,
                    null,
                    PolarisEntityType.PRINCIPAL,
                    PolarisEntitySubType.NULL_SUBTYPE,
                    principalName)
                .getEntity()));
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
    PolarisPassthroughResolutionView passthroughView =
        new PolarisPassthroughResolutionView(
            callContext, entityManager, authenticatedRoot, CATALOG_NAME);
    this.baseCatalog =
        new BasePolarisCatalog(
            entityManager,
            metaStoreManager,
            callContext,
            passthroughView,
            authenticatedRoot,
            Mockito.mock(),
            new DefaultFileIOFactory());
    this.baseCatalog.initialize(
        CATALOG_NAME,
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));
  }

  public class TestPolarisCallContextCatalogFactory extends PolarisCallContextCatalogFactory {
    public TestPolarisCallContextCatalogFactory() {
      super(
          new RealmEntityManagerFactory() {
            @Override
            public PolarisEntityManager getOrCreateEntityManager(RealmContext realmContext) {
              return entityManager;
            }
          },
          metaStoreManagerFactory,
          Mockito.mock(),
          new DefaultFileIOFactory());
    }

    @Override
    public Catalog createCallContextCatalog(
        CallContext context,
        AuthenticatedPolarisPrincipal authenticatedPolarisPrincipal,
        final PolarisResolutionManifest resolvedManifest) {
      // This depends on the BasePolarisCatalog allowing calling initialize multiple times
      // to override the previous config.
      Catalog catalog =
          super.createCallContextCatalog(context, authenticatedPolarisPrincipal, resolvedManifest);
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
      Function<PolarisPrivilege, Boolean> grantAction,
      Function<PolarisPrivilege, Boolean> revokeAction) {
    for (Set<PolarisPrivilege> privilegeSet : sufficientPrivileges) {
      for (PolarisPrivilege privilege : privilegeSet) {
        // Grant the single privilege at a catalog level to cascade to all objects.
        Assertions.assertThat(grantAction.apply(privilege)).isTrue();
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
          Assertions.assertThat(revokeAction.apply(privilege)).isTrue();

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
          Assertions.assertThat(grantAction.apply(privilege)).isTrue();
        }
      }

      // Now remove all the privileges
      for (PolarisPrivilege privilege : privilegeSet) {
        Assertions.assertThat(revokeAction.apply(privilege)).isTrue();
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
      Function<PolarisPrivilege, Boolean> grantAction,
      Function<PolarisPrivilege, Boolean> revokeAction) {
    for (PolarisPrivilege privilege : insufficientPrivileges) {
      // Grant the single privilege at a catalog level to cascade to all objects.
      Assertions.assertThat(grantAction.apply(privilege)).isTrue();

      // Should be insufficient
      try {
        Assertions.assertThatThrownBy(() -> action.run())
            .isInstanceOf(ForbiddenException.class)
            .hasMessageContaining(principalName)
            .hasMessageContaining("is not authorized");
      } catch (Throwable t) {
        Assertions.fail(
            String.format("Expected failure with insufficientPrivilege '%s'", privilege), t);
      }

      // Revoking only matters in case there are some multi-privilege actions being tested with
      // only granting individual privileges in isolation.
      Assertions.assertThat(revokeAction.apply(privilege)).isTrue();
    }
  }
}
