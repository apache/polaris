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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.polaris.core.admin.model.AddGrantRequest;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.GrantResources;
import org.apache.polaris.core.admin.model.NamespaceGrant;
import org.apache.polaris.core.admin.model.NamespacePrivilege;
import org.apache.polaris.core.admin.model.RevokeGrantRequest;
import org.apache.polaris.core.admin.model.SemanticModelGrant;
import org.apache.polaris.core.admin.model.SemanticModelPrivilege;
import org.apache.polaris.core.auth.AuthorizationDecision;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.semantic.SemanticModelEntity;
import org.apache.polaris.core.semantic.exceptions.NoSuchSemanticModelException;
import org.apache.polaris.service.Profiles;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@QuarkusTest
@TestProfile(Profiles.PolarisAuthzBaseProfile.class)
class SemanticModelGrantAuthzTest extends PolarisAuthzTestBase {
  private static final TableIdentifier MODEL = TableIdentifier.of(NS1, "model");
  private static final TableIdentifier OTHER_MODEL = TableIdentifier.of(NS1, "other-model");

  @BeforeEach
  void createModels() {
    var namespace =
        metaStoreManager
            .readEntityByName(
                polarisContext,
                List.of(catalogEntity),
                PolarisEntityType.NAMESPACE,
                PolarisEntitySubType.NULL_SUBTYPE,
                NS1.level(0))
            .getEntity();
    for (TableIdentifier identifier : List.of(MODEL, OTHER_MODEL)) {
      var model =
          new SemanticModelEntity.Builder(NS1, identifier.name())
              .setId(metaStoreManager.generateNewEntityId(polarisContext).getId())
              .setCreateTimestamp(System.currentTimeMillis())
              .setCatalogId(catalogEntity.getId())
              .setParentId(namespace.getId())
              .setSpecVersion("0.1.1")
              .setContent("[]")
              .build();
      assertSuccess(
          metaStoreManager.createEntityIfNotExists(
              polarisContext, List.of(catalogEntity, namespace), model));
    }
  }

  private PolarisAdminService caller() {
    return caller(polarisAuthorizer);
  }

  private PolarisAdminService caller(PolarisAuthorizer authorizer) {
    PolarisPrincipal principal =
        PolarisPrincipal.of(
            principalEntity.getName(),
            Map.of(PolarisPrincipal.PRINCIPAL_ENTITY_ATTRIBUTE_KEY, principalEntity),
            Set.of(PRINCIPAL_ROLE1));
    return PolarisAdminServiceTestSupport.newAdminService(
        callContext,
        resolutionManifestFactory,
        metaStoreManager,
        userSecretsManager,
        serviceIdentityProvider,
        principal,
        authorizer,
        reservedProperties);
  }

  @TestFactory
  Stream<DynamicNode> grantRequiresManageGrants() {
    return authzTestsBuilder("grantPrivilegeOnSemanticModelToRole")
        .action(
            () ->
                caller()
                    .grantPrivilegeOnSemanticModelToRole(
                        CATALOG_NAME, CATALOG_ROLE2, MODEL, PolarisPrivilege.SEMANTIC_MODEL_READ))
        .cleanupAction(
            () ->
                newRootAdminService()
                    .revokePrivilegeOnSemanticModelFromRole(
                        CATALOG_NAME, CATALOG_ROLE2, MODEL, PolarisPrivilege.SEMANTIC_MODEL_READ))
        .shouldPassWith(PolarisPrivilege.SEMANTIC_MODEL_MANAGE_GRANTS_ON_SECURABLE)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> revokeRequiresManageGrantsOnModelAndRole() {
    return authzTestsBuilder("revokePrivilegeOnSemanticModelFromRole")
        .action(
            () -> {
              assertSuccess(
                  newRootAdminService()
                      .grantPrivilegeOnSemanticModelToRole(
                          CATALOG_NAME,
                          CATALOG_ROLE2,
                          MODEL,
                          PolarisPrivilege.SEMANTIC_MODEL_READ));
              caller()
                  .revokePrivilegeOnSemanticModelFromRole(
                      CATALOG_NAME, CATALOG_ROLE2, MODEL, PolarisPrivilege.SEMANTIC_MODEL_READ);
            })
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .shouldPassWith(
            PolarisPrivilege.SEMANTIC_MODEL_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .createTests();
  }

  @Test
  void modelGrantManagerCannotManageSiblingModel() {
    assertSuccess(
        newRootAdminService()
            .grantPrivilegeOnSemanticModelToRole(
                CATALOG_NAME,
                CATALOG_ROLE1,
                MODEL,
                PolarisPrivilege.SEMANTIC_MODEL_MANAGE_GRANTS_ON_SECURABLE));
    assertSuccess(
        caller()
            .grantPrivilegeOnSemanticModelToRole(
                CATALOG_NAME, CATALOG_ROLE2, MODEL, PolarisPrivilege.SEMANTIC_MODEL_READ));
    assertThatThrownBy(
            () ->
                caller()
                    .grantPrivilegeOnSemanticModelToRole(
                        CATALOG_NAME,
                        CATALOG_ROLE2,
                        OTHER_MODEL,
                        PolarisPrivilege.SEMANTIC_MODEL_READ))
        .isInstanceOf(ForbiddenException.class);
  }

  private PolarisAuthorizer externalAuthorizer(AuthorizationDecision decision) {
    PolarisAuthorizer authorizer = mock(PolarisAuthorizer.class);
    doAnswer(
            invocation -> {
              AuthorizationState state = invocation.getArgument(0);
              state.getResolutionManifest().resolveAll();
              return null;
            })
        .when(authorizer)
        .resolveAuthorizationInputs(any(), any());
    when(authorizer.authorize(any(), any())).thenReturn(decision);
    return authorizer;
  }

  @Test
  void authorizedGrantReportsMissingResources() {
    // An external authorizer can authorize by resource name even when resolution fails.
    PolarisAuthorizer authorizer = externalAuthorizer(AuthorizationDecision.allow());
    PolarisAdminService service = caller(authorizer);
    assertThatThrownBy(
            () ->
                service.grantPrivilegeOnSemanticModelToRole(
                    CATALOG_NAME,
                    CATALOG_ROLE2,
                    TableIdentifier.of(NS1, "missing"),
                    PolarisPrivilege.SEMANTIC_MODEL_READ))
        .isInstanceOf(NoSuchSemanticModelException.class);
    assertThatThrownBy(
            () ->
                service.grantPrivilegeOnSemanticModelToRole(
                    CATALOG_NAME, "missing-role", MODEL, PolarisPrivilege.SEMANTIC_MODEL_READ))
        .isInstanceOf(NotFoundException.class);
    assertThatThrownBy(
            () ->
                service.grantPrivilegeOnSemanticModelToRole(
                    "missing-catalog", CATALOG_ROLE2, MODEL, PolarisPrivilege.SEMANTIC_MODEL_READ))
        .isInstanceOf(NotFoundException.class);
    assertThatThrownBy(
            () ->
                service.grantPrivilegeOnSemanticModelToRole(
                    CATALOG_NAME,
                    CATALOG_ROLE2,
                    TableIdentifier.of("missing-ns", "model"),
                    PolarisPrivilege.SEMANTIC_MODEL_READ))
        .isInstanceOf(NoSuchSemanticModelException.class);
    verify(authorizer, times(4)).authorize(any(), any());
    assertThat(newRootAdminService().listGrantsForCatalogRole(CATALOG_NAME, CATALOG_ROLE2))
        .isEmpty();
  }

  static Stream<Arguments> grantOperationTargets() {
    return Stream.of(false, true)
        .flatMap(
            revoke ->
                Stream.of(
                    Arguments.of(revoke, CATALOG_NAME, CATALOG_ROLE2, MODEL),
                    Arguments.of(revoke, "missing-catalog", CATALOG_ROLE2, MODEL),
                    Arguments.of(revoke, CATALOG_NAME, "missing-role", MODEL),
                    Arguments.of(
                        revoke,
                        CATALOG_NAME,
                        CATALOG_ROLE2,
                        TableIdentifier.of("missing-ns", "model")),
                    Arguments.of(
                        revoke,
                        CATALOG_NAME,
                        CATALOG_ROLE2,
                        TableIdentifier.of(NS1, "missing-model"))));
  }

  @ParameterizedTest
  @MethodSource("grantOperationTargets")
  void deniedGrantOperationsDoNotReportMissingResources(
      boolean revoke, String catalogName, String roleName, TableIdentifier identifier) {
    PolarisAuthorizer authorizer = externalAuthorizer(AuthorizationDecision.deny("Not authorized"));
    PolarisAdminService service = caller(authorizer);
    assertThatThrownBy(
            () -> {
              if (revoke) {
                service.revokePrivilegeOnSemanticModelFromRole(
                    catalogName, roleName, identifier, PolarisPrivilege.SEMANTIC_MODEL_READ);
              } else {
                service.grantPrivilegeOnSemanticModelToRole(
                    catalogName, roleName, identifier, PolarisPrivilege.SEMANTIC_MODEL_READ);
              }
            })
        .isInstanceOf(ForbiddenException.class)
        .hasMessage("Not authorized");
    verify(authorizer).authorize(any(), any());
  }

  @ParameterizedTest
  @MethodSource("grantOperationTargets")
  void rbacDeniesGrantOperationsWithoutPrivileges(
      boolean revoke, String catalogName, String roleName, TableIdentifier identifier) {
    assertThatThrownBy(() -> changeGrant(caller(), revoke, catalogName, roleName, identifier))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageNotContaining("missing-");
  }

  static Stream<Arguments> missingGrantTargets() {
    return Stream.of(false, true)
        .flatMap(
            revoke ->
                Stream.of(
                    Arguments.of(
                        revoke, "missing-catalog", CATALOG_ROLE2, MODEL, ForbiddenException.class),
                    Arguments.of(
                        revoke, CATALOG_NAME, "missing-role", MODEL, NotFoundException.class),
                    Arguments.of(
                        revoke,
                        CATALOG_NAME,
                        CATALOG_ROLE2,
                        TableIdentifier.of("missing-ns", "model"),
                        NoSuchSemanticModelException.class),
                    Arguments.of(
                        revoke,
                        CATALOG_NAME,
                        CATALOG_ROLE2,
                        TableIdentifier.of(NS1, "missing-model"),
                        NoSuchSemanticModelException.class),
                    Arguments.of(
                        revoke,
                        CATALOG_NAME,
                        "missing-role",
                        TableIdentifier.of(NS1, "missing-model"),
                        NoSuchSemanticModelException.class)));
  }

  @ParameterizedTest
  @MethodSource("missingGrantTargets")
  void rbacRootChecksGrantsBeforeReportingMissingTargets(
      boolean revoke,
      String catalogName,
      String roleName,
      TableIdentifier identifier,
      Class<? extends Exception> expectedException) {
    assertThatThrownBy(
            () -> changeGrant(newRootAdminService(), revoke, catalogName, roleName, identifier))
        .isInstanceOf(expectedException);
    assertThat(newRootAdminService().listGrantsForCatalogRole(CATALOG_NAME, CATALOG_ROLE2))
        .isEmpty();
  }

  @ParameterizedTest
  @MethodSource("missingGrantTargets")
  void rbacCatalogAdminReportsOnlyMissingTargetsWithinItsCatalog(
      boolean revoke,
      String catalogName,
      String roleName,
      TableIdentifier identifier,
      Class<? extends Exception> expectedException) {
    assertSuccess(
        newRootAdminService()
            .grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE1, PolarisPrivilege.CATALOG_MANAGE_ACCESS));
    assertThatThrownBy(() -> changeGrant(caller(), revoke, catalogName, roleName, identifier))
        .isInstanceOf(
            CATALOG_NAME.equals(catalogName) ? expectedException : ForbiddenException.class);
    assertThat(newRootAdminService().listGrantsForCatalogRole(CATALOG_NAME, CATALOG_ROLE2))
        .isEmpty();
  }

  static Stream<Arguments> missingDescendants() {
    return Stream.of(false, true)
        .flatMap(
            revoke ->
                Stream.of(
                    Arguments.of(revoke, TableIdentifier.of(NS1, "missing-model")),
                    Arguments.of(revoke, TableIdentifier.of(NS1A, "missing-model")),
                    Arguments.of(
                        revoke,
                        TableIdentifier.of(Namespace.of(NS1.level(0), "missing-ns"), "model"))));
  }

  @ParameterizedTest
  @MethodSource("missingDescendants")
  void rbacAuthorizesMissingDescendantsWithNamespaceGrants(
      boolean revoke, TableIdentifier identifier) {
    assertSuccess(
        newRootAdminService()
            .grantPrivilegeOnNamespaceToRole(
                CATALOG_NAME,
                CATALOG_ROLE1,
                NS1,
                PolarisPrivilege.SEMANTIC_MODEL_MANAGE_GRANTS_ON_SECURABLE));
    if (revoke) {
      // The model's namespace grant must not satisfy the separate grantee-side requirement.
      assertThatThrownBy(() -> changeGrant(caller(), true, CATALOG_NAME, CATALOG_ROLE2, identifier))
          .isInstanceOf(ForbiddenException.class);
      assertSuccess(
          newRootAdminService()
              .grantPrivilegeOnCatalogToRole(
                  CATALOG_NAME,
                  CATALOG_ROLE1,
                  PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE));
    }
    assertThatThrownBy(() -> changeGrant(caller(), revoke, CATALOG_NAME, CATALOG_ROLE2, identifier))
        .isInstanceOf(NoSuchSemanticModelException.class);
    assertThat(newRootAdminService().listGrantsForCatalogRole(CATALOG_NAME, CATALOG_ROLE2))
        .isEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rbacDoesNotUseSiblingNamespaceGrantsForMissingTargets(boolean revoke) {
    assertSuccess(
        newRootAdminService()
            .grantPrivilegeOnNamespaceToRole(
                CATALOG_NAME,
                CATALOG_ROLE1,
                NS2,
                PolarisPrivilege.SEMANTIC_MODEL_MANAGE_GRANTS_ON_SECURABLE));
    assertThatThrownBy(
            () ->
                changeGrant(
                    caller(),
                    revoke,
                    CATALOG_NAME,
                    CATALOG_ROLE2,
                    TableIdentifier.of(NS1, "missing-model")))
        .isInstanceOf(ForbiddenException.class);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rbacModelGrantManagerHandlesMissingRecipient(boolean revoke) {
    assertSuccess(
        newRootAdminService()
            .grantPrivilegeOnSemanticModelToRole(
                CATALOG_NAME,
                CATALOG_ROLE1,
                MODEL,
                PolarisPrivilege.SEMANTIC_MODEL_MANAGE_GRANTS_ON_SECURABLE));
    assertThatThrownBy(() -> changeGrant(caller(), revoke, CATALOG_NAME, "missing-role", MODEL))
        .isInstanceOf(revoke ? ForbiddenException.class : NotFoundException.class);
  }

  private static void changeGrant(
      PolarisAdminService service,
      boolean revoke,
      String catalogName,
      String roleName,
      TableIdentifier identifier) {
    if (revoke) {
      service.revokePrivilegeOnSemanticModelFromRole(
          catalogName, roleName, identifier, PolarisPrivilege.SEMANTIC_MODEL_READ);
    } else {
      service.grantPrivilegeOnSemanticModelToRole(
          catalogName, roleName, identifier, PolarisPrivilege.SEMANTIC_MODEL_READ);
    }
  }

  static Stream<GrantResource> scopedGrants() {
    Stream<GrantResource> inheritedGrants =
        Stream.of(PolarisPrivilege.values())
            .filter(privilege -> privilege.name().startsWith("SEMANTIC_MODEL_"))
            .flatMap(
                privilege ->
                    Stream.of(
                        new NamespaceGrant(
                            List.of(NS1.levels()),
                            NamespacePrivilege.valueOf(privilege.name()),
                            GrantResource.TypeEnum.NAMESPACE),
                        new CatalogGrant(
                            CatalogPrivilege.valueOf(privilege.name()),
                            GrantResource.TypeEnum.CATALOG)));
    return Stream.concat(
        inheritedGrants,
        Stream.of(SemanticModelPrivilege.values())
            .map(
                privilege ->
                    new SemanticModelGrant(
                        List.of(NS1.levels()),
                        MODEL.name(),
                        privilege,
                        GrantResource.TypeEnum.SEMANTIC_MODEL)));
  }

  @ParameterizedTest
  @MethodSource("scopedGrants")
  void grantsRoundTripThroughManagementApi(GrantResource grant) throws Exception {
    var api =
        new PolarisServiceImpl(
            realmConfig, reservedProperties, newRootAdminService(), serviceIdentityProvider);
    ObjectMapper mapper = new ObjectMapper();
    AddGrantRequest request =
        mapper.readValue(
            mapper.writeValueAsString(new AddGrantRequest(grant)), AddGrantRequest.class);
    assertThat(request.getGrant()).isEqualTo(grant);
    try (Response response =
        api.addGrantToCatalogRole(CATALOG_NAME, CATALOG_ROLE2, request, null, null)) {
      assertThat(response.getStatus()).isEqualTo(201);
    }
    try (Response response =
        api.listGrantsForCatalogRole(CATALOG_NAME, CATALOG_ROLE2, null, null)) {
      assertThat(((GrantResources) response.getEntity()).getGrants()).contains(grant);
    }
    try (Response response =
        api.revokeGrantFromCatalogRole(
            CATALOG_NAME, CATALOG_ROLE2, false, new RevokeGrantRequest(grant), null, null)) {
      assertThat(response.getStatus()).isEqualTo(201);
    }
    assertThat(newRootAdminService().listGrantsForCatalogRole(CATALOG_NAME, CATALOG_ROLE2))
        .isEmpty();
  }
}
