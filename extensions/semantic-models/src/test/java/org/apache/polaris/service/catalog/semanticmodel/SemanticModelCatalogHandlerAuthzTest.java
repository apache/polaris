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
package org.apache.polaris.service.catalog.semanticmodel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.auth.AuthorizationDecision;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisAuthorizerImpl;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.SingleTargetAuthorizationIntent;
import org.apache.polaris.core.entity.CatalogRoleEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.semantic.exceptions.NoSuchSemanticModelException;
import org.apache.polaris.core.semantic.exceptions.SemanticModelVersionMismatchException;
import org.apache.polaris.service.catalog.common.PolarisSecurableMapper;
import org.apache.polaris.service.catalog.semanticmodel.types.UpdateSemanticModelRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

/** Exercises semantic-model privileges with persisted grants and the built-in authorizer. */
class SemanticModelCatalogHandlerAuthzTest extends AbstractSemanticModelCatalogHandlerTest {

  private PolarisEntity catalog;
  private PolarisEntity namespace;
  private PolarisEntity role;
  private PolarisPrincipal caller;

  @BeforeEach
  void seedModel() {
    passthroughHandler().createSemanticModel(NS, createRequest("m1", modelJson("ns1.t1")));
    catalog = read(null, PolarisEntityType.CATALOG, CATALOG_NAME);
    namespace = read(List.of(catalog), PolarisEntityType.NAMESPACE, NS.toString());
    var context = services.newCallContext().getPolarisCallContext();
    var principal =
        services
            .metaStoreManager()
            .createPrincipal(context, new PrincipalEntity.Builder().setName("model-user").build())
            .getPrincipal();
    var principalRole =
        new PrincipalRoleEntity.Builder()
            .setName("model-users")
            .setId(services.metaStoreManager().generateNewEntityId(context).getId())
            .setCreateTimestamp(System.currentTimeMillis())
            .build();
    assertSuccess(
        services.metaStoreManager().createEntityIfNotExists(context, null, principalRole));
    role =
        new CatalogRoleEntity.Builder()
            .setName("model-role")
            .setId(services.metaStoreManager().generateNewEntityId(context).getId())
            .setCreateTimestamp(System.currentTimeMillis())
            .setCatalogId(catalog.getId())
            .setParentId(catalog.getId())
            .build();
    assertSuccess(
        services
            .metaStoreManager()
            .createEntityIfNotExists(context, PolarisEntity.toCoreList(List.of(catalog)), role));
    assertSuccess(
        services
            .metaStoreManager()
            .grantUsageOnRoleToGrantee(context, null, principalRole, principal));
    assertSuccess(
        services
            .metaStoreManager()
            .grantUsageOnRoleToGrantee(context, catalog, role, principalRole));
    caller =
        PolarisPrincipal.of(
            principal.getName(),
            Map.of(PolarisPrincipal.PRINCIPAL_ENTITY_ATTRIBUTE_KEY, principal),
            Set.of(principalRole.getName()));
  }

  @Test
  void createDeniedWithoutPrivilege() {
    assertThatThrownBy(
            () ->
                enforcingHandler()
                    .createSemanticModel(NS, createRequest("m2", modelJson("ns1.t1"))))
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  void listDeniedWithoutPrivilege() {
    assertThatThrownBy(() -> enforcingHandler().listSemanticModels(NS, null, null))
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  void loadDeniedWithoutPrivilege() {
    assertThatThrownBy(() -> enforcingHandler().loadSemanticModel(identifier("m1")))
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  void updateDeniedWithoutPrivilege() {
    assertThatThrownBy(
            () ->
                enforcingHandler()
                    .updateSemanticModel(
                        identifier("m1"),
                        UpdateSemanticModelRequest.builder()
                            .setDocument(doc(modelJson("ns1.t1")))
                            .setEntityVersion("1")
                            .build()))
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  void dropDeniedWithoutPrivilege() {
    assertThatThrownBy(() -> enforcingHandler().dropSemanticModel(identifier("m1")))
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  void loadResolvesAuthorizationInputsForSemanticModel() {
    PolarisAuthorizer authorizer = mock(PolarisAuthorizer.class);
    doAnswer(
            invocation -> {
              AuthorizationState authorizationState = invocation.getArgument(0);
              authorizationState.getResolutionManifest().resolveAll();
              return null;
            })
        .when(authorizer)
        .resolveAuthorizationInputs(any(), any());
    when(authorizer.authorize(any(), any())).thenReturn(AuthorizationDecision.allow());

    handler(authorizer).loadSemanticModel(identifier("m1"));

    ArgumentCaptor<AuthorizationRequest> requestCaptor =
        ArgumentCaptor.forClass(AuthorizationRequest.class);
    verify(authorizer).resolveAuthorizationInputs(any(), requestCaptor.capture());
    assertThat(requestCaptor.getValue().intents())
        .singleElement()
        .isInstanceOfSatisfying(
            SingleTargetAuthorizationIntent.class,
            intent -> {
              assertThat(intent.operation())
                  .isEqualTo(PolarisAuthorizableOperation.LOAD_SEMANTIC_MODEL);
              assertThat(intent.target())
                  .isEqualTo(PolarisSecurableMapper.semanticModel(CATALOG_NAME, NS, "m1"));
            });
  }

  @ParameterizedTest
  @MethodSource("operationPrivileges")
  void dedicatedPrivilegesAuthorizeOnlyTheirOperations(
      String operation, PolarisPrivilege privilege, boolean allowed) {
    PolarisEntity target =
        privilege == PolarisPrivilege.SEMANTIC_MODEL_CREATE
                || privilege == PolarisPrivilege.SEMANTIC_MODEL_LIST
                || privilege == PolarisPrivilege.SEMANTIC_MODEL_FULL_METADATA
            ? namespace
            : model("m1");
    grant(target, privilege);
    if (allowed) {
      runOperation(operation);
    } else {
      assertThatThrownBy(() -> runOperation(operation)).isInstanceOf(ForbiddenException.class);
    }
  }

  static Stream<Arguments> operationPrivileges() {
    Map<String, PolarisPrivilege> required =
        Map.of(
            "create", PolarisPrivilege.SEMANTIC_MODEL_CREATE,
            "list", PolarisPrivilege.SEMANTIC_MODEL_LIST,
            "load", PolarisPrivilege.SEMANTIC_MODEL_READ,
            "update", PolarisPrivilege.SEMANTIC_MODEL_WRITE,
            "drop", PolarisPrivilege.SEMANTIC_MODEL_DROP);
    return required.entrySet().stream()
        .flatMap(
            operation ->
                Stream.of(PolarisPrivilege.values())
                    .filter(p -> p.name().startsWith("SEMANTIC_MODEL_"))
                    .map(
                        privilege ->
                            Arguments.of(
                                operation.getKey(),
                                privilege,
                                privilege == operation.getValue()
                                    || privilege == PolarisPrivilege.SEMANTIC_MODEL_FULL_METADATA
                                    || (operation.getKey().equals("list")
                                        && privilege == PolarisPrivilege.SEMANTIC_MODEL_CREATE)
                                    || (operation.getKey().equals("load")
                                        && privilege == PolarisPrivilege.SEMANTIC_MODEL_WRITE))));
  }

  @ParameterizedTest
  @MethodSource("umbrellaPrivileges")
  void umbrellaPrivilegesApplyToDescendants(PolarisPrivilege privilege, boolean catalogScope) {
    grant(catalogScope ? catalog : namespace, privilege);
    for (String operation : List.of("create", "list", "load", "update", "drop")) {
      runOperation(operation);
    }
  }

  static Stream<Arguments> umbrellaPrivileges() {
    return Stream.of(
        Arguments.of(PolarisPrivilege.SEMANTIC_MODEL_FULL_METADATA, false),
        Arguments.of(PolarisPrivilege.SEMANTIC_MODEL_FULL_METADATA, true),
        Arguments.of(PolarisPrivilege.NAMESPACE_FULL_METADATA, false),
        Arguments.of(PolarisPrivilege.CATALOG_FULL_METADATA, true),
        Arguments.of(PolarisPrivilege.CATALOG_MANAGE_METADATA, true),
        Arguments.of(PolarisPrivilege.CATALOG_MANAGE_CONTENT, true));
  }

  @ParameterizedTest
  @MethodSource("namespaceReaders")
  void namespaceAndCatalogReadersCanDiscoverModels(
      PolarisPrivilege privilege, boolean catalogScope) {
    grant(catalogScope ? catalog : namespace, privilege);
    runOperation("list");
    runOperation("load");
  }

  static Stream<Arguments> namespaceReaders() {
    return Stream.of(PolarisPrivilege.SEMANTIC_MODEL_READ, PolarisPrivilege.SEMANTIC_MODEL_WRITE)
        .flatMap(
            privilege -> Stream.of(Arguments.of(privilege, false), Arguments.of(privilege, true)));
  }

  @Test
  void readerCannotWriteOrReadSiblingModel() {
    passthroughHandler().createSemanticModel(NS, createRequest("other", modelJson("ns1.t1")));
    grant(model("m1"), PolarisPrivilege.SEMANTIC_MODEL_READ);
    // Reading a model does not require privileges on its source.
    runOperation("load");
    for (String operation : List.of("create", "list", "update", "drop")) {
      assertThatThrownBy(() -> runOperation(operation)).isInstanceOf(ForbiddenException.class);
    }
    assertThatThrownBy(() -> enforcingHandler().loadSemanticModel(identifier("other")))
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  void tablePrivilegesDoNotAuthorizeModels() {
    grant(namespace, PolarisPrivilege.TABLE_FULL_METADATA);
    assertThatThrownBy(() -> runOperation("load")).isInstanceOf(ForbiddenException.class);
  }

  @Test
  void revokingModelReadRemovesAccess() {
    PolarisEntity model = model("m1");
    grant(model, PolarisPrivilege.SEMANTIC_MODEL_READ);
    runOperation("load");
    assertSuccess(
        services
            .metaStoreManager()
            .revokePrivilegeOnSecurableFromRole(
                services.newCallContext().getPolarisCallContext(),
                role,
                PolarisEntity.toCoreList(List.of(catalog, namespace)),
                model,
                PolarisPrivilege.SEMANTIC_MODEL_READ));
    assertThatThrownBy(() -> runOperation("load")).isInstanceOf(ForbiddenException.class);
  }

  @ParameterizedTest
  @MethodSource("replacementOperations")
  void operationsKeepAuthorizedModelWhenNameIsReused(String operation, PolarisPrivilege privilege) {
    grant(model("m1"), privilege);
    long originalId = model("m1").getId();
    String original = modelJson("ns1.t1");
    String replacement = original.replace("\"m\"", "\"replacement\"");
    SemanticModelCatalogHandler owner = replacementOwner();
    PolarisAuthorizer authorizer =
        new PolarisAuthorizerImpl(services.realmConfig()) {
          private boolean replaced;

          @Override
          public AuthorizationDecision authorize(
              AuthorizationState state, AuthorizationRequest request) {
            AuthorizationDecision decision = super.authorize(state, request);
            if (decision.isAllowed() && !replaced) {
              // Interleave an authorized owner's replacement after the caller's permission check.
              replaced = true;
              owner.dropSemanticModel(identifier("m1"));
              owner.createSemanticModel(NS, createRequest("m1", replacement));
              assertThat(model("m1").getId()).isNotEqualTo(originalId);
            }
            return decision;
          }
        };
    SemanticModelCatalogHandler handler =
        ImmutableSemanticModelCatalogHandler.builder()
            .from(enforcingHandler())
            .authorizer(authorizer)
            .build();
    switch (operation) {
      case "load" ->
          assertThat(handler.loadSemanticModel(identifier("m1")).getDocument().getSemanticModel())
              .isEqualTo(original);
      case "update" ->
          assertThatThrownBy(
                  () ->
                      handler.updateSemanticModel(
                          identifier("m1"),
                          UpdateSemanticModelRequest.builder()
                              .setDocument(doc(original))
                              .setEntityVersion("1")
                              .build()))
              .isInstanceOf(SemanticModelVersionMismatchException.class);
      case "drop" ->
          assertThatThrownBy(() -> handler.dropSemanticModel(identifier("m1")))
              .isInstanceOf(NoSuchSemanticModelException.class);
      default -> throw new IllegalArgumentException(operation);
    }
    assertThat(owner.loadSemanticModel(identifier("m1")).getDocument().getSemanticModel())
        .isEqualTo(replacement);
    assertThatThrownBy(() -> runOperation(operation)).isInstanceOf(ForbiddenException.class);
  }

  static Stream<Arguments> replacementOperations() {
    return Stream.of(
        Arguments.of("load", PolarisPrivilege.SEMANTIC_MODEL_READ),
        Arguments.of("update", PolarisPrivilege.SEMANTIC_MODEL_WRITE),
        Arguments.of("drop", PolarisPrivilege.SEMANTIC_MODEL_DROP));
  }

  private SemanticModelCatalogHandler replacementOwner() {
    var context = services.newCallContext().getPolarisCallContext();
    var manager = services.metaStoreManager();
    var principal =
        manager
            .createPrincipal(context, new PrincipalEntity.Builder().setName("model-owner").build())
            .getPrincipal();
    var principalRole =
        new PrincipalRoleEntity.Builder()
            .setName("model-owners")
            .setId(manager.generateNewEntityId(context).getId())
            .setCreateTimestamp(System.currentTimeMillis())
            .build();
    assertSuccess(manager.createEntityIfNotExists(context, null, principalRole));
    var ownerRole =
        new CatalogRoleEntity.Builder()
            .setName("model-owner-role")
            .setId(manager.generateNewEntityId(context).getId())
            .setCreateTimestamp(System.currentTimeMillis())
            .setCatalogId(catalog.getId())
            .setParentId(catalog.getId())
            .build();
    assertSuccess(
        manager.createEntityIfNotExists(
            context, PolarisEntity.toCoreList(List.of(catalog)), ownerRole));
    assertSuccess(manager.grantUsageOnRoleToGrantee(context, null, principalRole, principal));
    assertSuccess(manager.grantUsageOnRoleToGrantee(context, catalog, ownerRole, principalRole));
    assertSuccess(
        manager.grantPrivilegeOnSecurableToRole(
            context,
            ownerRole,
            PolarisEntity.toCoreList(List.of(catalog)),
            namespace,
            PolarisPrivilege.SEMANTIC_MODEL_FULL_METADATA));
    return ImmutableSemanticModelCatalogHandler.builder()
        .from(enforcingHandler())
        .polarisPrincipal(
            PolarisPrincipal.of(
                principal.getName(),
                Map.of(PolarisPrincipal.PRINCIPAL_ENTITY_ATTRIBUTE_KEY, principal),
                Set.of(principalRole.getName())))
        .build();
  }

  private void runOperation(String operation) {
    SemanticModelCatalogHandler handler = enforcingHandler();
    switch (operation) {
      case "create" -> handler.createSemanticModel(NS, createRequest("m2", modelJson("ns1.t1")));
      case "list" -> handler.listSemanticModels(NS, null, null);
      case "load" -> handler.loadSemanticModel(identifier("m1"));
      case "update" ->
          handler.updateSemanticModel(
              identifier("m1"),
              UpdateSemanticModelRequest.builder()
                  .setDocument(doc(modelJson("ns1.t1")))
                  .setEntityVersion("1")
                  .build());
      case "drop" -> handler.dropSemanticModel(identifier("m1"));
      default -> throw new IllegalArgumentException(operation);
    }
  }

  private PolarisEntity read(List<PolarisEntity> parents, PolarisEntityType type, String name) {
    var result =
        services
            .metaStoreManager()
            .readEntityByName(
                services.newCallContext().getPolarisCallContext(),
                parents == null ? null : PolarisEntity.toCoreList(parents),
                type,
                PolarisEntitySubType.ANY_SUBTYPE,
                name);
    assertSuccess(result);
    return new PolarisEntity(result.getEntity());
  }

  private PolarisEntity model(String name) {
    return read(List.of(catalog, namespace), PolarisEntityType.SEMANTIC_MODEL, name);
  }

  private void grant(PolarisEntity target, PolarisPrivilege privilege) {
    List<PolarisEntity> parents =
        target == catalog || target == namespace ? List.of(catalog) : List.of(catalog, namespace);
    assertSuccess(
        services
            .metaStoreManager()
            .grantPrivilegeOnSecurableToRole(
                services.newCallContext().getPolarisCallContext(),
                role,
                PolarisEntity.toCoreList(parents),
                target,
                privilege));
  }

  private static void assertSuccess(BaseResult result) {
    assertThat(result.isSuccess()).as("%s", result).isTrue();
  }

  private SemanticModelCatalogHandler enforcingHandler() {
    return ImmutableSemanticModelCatalogHandler.builder()
        .catalogName(CATALOG_NAME)
        .polarisPrincipal(caller)
        .callContext(services.newCallContext())
        .resolutionManifestFactory(services.resolutionManifestFactory())
        .metaStoreManager(services.metaStoreManager())
        .authorizer(new PolarisAuthorizerImpl(services.realmConfig()))
        .build();
  }
}
