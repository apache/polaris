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
package org.apache.polaris.core.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentMatchers;

public class PolarisAuthorizerImplTest {

  @ParameterizedTest
  @EnumSource(PolarisPrivilege.class)
  void subsumingPrivilegesOf(PolarisPrivilege privilege) {
    Set<PolarisPrivilege> actual = PolarisAuthorizerImpl.subsumingPrivilegesOf(privilege);
    assertThat(actual).isNotEmpty().contains(privilege);
    Set<PolarisPrivilege> expected =
        PolarisAuthorizerImpl.SUPER_PRIVILEGES.containsKey(privilege)
            ? PolarisAuthorizerImpl.SUPER_PRIVILEGES.get(privilege)
            : EnumSet.of(privilege);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void resolveAuthorizationInputsResolvesAll() {
    PolarisAuthorizerImpl authorizer = new PolarisAuthorizerImpl(mock(RealmConfig.class));
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    AuthorizationState authzState = new AuthorizationState(manifest);
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));
    AuthorizationRequest request =
        new AuthorizationRequest(
            principal,
            List.of(
                new SingleTargetAuthorizationIntent(
                    PolarisAuthorizableOperation.GET_CATALOG,
                    PolarisSecurable.of(new PathSegment(PolarisEntityType.CATALOG, "catalog")))));

    authorizer.resolveAuthorizationInputs(authzState, request);

    verify(manifest).resolveAll();
  }

  @Test
  void authorizeUsesRootTargetForRootGrantRequestWithoutPrimaryTarget() {
    // Verify that new authorize SPI call without primary target uses root_container
    // for resolution and authorization
    PolarisAuthorizerImpl authorizer = spy(new PolarisAuthorizerImpl(mock(RealmConfig.class)));
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    AuthorizationState authzState = new AuthorizationState(manifest);
    PolarisResolvedPathWrapper rootWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisResolvedPathWrapper principalRoleWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));

    when(manifest.getResolvedRootContainerEntityAsPath()).thenReturn(rootWrapper);
    when(manifest.getResolvedTopLevelEntity("analytics-admin", PolarisEntityType.PRINCIPAL_ROLE))
        .thenReturn(principalRoleWrapper);
    when(manifest.getAllActivatedCatalogRoleAndPrincipalRoles()).thenReturn(Set.of());
    doNothing()
        .when(authorizer)
        .authorizeOrThrow(
            any(PolarisPrincipal.class),
            ArgumentMatchers.any(),
            eq(PolarisAuthorizableOperation.ADD_ROOT_GRANT_TO_PRINCIPAL_ROLE),
            ArgumentMatchers.any(),
            ArgumentMatchers.<List<PolarisResolvedPathWrapper>>any());

    AuthorizationRequest request =
        new AuthorizationRequest(
            principal,
            List.of(
                new RootPrivilegeGrantAuthorizationIntent(
                    PolarisAuthorizableOperation.ADD_ROOT_GRANT_TO_PRINCIPAL_ROLE,
                    PolarisSecurable.of(
                        new PathSegment(PolarisEntityType.PRINCIPAL_ROLE, "analytics-admin")))));

    AuthorizationDecision decision = authorizer.authorize(authzState, request);

    assertThat(decision.isAllowed()).isTrue();
    verify(authorizer)
        .authorizeOrThrow(
            eq(principal),
            eq(Set.of()),
            eq(PolarisAuthorizableOperation.ADD_ROOT_GRANT_TO_PRINCIPAL_ROLE),
            eq(List.of(rootWrapper)),
            eq(List.of(principalRoleWrapper)));
  }

  @Test
  void authorizeUsesRootTargetForListCatalogsRequestWithoutPrimaryTarget() {
    // Verify that new authorize SPI call without primary target uses root_container
    // for resolution and authorization
    PolarisAuthorizerImpl authorizer = spy(new PolarisAuthorizerImpl(mock(RealmConfig.class)));
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    AuthorizationState authzState = new AuthorizationState(manifest);
    PolarisResolvedPathWrapper rootWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));

    when(manifest.getResolvedRootContainerEntityAsPath()).thenReturn(rootWrapper);
    when(manifest.getAllActivatedCatalogRoleAndPrincipalRoles()).thenReturn(Set.of());
    doNothing()
        .when(authorizer)
        .authorizeOrThrow(
            any(PolarisPrincipal.class),
            ArgumentMatchers.any(),
            eq(PolarisAuthorizableOperation.LIST_CATALOGS),
            ArgumentMatchers.any(),
            ArgumentMatchers.<List<PolarisResolvedPathWrapper>>any());

    AuthorizationRequest request =
        new AuthorizationRequest(
            principal,
            List.of(new TargetlessAuthorizationIntent(PolarisAuthorizableOperation.LIST_CATALOGS)));

    AuthorizationDecision decision = authorizer.authorize(authzState, request);

    assertThat(decision.isAllowed()).isTrue();
    verify(authorizer)
        .authorizeOrThrow(
            eq(principal),
            eq(Set.of()),
            eq(PolarisAuthorizableOperation.LIST_CATALOGS),
            eq(List.of(rootWrapper)),
            eq(null));
  }

  @Test
  void authorizeResolvesNamespaceTargetUsingCatalog() {
    // Verify authorize call that includes Catalog name in the PolarisSecurable
    // successfully resolves the correct namespace
    PolarisAuthorizerImpl authorizer = spy(new PolarisAuthorizerImpl(mock(RealmConfig.class)));
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    AuthorizationState authzState = new AuthorizationState(manifest);
    PolarisResolvedPathWrapper namespaceWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));

    when(manifest.getResolvedPath(
            ResolvedPathKey.of(List.of("ns"), PolarisEntityType.NAMESPACE), true))
        .thenReturn(namespaceWrapper);
    when(manifest.getAllActivatedCatalogRoleAndPrincipalRoles()).thenReturn(Set.of());
    doNothing()
        .when(authorizer)
        .authorizeOrThrow(
            any(PolarisPrincipal.class),
            ArgumentMatchers.any(),
            eq(PolarisAuthorizableOperation.LIST_NAMESPACES),
            ArgumentMatchers.any(),
            ArgumentMatchers.<List<PolarisResolvedPathWrapper>>any());

    AuthorizationRequest request =
        new AuthorizationRequest(
            principal,
            List.of(
                new SingleTargetAuthorizationIntent(
                    PolarisAuthorizableOperation.LIST_NAMESPACES,
                    PolarisSecurable.of(
                        new PathSegment(PolarisEntityType.CATALOG, "catalog"),
                        new PathSegment(PolarisEntityType.NAMESPACE, "ns")))));

    AuthorizationDecision decision = authorizer.authorize(authzState, request);

    assertThat(decision.isAllowed()).isTrue();
    verify(manifest)
        .getResolvedPath(ResolvedPathKey.of(List.of("ns"), PolarisEntityType.NAMESPACE), true);
    verify(authorizer)
        .authorizeOrThrow(
            eq(principal),
            eq(Set.of()),
            eq(PolarisAuthorizableOperation.LIST_NAMESPACES),
            eq(List.of(namespaceWrapper)),
            eq(null));
  }

  @Test
  void authorizeSingleOperationMultiIntentRequestEvaluatesSequentially() {
    PolarisAuthorizerImpl authorizer = spy(new PolarisAuthorizerImpl(mock(RealmConfig.class)));
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    AuthorizationState authzState = new AuthorizationState(manifest);
    PolarisResolvedPathWrapper firstCatalogWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisResolvedPathWrapper secondCatalogWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));

    when(manifest.getResolvedTopLevelEntity("catalog1", PolarisEntityType.CATALOG))
        .thenReturn(firstCatalogWrapper);
    when(manifest.getResolvedTopLevelEntity("catalog2", PolarisEntityType.CATALOG))
        .thenReturn(secondCatalogWrapper);
    when(manifest.getAllActivatedCatalogRoleAndPrincipalRoles()).thenReturn(Set.of());
    doNothing()
        .when(authorizer)
        .authorizeOrThrow(
            any(PolarisPrincipal.class),
            ArgumentMatchers.any(),
            eq(PolarisAuthorizableOperation.GET_CATALOG),
            ArgumentMatchers.any(),
            ArgumentMatchers.<List<PolarisResolvedPathWrapper>>any());

    AuthorizationDecision decision =
        authorizer.authorize(
            authzState,
            new AuthorizationRequest(
                principal,
                List.of(
                    new SingleTargetAuthorizationIntent(
                        PolarisAuthorizableOperation.GET_CATALOG,
                        PolarisSecurable.of(
                            new PathSegment(PolarisEntityType.CATALOG, "catalog1"))),
                    new SingleTargetAuthorizationIntent(
                        PolarisAuthorizableOperation.GET_CATALOG,
                        PolarisSecurable.of(
                            new PathSegment(PolarisEntityType.CATALOG, "catalog2"))))));

    assertThat(decision.isAllowed()).isTrue();
    verify(authorizer, times(1))
        .authorizeOrThrow(
            eq(principal),
            eq(Set.of()),
            eq(PolarisAuthorizableOperation.GET_CATALOG),
            eq(List.of(firstCatalogWrapper)),
            eq(null));
    verify(authorizer, times(1))
        .authorizeOrThrow(
            eq(principal),
            eq(Set.of()),
            eq(PolarisAuthorizableOperation.GET_CATALOG),
            eq(List.of(secondCatalogWrapper)),
            eq(null));
  }

  @Test
  void authorizeUpdateTableMultiIntentRequestEvaluatesSequentially() {
    PolarisAuthorizerImpl authorizer = spy(new PolarisAuthorizerImpl(mock(RealmConfig.class)));
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    AuthorizationState authzState = new AuthorizationState(manifest);
    PolarisResolvedPathWrapper tableWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));

    when(manifest.getResolvedPath(
            ResolvedPathKey.of(List.of("ns", "table"), PolarisEntityType.TABLE_LIKE), true))
        .thenReturn(tableWrapper);
    when(manifest.getAllActivatedCatalogRoleAndPrincipalRoles()).thenReturn(Set.of());
    doNothing()
        .when(authorizer)
        .authorizeOrThrow(
            any(PolarisPrincipal.class),
            ArgumentMatchers.any(),
            any(PolarisAuthorizableOperation.class),
            ArgumentMatchers.any(),
            ArgumentMatchers.<List<PolarisResolvedPathWrapper>>any());

    PolarisSecurable tableTarget =
        PolarisSecurable.of(
            new PathSegment(PolarisEntityType.CATALOG, "catalog"),
            new PathSegment(PolarisEntityType.NAMESPACE, "ns"),
            new PathSegment(PolarisEntityType.TABLE_LIKE, "table"));

    AuthorizationDecision decision =
        authorizer.authorize(
            authzState,
            new AuthorizationRequest(
                principal,
                List.of(
                    new SingleTargetAuthorizationIntent(
                        PolarisAuthorizableOperation.REMOVE_TABLE_PROPERTIES, tableTarget),
                    new SingleTargetAuthorizationIntent(
                        PolarisAuthorizableOperation.SET_TABLE_SNAPSHOT_REF, tableTarget))));

    assertThat(decision.isAllowed()).isTrue();
    verify(authorizer, times(1))
        .authorizeOrThrow(
            eq(principal),
            eq(Set.of()),
            eq(PolarisAuthorizableOperation.REMOVE_TABLE_PROPERTIES),
            eq(List.of(tableWrapper)),
            eq(null));
    verify(authorizer, times(1))
        .authorizeOrThrow(
            eq(principal),
            eq(Set.of()),
            eq(PolarisAuthorizableOperation.SET_TABLE_SNAPSHOT_REF),
            eq(List.of(tableWrapper)),
            eq(null));
  }

  @Test
  void authorizationRequestThrowsWhenIntentsAreEmpty() {
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));

    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> new AuthorizationRequest(principal, List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must contain at least one intent");
  }

  @Test
  void authorizeReturnsDenyDecision() {
    PolarisAuthorizerImpl authorizer = spy(new PolarisAuthorizerImpl(mock(RealmConfig.class)));
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    AuthorizationState authzState = new AuthorizationState(manifest);
    PolarisResolvedPathWrapper catalogWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));

    when(manifest.getResolvedTopLevelEntity("catalog", PolarisEntityType.CATALOG))
        .thenReturn(catalogWrapper);
    when(manifest.getAllActivatedCatalogRoleAndPrincipalRoles()).thenReturn(Set.of());
    doThrow(new ForbiddenException("missing privilege"))
        .when(authorizer)
        .authorizeOrThrow(
            any(PolarisPrincipal.class),
            ArgumentMatchers.any(),
            eq(PolarisAuthorizableOperation.GET_CATALOG),
            ArgumentMatchers.any(),
            ArgumentMatchers.<List<PolarisResolvedPathWrapper>>any());

    AuthorizationRequest request =
        new AuthorizationRequest(
            principal,
            List.of(
                new SingleTargetAuthorizationIntent(
                    PolarisAuthorizableOperation.GET_CATALOG,
                    PolarisSecurable.of(new PathSegment(PolarisEntityType.CATALOG, "catalog")))));

    AuthorizationDecision decision = authorizer.authorize(authzState, request);

    assertThat(decision.isAllowed()).isFalse();
    assertThat(decision.getMessage()).hasValue("missing privilege");
  }

  // ----- Tests for server-side missing privilege logging -----

  @Test
  void authorizeOrThrowLogsMissingPrivilegeDetailsServerSide() {
    PolarisAuthorizerImpl authorizer = new PolarisAuthorizerImpl(realmConfigWithDefaults());
    PolarisResolvedPathWrapper namespace = resolvedPath(namespaceEntity("ns1"));

    assertThatThrownBy(
            () ->
                authorizer.authorizeOrThrow(
                    PolarisPrincipal.of("alice", Map.of(), Set.of("reader")),
                    Set.of(),
                    PolarisAuthorizableOperation.CREATE_TABLE_DIRECT,
                    List.of(namespace),
                    null))
        .isInstanceOf(ForbiddenException.class)
        // Client-facing message is generic (no missing privilege details).
        .hasMessage(
            "Principal 'alice' with activated PrincipalRoles '[reader]'"
                + " and activated grants via '[]' is not authorized for op CREATE_TABLE_DIRECT")
        .hasMessageNotContaining("TABLE_CREATE")
        .hasMessageNotContaining("NAMESPACE");
    // Server-side log contains the detailed missing privilege info (verified via log capture
    // in integration tests; unit tests here verify the exception message stays generic).
  }

  @Test
  void authorizeOrThrowLogsAllMissingTargetPrivilegesWithoutShortCircuit() {
    // CREATE_TABLE_DIRECT_WITH_WRITE_DELEGATION requires both TABLE_CREATE and TABLE_WRITE_DATA
    // on the target namespace. With no grants at all, both should be logged server-side but NOT
    // exposed in the client-facing exception.
    PolarisAuthorizerImpl authorizer = new PolarisAuthorizerImpl(realmConfigWithDefaults());
    PolarisResolvedPathWrapper namespace = resolvedPath(namespaceEntity("ns1"));

    assertThatThrownBy(
            () ->
                authorizer.authorizeOrThrow(
                    PolarisPrincipal.of("alice", Map.of(), Set.of("reader")),
                    Set.of(),
                    PolarisAuthorizableOperation.CREATE_TABLE_DIRECT_WITH_WRITE_DELEGATION,
                    List.of(namespace),
                    null))
        .isInstanceOf(ForbiddenException.class)
        // Generic client message.
        .hasMessage(
            "Principal 'alice' with activated PrincipalRoles '[reader]'"
                + " and activated grants via '[]' is not authorized for op CREATE_TABLE_DIRECT_WITH_WRITE_DELEGATION")
        // No privilege details leaked to client.
        .hasMessageNotContaining("TABLE_CREATE")
        .hasMessageNotContaining("TABLE_WRITE_DATA");
  }

  @Test
  void authorizeOrThrowLogsSecondaryMissingPrivilegeServerSide() {
    // RENAME_TABLE: target privilege TABLE_DROP on the existing table; secondary privileges
    // TABLE_LIST and TABLE_CREATE on the destination namespace. Secondary details must NOT
    // appear in the client-facing exception.
    PolarisAuthorizerImpl authorizer = new PolarisAuthorizerImpl(realmConfigWithDefaults());
    PolarisResolvedPathWrapper srcTable = resolvedPath(tableEntity("src_t"));
    PolarisResolvedPathWrapper dstNamespace = resolvedPath(namespaceEntity("dst_ns"));

    assertThatThrownBy(
            () ->
                authorizer.authorizeOrThrow(
                    PolarisPrincipal.of("alice", Map.of(), Set.of("reader")),
                    Set.of(),
                    PolarisAuthorizableOperation.RENAME_TABLE,
                    List.of(srcTable),
                    List.of(dstNamespace)))
        .isInstanceOf(ForbiddenException.class)
        // Generic client message.
        .hasMessage(
            "Principal 'alice' with activated PrincipalRoles '[reader]'"
                + " and activated grants via '[]' is not authorized for op RENAME_TABLE")
        // No secondary details leaked to client.
        .hasMessageNotContaining("TABLE_DROP")
        .hasMessageNotContaining("TABLE_LIST")
        .hasMessageNotContaining("TABLE_CREATE")
        .hasMessageNotContaining("secondary");
  }

  @Test
  void findMissingPrivilegesReturnsEmptyWhenNothingRequiredFails() {
    // When the authorizer would say "yes", the helper must return an empty list — this is the
    // invariant relied on by isAuthorized.
    PolarisAuthorizerImpl authorizer = spy(new PolarisAuthorizerImpl(realmConfigWithDefaults()));
    PolarisResolvedPathWrapper namespace = resolvedPath(namespaceEntity("ns1"));
    // Stub hasTransitivePrivilege to always return true so we exercise the empty-result branch
    // of findMissingPrivileges without rebuilding a full grant graph.
    doReturn(true)
        .when(authorizer)
        .hasTransitivePrivilege(
            any(PolarisPrincipal.class),
            ArgumentMatchers.any(),
            any(PolarisPrivilege.class),
            any(PolarisResolvedPathWrapper.class));

    List<PolarisAuthorizerImpl.MissingPrivilege> missing =
        authorizer.findMissingPrivileges(
            PolarisPrincipal.of("alice", Map.of(), Set.of("reader")),
            Set.of(),
            PolarisAuthorizableOperation.CREATE_TABLE_DIRECT,
            List.of(namespace),
            null);

    assertThat(missing).isEmpty();
  }

  @Test
  void missingPrivilegeDescribeHandlesNullLeafGracefully() {
    // Defensive: the error-formatting path must never throw, even on partial mocks.
    PolarisAuthorizerImpl.MissingPrivilege m =
        PolarisAuthorizerImpl.MissingPrivilege.onTarget(
            PolarisPrivilege.TABLE_CREATE, mock(PolarisResolvedPathWrapper.class));

    assertThat(m.describe()).contains("TABLE_CREATE", "<unknown>");
  }

  // ----- helpers -----

  private static PolarisResolvedPathWrapper resolvedPath(PolarisEntity leaf) {
    // Single-entry path with no grants — the privilege check will always miss.
    return new PolarisResolvedPathWrapper(
        List.of(new ResolvedPolarisEntity(leaf, List.of(), List.of())));
  }

  private static PolarisEntity namespaceEntity(String name) {
    return new PolarisEntity.Builder()
        .setId(1)
        .setCatalogId(0)
        .setType(PolarisEntityType.NAMESPACE)
        .setSubType(PolarisEntitySubType.NULL_SUBTYPE)
        .setName(name)
        .build();
  }

  private static PolarisEntity tableEntity(String name) {
    return new PolarisEntity.Builder()
        .setId(2)
        .setCatalogId(0)
        .setType(PolarisEntityType.TABLE_LIKE)
        .setSubType(PolarisEntitySubType.ICEBERG_TABLE)
        .setName(name)
        .build();
  }

  private static RealmConfig realmConfigWithDefaults() {
    // The credential-rotation-required gate auto-unboxes a Boolean from RealmConfig.getConfig;
    // Mockito's default null answer would NPE before we ever reach the authorization logic.
    // Returning false (the production default for the configuration flag) sends control through
    // to findMissingPrivileges, which is what we want to exercise.
    RealmConfig realmConfig = mock(RealmConfig.class);
    when(realmConfig.getConfig(
            FeatureConfiguration.ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING))
        .thenReturn(false);
    return realmConfig;
  }
}
