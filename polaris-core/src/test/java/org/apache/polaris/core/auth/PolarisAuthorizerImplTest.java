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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

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
  void authorizeUsesRootTargetForRootGrantRequestWithoutPrimaryTarget() {
    // Verify that new authorize SPI call without primary target uses root_container
    // for resolution and authorization
    PolarisAuthorizerImpl authorizer = spy(new PolarisAuthorizerImpl(mock(RealmConfig.class)));
    AuthorizationState authzState = new AuthorizationState();
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    PolarisResolvedPathWrapper rootWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisResolvedPathWrapper principalRoleWrapper = mock(PolarisResolvedPathWrapper.class);

    authzState.setResolutionManifest(manifest);
    when(manifest.getResolvedRootContainerEntityAsPath()).thenReturn(rootWrapper);
    when(manifest.getResolvedTopLevelEntity("analytics-admin", PolarisEntityType.PRINCIPAL_ROLE))
        .thenReturn(principalRoleWrapper);
    when(manifest.getAllActivatedCatalogRoleAndPrincipalRoles())
        .thenReturn(Set.<PolarisBaseEntity>of());
    doNothing()
        .when(authorizer)
        .authorizeOrThrow(
            any(PolarisPrincipal.class),
            org.mockito.ArgumentMatchers.<Set<PolarisBaseEntity>>any(),
            eq(PolarisAuthorizableOperation.ADD_ROOT_GRANT_TO_PRINCIPAL_ROLE),
            org.mockito.ArgumentMatchers.<List<PolarisResolvedPathWrapper>>any(),
            org.mockito.ArgumentMatchers.<List<PolarisResolvedPathWrapper>>any());

    AuthorizationRequest request =
        AuthorizationRequest.of(
            PolarisPrincipal.of("alice", Map.of(), Set.of("role")),
            PolarisAuthorizableOperation.ADD_ROOT_GRANT_TO_PRINCIPAL_ROLE,
            List.of(
                AuthorizationTargetBinding.of(
                    null,
                    PolarisSecurable.of(
                        new PathSegment(PolarisEntityType.PRINCIPAL_ROLE, "analytics-admin")))));

    AuthorizationDecision decision = authorizer.authorize(authzState, request);

    assertThat(decision.isAllowed()).isTrue();
    verify(authorizer)
        .authorizeOrThrow(
            eq(request.getPrincipal()),
            eq(Set.of()),
            eq(PolarisAuthorizableOperation.ADD_ROOT_GRANT_TO_PRINCIPAL_ROLE),
            eq(List.of(rootWrapper)),
            eq(null));
  }

  @Test
  void authorizeUsesRootTargetForListCatalogsRequestWithoutPrimaryTarget() {
    // Verify that new authorize SPI call without primary target uses root_container
    // for resolution and authorization
    PolarisAuthorizerImpl authorizer = spy(new PolarisAuthorizerImpl(mock(RealmConfig.class)));
    AuthorizationState authzState = new AuthorizationState();
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    PolarisResolvedPathWrapper rootWrapper = mock(PolarisResolvedPathWrapper.class);

    authzState.setResolutionManifest(manifest);
    when(manifest.getResolvedRootContainerEntityAsPath()).thenReturn(rootWrapper);
    when(manifest.getAllActivatedCatalogRoleAndPrincipalRoles())
        .thenReturn(Set.<PolarisBaseEntity>of());
    doNothing()
        .when(authorizer)
        .authorizeOrThrow(
            any(PolarisPrincipal.class),
            org.mockito.ArgumentMatchers.<Set<PolarisBaseEntity>>any(),
            eq(PolarisAuthorizableOperation.LIST_CATALOGS),
            org.mockito.ArgumentMatchers.<List<PolarisResolvedPathWrapper>>any(),
            any());

    AuthorizationRequest request =
        AuthorizationRequest.of(
            PolarisPrincipal.of("alice", Map.of(), Set.of("role")),
            PolarisAuthorizableOperation.LIST_CATALOGS,
            List.of());

    AuthorizationDecision decision = authorizer.authorize(authzState, request);

    assertThat(decision.isAllowed()).isTrue();
    verify(authorizer)
        .authorizeOrThrow(
            eq(request.getPrincipal()),
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
    AuthorizationState authzState = new AuthorizationState();
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    PolarisResolvedPathWrapper namespaceWrapper = mock(PolarisResolvedPathWrapper.class);

    authzState.setResolutionManifest(manifest);
    when(manifest.getResolvedPath(
            ResolvedPathKey.of(List.of("ns"), PolarisEntityType.NAMESPACE), true))
        .thenReturn(namespaceWrapper);
    when(manifest.getAllActivatedCatalogRoleAndPrincipalRoles())
        .thenReturn(Set.<PolarisBaseEntity>of());
    doNothing()
        .when(authorizer)
        .authorizeOrThrow(
            any(PolarisPrincipal.class),
            org.mockito.ArgumentMatchers.<Set<PolarisBaseEntity>>any(),
            eq(PolarisAuthorizableOperation.LIST_NAMESPACES),
            org.mockito.ArgumentMatchers.<List<PolarisResolvedPathWrapper>>any(),
            any());

    AuthorizationRequest request =
        AuthorizationRequest.of(
            PolarisPrincipal.of("alice", Map.of(), Set.of("role")),
            PolarisAuthorizableOperation.LIST_NAMESPACES,
            List.of(
                AuthorizationTargetBinding.of(
                    PolarisSecurable.of(
                        new PathSegment(PolarisEntityType.CATALOG, "catalog"),
                        new PathSegment(PolarisEntityType.NAMESPACE, "ns")),
                    null)));

    AuthorizationDecision decision = authorizer.authorize(authzState, request);

    assertThat(decision.isAllowed()).isTrue();
    verify(manifest)
        .getResolvedPath(ResolvedPathKey.of(List.of("ns"), PolarisEntityType.NAMESPACE), true);
    verify(authorizer)
        .authorizeOrThrow(
            eq(request.getPrincipal()),
            eq(Set.of()),
            eq(PolarisAuthorizableOperation.LIST_NAMESPACES),
            eq(List.of(namespaceWrapper)),
            eq(null));
  }
}
