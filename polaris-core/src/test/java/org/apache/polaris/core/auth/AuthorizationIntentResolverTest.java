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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.polaris.core.auth.AuthorizationIntentResolver.ResolvedIntent;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link AuthorizationIntentResolver}, verifying that each {@link
 * AuthorizationIntent} type is translated into the expected target/secondary resolved paths and
 * that each securable-resolution branch (reference catalog, top-level entity, path) is exercised.
 */
public class AuthorizationIntentResolverTest {

  private static final PolarisAuthorizableOperation OP = PolarisAuthorizableOperation.LIST_CATALOGS;

  private static PolarisSecurable catalogSecurable() {
    return PolarisSecurable.of(new PathSegment(PolarisEntityType.CATALOG, "catalog"));
  }

  private static PolarisSecurable namespaceSecurable() {
    // Leaf is a NAMESPACE (not top-level) reached through a catalog -> resolved via a path key.
    return PolarisSecurable.of(
        new PathSegment(PolarisEntityType.CATALOG, "catalog"),
        new PathSegment(PolarisEntityType.NAMESPACE, "ns"));
  }

  private static PolarisSecurable principalRoleSecurable(String name) {
    // Leaf is a top-level PRINCIPAL_ROLE -> resolved via getResolvedTopLevelEntity.
    return PolarisSecurable.of(new PathSegment(PolarisEntityType.PRINCIPAL_ROLE, name));
  }

  @Test
  void resolve_targetless_prependRootContainer_targetsRootContainerAndNoSecondaries() {
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    PolarisResolvedPathWrapper root = mock(PolarisResolvedPathWrapper.class);
    when(manifest.getResolvedRootContainerEntityAsPath()).thenReturn(root);

    ResolvedIntent resolved =
        AuthorizationIntentResolver.resolve(manifest, new TargetlessAuthorizationIntent(OP), true);

    assertThat(resolved.targets()).containsExactly(root);
    assertThat(resolved.secondaries()).isNull();
  }

  @Test
  void resolve_targetless_noPrependRootContainer_targetsAndSecondariesNull() {
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);

    ResolvedIntent resolved =
        AuthorizationIntentResolver.resolve(manifest, new TargetlessAuthorizationIntent(OP), false);

    assertThat(resolved.targets()).isNull();
    assertThat(resolved.secondaries()).isNull();
  }

  @Test
  void resolve_singleTarget_catalogLeaf_resolvedViaReferenceCatalog() {
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    PolarisResolvedPathWrapper catalog = mock(PolarisResolvedPathWrapper.class);
    when(manifest.getResolvedReferenceCatalogEntity(true)).thenReturn(catalog);

    ResolvedIntent resolved =
        AuthorizationIntentResolver.resolve(
            manifest, new SingleTargetAuthorizationIntent(OP, catalogSecurable()), true);

    assertThat(resolved.targets()).containsExactly(catalog);
    assertThat(resolved.secondaries()).isNull();
  }

  @Test
  void resolve_singleTarget_topLevelLeaf_resolvedViaTopLevelEntity() {
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    PolarisResolvedPathWrapper role = mock(PolarisResolvedPathWrapper.class);
    when(manifest.getResolvedTopLevelEntity("admin", PolarisEntityType.PRINCIPAL_ROLE))
        .thenReturn(role);

    ResolvedIntent resolved =
        AuthorizationIntentResolver.resolve(
            manifest,
            new SingleTargetAuthorizationIntent(OP, principalRoleSecurable("admin")),
            true);

    assertThat(resolved.targets()).containsExactly(role);
    assertThat(resolved.secondaries()).isNull();
  }

  @Test
  void resolve_singleTarget_pathLeaf_resolvedViaResolvedPathWithCatalogSegmentStripped() {
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    PolarisResolvedPathWrapper namespace = mock(PolarisResolvedPathWrapper.class);
    // The CATALOG segment is stripped, so the lookup key is just ["ns"].
    when(manifest.getResolvedPath(
            ResolvedPathKey.of(List.of("ns"), PolarisEntityType.NAMESPACE), true))
        .thenReturn(namespace);

    ResolvedIntent resolved =
        AuthorizationIntentResolver.resolve(
            manifest, new SingleTargetAuthorizationIntent(OP, namespaceSecurable()), true);

    assertThat(resolved.targets()).containsExactly(namespace);
    assertThat(resolved.secondaries()).isNull();
  }

  @Test
  void resolve_rename_resolvesFromAsTargetAndToAsSecondary() {
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    PolarisResolvedPathWrapper from = mock(PolarisResolvedPathWrapper.class);
    PolarisResolvedPathWrapper to = mock(PolarisResolvedPathWrapper.class);
    when(manifest.getResolvedTopLevelEntity("from", PolarisEntityType.PRINCIPAL_ROLE))
        .thenReturn(from);
    when(manifest.getResolvedTopLevelEntity("to", PolarisEntityType.PRINCIPAL_ROLE)).thenReturn(to);

    ResolvedIntent resolved =
        AuthorizationIntentResolver.resolve(
            manifest,
            new RenameAuthorizationIntent(
                OP, principalRoleSecurable("from"), principalRoleSecurable("to")),
            true);

    assertThat(resolved.targets()).containsExactly(from);
    assertThat(resolved.secondaries()).containsExactly(to);
  }

  @Test
  void resolve_policyAttachment_resolvesPolicyAndAttachedTo() {
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    PolarisResolvedPathWrapper policy = mock(PolarisResolvedPathWrapper.class);
    PolarisResolvedPathWrapper attachedTo = mock(PolarisResolvedPathWrapper.class);
    when(manifest.getResolvedTopLevelEntity("policy", PolarisEntityType.PRINCIPAL_ROLE))
        .thenReturn(policy);
    when(manifest.getResolvedTopLevelEntity("attachedTo", PolarisEntityType.PRINCIPAL_ROLE))
        .thenReturn(attachedTo);

    ResolvedIntent resolved =
        AuthorizationIntentResolver.resolve(
            manifest,
            new PolicyAttachmentAuthorizationIntent(
                OP, principalRoleSecurable("policy"), principalRoleSecurable("attachedTo")),
            true);

    assertThat(resolved.targets()).containsExactly(policy);
    assertThat(resolved.secondaries()).containsExactly(attachedTo);
  }

  @Test
  void resolve_roleAssignment_resolvesRoleAndAssignee() {
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    PolarisResolvedPathWrapper role = mock(PolarisResolvedPathWrapper.class);
    PolarisResolvedPathWrapper assignee = mock(PolarisResolvedPathWrapper.class);
    when(manifest.getResolvedTopLevelEntity("role", PolarisEntityType.PRINCIPAL_ROLE))
        .thenReturn(role);
    when(manifest.getResolvedTopLevelEntity("assignee", PolarisEntityType.PRINCIPAL_ROLE))
        .thenReturn(assignee);

    ResolvedIntent resolved =
        AuthorizationIntentResolver.resolve(
            manifest,
            new RoleAssignmentAuthorizationIntent(
                OP, principalRoleSecurable("role"), principalRoleSecurable("assignee")),
            true);

    assertThat(resolved.targets()).containsExactly(role);
    assertThat(resolved.secondaries()).containsExactly(assignee);
  }

  @Test
  void resolve_privilegeGrant_resolvesGrantTargetAndGrantee() {
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    PolarisResolvedPathWrapper grantTarget = mock(PolarisResolvedPathWrapper.class);
    PolarisResolvedPathWrapper grantee = mock(PolarisResolvedPathWrapper.class);
    when(manifest.getResolvedReferenceCatalogEntity(true)).thenReturn(grantTarget);
    when(manifest.getResolvedTopLevelEntity("grantee", PolarisEntityType.PRINCIPAL_ROLE))
        .thenReturn(grantee);

    ResolvedIntent resolved =
        AuthorizationIntentResolver.resolve(
            manifest,
            new PrivilegeGrantAuthorizationIntent(
                OP, catalogSecurable(), principalRoleSecurable("grantee")),
            true);

    assertThat(resolved.targets()).containsExactly(grantTarget);
    assertThat(resolved.secondaries()).containsExactly(grantee);
  }

  @Test
  void resolve_rootPrivilegeGrant_targetsRootContainerAndResolvesGrantee() {
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    PolarisResolvedPathWrapper root = mock(PolarisResolvedPathWrapper.class);
    PolarisResolvedPathWrapper grantee = mock(PolarisResolvedPathWrapper.class);
    when(manifest.getResolvedRootContainerEntityAsPath()).thenReturn(root);
    when(manifest.getResolvedTopLevelEntity("grantee", PolarisEntityType.PRINCIPAL_ROLE))
        .thenReturn(grantee);

    ResolvedIntent resolved =
        AuthorizationIntentResolver.resolve(
            manifest,
            new RootPrivilegeGrantAuthorizationIntent(OP, principalRoleSecurable("grantee")),
            true);

    assertThat(resolved.targets()).containsExactly(root);
    assertThat(resolved.secondaries()).containsExactly(grantee);
  }

  @Test
  void resolve_unresolvableSecurable_throwsIllegalState() {
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    // Manifest returns null (default) for the reference catalog lookup -> checkState should trip.

    assertThatThrownBy(
            () ->
                AuthorizationIntentResolver.resolve(
                    manifest, new SingleTargetAuthorizationIntent(OP, catalogSecurable()), true))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Resolved path for securable is null");
  }
}
