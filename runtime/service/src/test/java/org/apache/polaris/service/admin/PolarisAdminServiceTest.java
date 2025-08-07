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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.core.SecurityContext;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.PrivilegeResult;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.service.config.ReservedProperties;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class PolarisAdminServiceTest {

  @Mock private CallContext callContext;
  @Mock private PolarisCallContext polarisCallContext;
  @Mock private PolarisDiagnostics polarisDiagnostics;
  @Mock private ResolutionManifestFactory resolutionManifestFactory;
  @Mock private PolarisMetaStoreManager metaStoreManager;
  @Mock private UserSecretsManager userSecretsManager;
  @Mock private SecurityContext securityContext;
  @Mock private PolarisAuthorizer authorizer;
  @Mock private ReservedProperties reservedProperties;
  @Mock private AuthenticatedPolarisPrincipal authenticatedPrincipal;
  @Mock private PolarisResolutionManifest resolutionManifest;
  @Mock private PolarisResolvedPathWrapper resolvedPathWrapper;

  private PolarisAdminService adminService;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    when(securityContext.getUserPrincipal()).thenReturn(authenticatedPrincipal);
    when(callContext.getPolarisCallContext()).thenReturn(polarisCallContext);
    when(polarisCallContext.getDiagServices()).thenReturn(polarisDiagnostics);

    adminService =
        new PolarisAdminService(
            callContext,
            resolutionManifestFactory,
            metaStoreManager,
            userSecretsManager,
            securityContext,
            authorizer,
            reservedProperties);
  }

  protected static void assertSuccess(BaseResult result) {
    Assertions.assertThat(result.isSuccess()).isTrue();
  }

  @Test
  void testGrantPrivilegeOnNamespaceToRole() throws Exception {
    String catalogName = "test-catalog";
    String catalogRoleName = "test-role";
    Namespace namespace = Namespace.of("existing-ns");
    PolarisPrivilege privilege = PolarisPrivilege.NAMESPACE_FULL_METADATA;

    setupSuccessfulNamespaceResolution(catalogName, catalogRoleName, namespace);

    PrivilegeResult successResult = mock(PrivilegeResult.class);
    when(successResult.isSuccess()).thenReturn(true);
    when(metaStoreManager.grantPrivilegeOnSecurableToRole(any(), any(), any(), any(), any()))
        .thenReturn(successResult);

    PrivilegeResult result =
        adminService.grantPrivilegeOnNamespaceToRole(
            catalogName, catalogRoleName, namespace, privilege);

    assertSuccess(result);
  }

  @Test
  void testGrantPrivilegeOnNamespaceToRole_ThrowsNamespaceNotFoundException() {
    String catalogName = "test-catalog";
    String catalogRoleName = "test-role";
    Namespace namespace = Namespace.of("non-existent-ns");
    PolarisPrivilege privilege = PolarisPrivilege.NAMESPACE_FULL_METADATA;

    when(resolutionManifestFactory.createResolutionManifest(any(), any(), eq(catalogName)))
        .thenReturn(resolutionManifest);
    when(resolutionManifest.resolveAll()).thenReturn(createSuccessfulResolverStatus());

    PolarisResolvedPathWrapper catalogRoleWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisEntity catalogRoleEntity = createEntity(catalogRoleName, PolarisEntityType.CATALOG_ROLE);
    when(catalogRoleWrapper.getRawLeafEntity()).thenReturn(catalogRoleEntity);
    when(resolutionManifest.getResolvedPath(eq(catalogRoleName))).thenReturn(catalogRoleWrapper);

    when(resolutionManifest.getResolvedPath(eq(namespace))).thenReturn(null);

    assertThatThrownBy(
            () ->
                adminService.grantPrivilegeOnNamespaceToRole(
                    catalogName, catalogRoleName, namespace, privilege))
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining("Namespace " + namespace + " not found");
  }

  @Test
  void testGrantPrivilegeOnNamespaceToRole_IncompleteNamespaceThrowsNamespaceNotFoundException()
      throws Exception {
    String catalogName = "test-catalog";
    String catalogRoleName = "test-role";
    Namespace namespace = Namespace.of("complete-ns", "incomplete-ns");
    PolarisPrivilege privilege = PolarisPrivilege.NAMESPACE_FULL_METADATA;

    when(resolutionManifestFactory.createResolutionManifest(any(), any(), eq(catalogName)))
        .thenReturn(resolutionManifest);
    when(resolutionManifest.resolveAll()).thenReturn(createSuccessfulResolverStatus());

    PolarisResolvedPathWrapper catalogRoleWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisEntity catalogRoleEntity = createEntity(catalogRoleName, PolarisEntityType.CATALOG_ROLE);
    when(catalogRoleWrapper.getRawLeafEntity()).thenReturn(catalogRoleEntity);
    when(resolutionManifest.getResolvedPath(eq(catalogRoleName))).thenReturn(catalogRoleWrapper);

    when(resolutionManifest.getResolvedPath(eq(namespace))).thenReturn(resolvedPathWrapper);
    when(resolvedPathWrapper.getRawFullPath())
        .thenReturn(
            List.of(
                createEntity("test-catalog", PolarisEntityType.CATALOG),
                createEntity("complete-ns", PolarisEntityType.NAMESPACE)));
    when(resolvedPathWrapper.isFullyResolvedNamespace(eq(catalogName), eq(namespace)))
        .thenReturn(false);

    assertThatThrownBy(
            () ->
                adminService.grantPrivilegeOnNamespaceToRole(
                    catalogName, catalogRoleName, namespace, privilege))
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining("Namespace " + namespace + " not found");
  }

  @Test
  void testRevokePrivilegeOnNamespaceFromRole() throws Exception {
    String catalogName = "test-catalog";
    String catalogRoleName = "test-role";
    Namespace namespace = Namespace.of("existing-ns");
    PolarisPrivilege privilege = PolarisPrivilege.NAMESPACE_FULL_METADATA;

    setupSuccessfulNamespaceResolution(catalogName, catalogRoleName, namespace);

    PrivilegeResult successResult = mock(PrivilegeResult.class);
    when(successResult.isSuccess()).thenReturn(true);
    when(metaStoreManager.revokePrivilegeOnSecurableFromRole(any(), any(), any(), any(), any()))
        .thenReturn(successResult);

    PrivilegeResult result =
        adminService.revokePrivilegeOnNamespaceFromRole(
            catalogName, catalogRoleName, namespace, privilege);

    assertSuccess(result);
  }

  @Test
  void testRevokePrivilegeOnNamespaceFromRole_ThrowsNamespaceNotFoundException() {
    String catalogName = "test-catalog";
    String catalogRoleName = "test-role";
    Namespace namespace = Namespace.of("non-existent-ns");
    PolarisPrivilege privilege = PolarisPrivilege.NAMESPACE_FULL_METADATA;

    when(resolutionManifestFactory.createResolutionManifest(any(), any(), eq(catalogName)))
        .thenReturn(resolutionManifest);
    when(resolutionManifest.resolveAll()).thenReturn(createSuccessfulResolverStatus());

    PolarisResolvedPathWrapper catalogRoleWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisEntity catalogRoleEntity = createEntity(catalogRoleName, PolarisEntityType.CATALOG_ROLE);
    when(catalogRoleWrapper.getRawLeafEntity()).thenReturn(catalogRoleEntity);
    when(resolutionManifest.getResolvedPath(eq(catalogRoleName))).thenReturn(catalogRoleWrapper);

    when(resolutionManifest.getResolvedPath(eq(namespace))).thenReturn(null);

    assertThatThrownBy(
            () ->
                adminService.revokePrivilegeOnNamespaceFromRole(
                    catalogName, catalogRoleName, namespace, privilege))
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining("Namespace " + namespace + " not found");
  }

  @Test
  void testRevokePrivilegeOnNamespaceFromRole_IncompletelNamespaceThrowsNamespaceNotFoundException()
      throws Exception {
    String catalogName = "test-catalog";
    String catalogRoleName = "test-role";
    Namespace namespace = Namespace.of("incomplete-ns");
    PolarisPrivilege privilege = PolarisPrivilege.NAMESPACE_FULL_METADATA;

    when(resolutionManifestFactory.createResolutionManifest(any(), any(), eq(catalogName)))
        .thenReturn(resolutionManifest);
    when(resolutionManifest.resolveAll()).thenReturn(createSuccessfulResolverStatus());

    PolarisResolvedPathWrapper catalogRoleWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisEntity catalogRoleEntity = createEntity(catalogRoleName, PolarisEntityType.CATALOG_ROLE);
    when(catalogRoleWrapper.getRawLeafEntity()).thenReturn(catalogRoleEntity);
    when(resolutionManifest.getResolvedPath(eq(catalogRoleName))).thenReturn(catalogRoleWrapper);

    when(resolutionManifest.getResolvedPath(eq(namespace))).thenReturn(resolvedPathWrapper);
    when(resolvedPathWrapper.getRawFullPath())
        .thenReturn(List.of(createEntity("wrong-catalog", PolarisEntityType.CATALOG)));
    when(resolvedPathWrapper.isFullyResolvedNamespace(eq(catalogName), eq(namespace)))
        .thenReturn(false);

    assertThatThrownBy(
            () ->
                adminService.revokePrivilegeOnNamespaceFromRole(
                    catalogName, catalogRoleName, namespace, privilege))
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining("Namespace " + namespace + " not found");
  }

  private PolarisEntity createEntity(String name, PolarisEntityType type) {
    return new PolarisEntity.Builder()
        .setName(name)
        .setType(type)
        .setId(1L)
        .setCatalogId(1L)
        .setParentId(1L)
        .setCreateTimestamp(System.currentTimeMillis())
        .build();
  }

  private ResolverStatus createSuccessfulResolverStatus() {
    return new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS);
  }

  private void setupSuccessfulNamespaceResolution(
      String catalogName, String catalogRoleName, Namespace namespace) throws Exception {

    when(resolutionManifestFactory.createResolutionManifest(any(), any(), eq(catalogName)))
        .thenReturn(resolutionManifest);
    when(resolutionManifest.resolveAll()).thenReturn(createSuccessfulResolverStatus());
    when(resolutionManifest.getResolvedPath(eq(namespace))).thenReturn(resolvedPathWrapper);

    PolarisEntity catalogEntity = createEntity(catalogName, PolarisEntityType.CATALOG);
    PolarisEntity namespaceEntity =
        createEntity(namespace.levels()[0], PolarisEntityType.NAMESPACE);
    List<PolarisEntity> fullPath = List.of(catalogEntity, namespaceEntity);
    when(resolvedPathWrapper.getRawFullPath()).thenReturn(fullPath);
    when(resolvedPathWrapper.getRawParentPath()).thenReturn(List.of(catalogEntity));
    when(resolvedPathWrapper.getRawLeafEntity()).thenReturn(namespaceEntity);
    when(resolvedPathWrapper.isFullyResolvedNamespace(eq(catalogName), eq(namespace)))
        .thenReturn(true);

    PolarisResolvedPathWrapper catalogRoleWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisEntity catalogRoleEntity = createEntity(catalogRoleName, PolarisEntityType.CATALOG_ROLE);
    when(catalogRoleWrapper.getRawLeafEntity()).thenReturn(catalogRoleEntity);
    when(resolutionManifest.getResolvedPath(eq(catalogRoleName))).thenReturn(catalogRoleWrapper);
  }
}
