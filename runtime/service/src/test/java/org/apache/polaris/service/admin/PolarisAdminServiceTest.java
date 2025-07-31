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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.core.SecurityContext;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.GenerateEntityIdResult;
import org.apache.polaris.core.persistence.dao.entity.PrivilegeResult;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.service.config.ReservedProperties;
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

    when(resolutionManifestFactory.createResolutionManifest(any(), any(), any()))
        .thenReturn(resolutionManifest);
    when(resolutionManifest.resolveAll()).thenReturn(createSuccessfulResolverStatus());
    when(resolutionManifest.getIsPassthroughFacade()).thenReturn(false);

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

    boolean result =
        adminService.grantPrivilegeOnNamespaceToRole(
            catalogName, catalogRoleName, namespace, privilege);

    assertThat(result).isTrue();
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

    PolarisEntity catalogEntity = createEntity(catalogName, PolarisEntityType.CATALOG);
    PolarisResolvedPathWrapper catalogWrapper = mock(PolarisResolvedPathWrapper.class);
    when(catalogWrapper.getRawLeafEntity()).thenReturn(catalogEntity);
    when(resolutionManifest.getResolvedReferenceCatalogEntity()).thenReturn(catalogWrapper);

    PolarisResolvedPathWrapper catalogRoleWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisEntity catalogRoleEntity =
        createEntity(catalogRoleName, PolarisEntityType.CATALOG_ROLE, 2L);
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

    PolarisEntity catalogEntity = createEntity(catalogName, PolarisEntityType.CATALOG, 1L);
    PolarisResolvedPathWrapper catalogWrapper = mock(PolarisResolvedPathWrapper.class);
    when(catalogWrapper.getRawLeafEntity()).thenReturn(catalogEntity);
    when(resolutionManifest.getResolvedReferenceCatalogEntity()).thenReturn(catalogWrapper);

    PolarisResolvedPathWrapper catalogRoleWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisEntity catalogRoleEntity =
        createEntity(catalogRoleName, PolarisEntityType.CATALOG_ROLE, 2L);
    when(catalogRoleWrapper.getRawLeafEntity()).thenReturn(catalogRoleEntity);
    when(resolutionManifest.getResolvedPath(eq(catalogRoleName))).thenReturn(catalogRoleWrapper);

    when(resolutionManifest.getResolvedPath(eq(namespace))).thenReturn(resolvedPathWrapper);
    when(resolvedPathWrapper.getRawFullPath())
        .thenReturn(
            List.of(
                createEntity("test-catalog", PolarisEntityType.CATALOG),
                createEntity("complete-ns", PolarisEntityType.NAMESPACE, 3L, 1L)));
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

    boolean result =
        adminService.revokePrivilegeOnNamespaceFromRole(
            catalogName, catalogRoleName, namespace, privilege);

    assertThat(result).isTrue();
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
    PolarisEntity catalogRoleEntity =
        createEntity(catalogRoleName, PolarisEntityType.CATALOG_ROLE, 2L);
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
    PolarisEntity catalogRoleEntity =
        createEntity(catalogRoleName, PolarisEntityType.CATALOG_ROLE, 2L);
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

  @Test
  void testGrantPrivilegeOnNamespaceToRole_PassthroughFacade() throws Exception {
    String catalogName = "test-catalog";
    String catalogRoleName = "test-role";
    Namespace namespace = Namespace.of("org-ns", "team-ns", "project-ns");
    PolarisPrivilege privilege = PolarisPrivilege.NAMESPACE_FULL_METADATA;

    PolarisEntity catalogEntity = createEntity(catalogName, PolarisEntityType.CATALOG);
    PolarisResolvedPathWrapper catalogWrapper = mock(PolarisResolvedPathWrapper.class);
    when(catalogWrapper.getRawLeafEntity()).thenReturn(catalogEntity);
    when(resolutionManifest.getResolvedReferenceCatalogEntity()).thenReturn(catalogWrapper);
    when(resolutionManifest.getIsPassthroughFacade()).thenReturn(true);

    PolarisResolvedPathWrapper catalogRoleWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisEntity catalogRoleEntity =
        createEntity(catalogRoleName, PolarisEntityType.CATALOG_ROLE, 2L);
    when(catalogRoleWrapper.getRawLeafEntity()).thenReturn(catalogRoleEntity);
    when(resolutionManifest.getResolvedPath(eq(catalogRoleName))).thenReturn(catalogRoleWrapper);

    PolarisEntity orgNsEntity = createEntity("org-ns", PolarisEntityType.NAMESPACE, 3L, 1L);
    when(resolutionManifest.getResolvedPath(eq(namespace))).thenReturn(resolvedPathWrapper);
    when(resolvedPathWrapper.getRawFullPath()).thenReturn(List.of(catalogEntity, orgNsEntity));
    when(resolvedPathWrapper.getRawLeafEntity()).thenReturn(orgNsEntity);

    // Mock creation of team-ns.
    GenerateEntityIdResult idResult = mock(GenerateEntityIdResult.class);
    when(idResult.getId()).thenReturn(4L);
    when(metaStoreManager.generateNewEntityId(any())).thenReturn(idResult);
    EntityResult teamNsCreateResult = mock(EntityResult.class);
    EntityResult projectNsCreateResult = mock(EntityResult.class);
    when(teamNsCreateResult.isSuccess()).thenReturn(true);
    when(projectNsCreateResult.isSuccess()).thenReturn(true);

    PolarisEntity teamNsEntity = createEntity("team-ns", PolarisEntityType.NAMESPACE, 4L, 3L);
    when(teamNsCreateResult.getEntity()).thenReturn(teamNsEntity);

    // Mock creation of project-ns.
    when(idResult.getId()).thenReturn(5L);
    when(metaStoreManager.generateNewEntityId(any())).thenReturn(idResult);
    PolarisEntity projectNsEntity = createEntity("project-ns", PolarisEntityType.NAMESPACE, 5L, 4L);
    when(projectNsCreateResult.getEntity()).thenReturn(projectNsEntity);

    when(metaStoreManager.createEntityIfNotExists(any(), any(), any()))
        .thenReturn(teamNsCreateResult, projectNsCreateResult);

    // Mock successful synthetic namespace resolution.
    PolarisResolvedPathWrapper syntheticPathWrapper = mock(PolarisResolvedPathWrapper.class);
    when(resolutionManifest.getResolvedPath(eq(namespace))).thenReturn(syntheticPathWrapper);
    when(syntheticPathWrapper.isFullyResolvedNamespace(eq(catalogName), eq(namespace)))
        .thenReturn(true);

    PrivilegeResult successResult = mock(PrivilegeResult.class);
    when(successResult.isSuccess()).thenReturn(true);
    when(metaStoreManager.grantPrivilegeOnSecurableToRole(any(), any(), any(), any(), any()))
        .thenReturn(successResult);

    boolean result =
        adminService.grantPrivilegeOnNamespaceToRole(
            catalogName, catalogRoleName, namespace, privilege);
    assertThat(result).isTrue();
  }

  @Test
  void testGrantPrivilegeOnNamespaceToRole_SyntheticEntityCreationFails() throws Exception {
    String catalogName = "test-catalog";
    String catalogRoleName = "test-role";
    Namespace namespace = Namespace.of("org-ns", "team-ns", "project-ns");
    PolarisPrivilege privilege = PolarisPrivilege.NAMESPACE_FULL_METADATA;

    PolarisEntity catalogEntity = createEntity(catalogName, PolarisEntityType.CATALOG);
    PolarisResolvedPathWrapper catalogWrapper = mock(PolarisResolvedPathWrapper.class);
    when(catalogWrapper.getRawLeafEntity()).thenReturn(catalogEntity);
    when(resolutionManifest.getResolvedReferenceCatalogEntity()).thenReturn(catalogWrapper);
    when(resolutionManifest.getIsPassthroughFacade()).thenReturn(true);

    PolarisResolvedPathWrapper catalogRoleWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisEntity catalogRoleEntity =
        createEntity(catalogRoleName, PolarisEntityType.CATALOG_ROLE, 2L);
    when(catalogRoleWrapper.getRawLeafEntity()).thenReturn(catalogRoleEntity);
    when(resolutionManifest.getResolvedPath(eq(catalogRoleName))).thenReturn(catalogRoleWrapper);

    PolarisEntity orgNsEntity = createEntity("org-ns", PolarisEntityType.NAMESPACE, 3L, 1L);
    when(resolutionManifest.getResolvedPath(eq(namespace))).thenReturn(resolvedPathWrapper);
    when(resolvedPathWrapper.getRawFullPath()).thenReturn(List.of(catalogEntity, orgNsEntity));
    when(resolvedPathWrapper.getRawLeafEntity()).thenReturn(orgNsEntity);

    // Mock generateNewEntityId for team-ns
    GenerateEntityIdResult idResult = mock(GenerateEntityIdResult.class);
    when(idResult.getId()).thenReturn(4L);
    when(metaStoreManager.generateNewEntityId(any())).thenReturn(idResult);

    // Mock createEntityIfNotExists to fail
    EntityResult failedResult = mock(EntityResult.class);
    when(failedResult.isSuccess()).thenReturn(false);
    when(metaStoreManager.createEntityIfNotExists(any(), any(), any())).thenReturn(failedResult);

    // Mock getResolvedPath to return null for partial namespace
    PolarisResolvedPathWrapper partialPathWrapper = mock(PolarisResolvedPathWrapper.class);
    when(partialPathWrapper.getRawLeafEntity()).thenReturn(orgNsEntity);
    when(resolutionManifest.getResolvedPath(eq(Namespace.of("org-ns", "team-ns"))))
        .thenReturn(partialPathWrapper);

    assertThatThrownBy(
            () ->
                adminService.grantPrivilegeOnNamespaceToRole(
                    catalogName, catalogRoleName, namespace, privilege))
        .isInstanceOf(RuntimeException.class)
        .hasMessage(
            "Failed to create or find namespace entity 'team-ns' in federated catalog 'test-catalog'");
  }

  @Test
  void testGrantPrivilegeOnTableLikeToRole_PassthroughFacade() throws Exception {
    String catalogName = "test-catalog";
    String catalogRoleName = "test-role";
    Namespace namespace = Namespace.of("org-ns", "team-ns", "project-ns");
    TableIdentifier identifier = TableIdentifier.of(namespace, "test-table");
    PolarisPrivilege privilege = PolarisPrivilege.TABLE_WRITE_DATA;

    PolarisEntity catalogEntity = createEntity(catalogName, PolarisEntityType.CATALOG);
    PolarisResolvedPathWrapper catalogWrapper = mock(PolarisResolvedPathWrapper.class);
    when(catalogWrapper.getRawLeafEntity()).thenReturn(catalogEntity);
    when(resolutionManifest.getResolvedReferenceCatalogEntity()).thenReturn(catalogWrapper);
    when(resolutionManifest.getIsPassthroughFacade()).thenReturn(true);

    PolarisResolvedPathWrapper catalogRoleWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisEntity catalogRoleEntity = createEntity(catalogRoleName, PolarisEntityType.CATALOG_ROLE);
    when(catalogRoleWrapper.getRawLeafEntity()).thenReturn(catalogRoleEntity);
    when(resolutionManifest.getResolvedPath(eq(catalogRoleName))).thenReturn(catalogRoleWrapper);

    PolarisEntity orgNsEntity = createEntity("org-ns", PolarisEntityType.NAMESPACE, 3L, 1L);
    PolarisEntity teamNsEntity = createEntity("team-ns", PolarisEntityType.NAMESPACE, 4L, 3L);

    PolarisResolvedPathWrapper existingPathWrapper = mock(PolarisResolvedPathWrapper.class);
    when(existingPathWrapper.getRawFullPath())
        .thenReturn(List.of(catalogEntity, orgNsEntity, teamNsEntity));
    when(existingPathWrapper.getRawLeafEntity()).thenReturn(teamNsEntity);
    when(resolutionManifest.getResolvedPath(
            identifier, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ANY_SUBTYPE))
        .thenReturn(existingPathWrapper);
    when(existingPathWrapper.getRawLeafEntity()).thenReturn(teamNsEntity);

    GenerateEntityIdResult idResult = mock(GenerateEntityIdResult.class);
    when(idResult.getId()).thenReturn(5L);
    when(metaStoreManager.generateNewEntityId(any())).thenReturn(idResult);
    PolarisEntity projectNsEntity = createEntity("project-ns", PolarisEntityType.NAMESPACE, 5L, 4L);
    EntityResult projectNsCreateResult = mock(EntityResult.class);
    when(projectNsCreateResult.isSuccess()).thenReturn(true);
    when(projectNsCreateResult.getEntity()).thenReturn(projectNsEntity);
    when(metaStoreManager.createEntityIfNotExists(any(), any(), any()))
        .thenReturn(projectNsCreateResult);

    PolarisResolvedPathWrapper syntheticPathWrapper = mock(PolarisResolvedPathWrapper.class);
    when(syntheticPathWrapper.getRawFullPath())
        .thenReturn(List.of(catalogEntity, orgNsEntity, teamNsEntity, projectNsEntity));
    when(resolutionManifest.getResolvedPath(eq(namespace))).thenReturn(syntheticPathWrapper);
    when(syntheticPathWrapper.isFullyResolvedNamespace(eq(catalogName), eq(namespace)))
        .thenReturn(true);
    when(syntheticPathWrapper.getRawLeafEntity()).thenReturn(projectNsEntity);

    when(idResult.getId()).thenReturn(6L);
    when(metaStoreManager.generateNewEntityId(any())).thenReturn(idResult);
    PolarisEntity tableEntity =
        createEntity(
            "test-table", PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_TABLE, 6L, 5L);
    EntityResult tableCreateResult = mock(EntityResult.class);
    when(tableCreateResult.isSuccess()).thenReturn(true);
    when(tableCreateResult.getEntity()).thenReturn(tableEntity);
    when(metaStoreManager.createEntityIfNotExists(any(), any(), any()))
        .thenReturn(tableCreateResult);

    PolarisResolvedPathWrapper tablePathWrapper = mock(PolarisResolvedPathWrapper.class);
    when(tablePathWrapper.getRawLeafEntity()).thenReturn(tableEntity);
    when(resolutionManifest.getResolvedPath(
            identifier, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ANY_SUBTYPE))
        .thenReturn(tablePathWrapper);

    PrivilegeResult successResult = mock(PrivilegeResult.class);
    when(successResult.isSuccess()).thenReturn(true);
    when(metaStoreManager.grantPrivilegeOnSecurableToRole(any(), any(), any(), any(), any()))
        .thenReturn(successResult);

    boolean result =
        adminService.grantPrivilegeOnTableToRole(
            catalogName, catalogRoleName, identifier, privilege);
    assertThat(result).isTrue();
  }

  @Test
  void testGrantPrivilegeOnTableLikeToRole_SyntheticEntityCreationFails() throws Exception {
    String catalogName = "test-catalog";
    String catalogRoleName = "test-role";
    TableIdentifier identifier = TableIdentifier.of(Namespace.empty(), "test-table");
    PolarisPrivilege privilege = PolarisPrivilege.TABLE_WRITE_DATA;

    PolarisEntity catalogEntity = createEntity(catalogName, PolarisEntityType.CATALOG);
    PolarisResolvedPathWrapper catalogWrapper = mock(PolarisResolvedPathWrapper.class);
    when(catalogWrapper.getRawLeafEntity()).thenReturn(catalogEntity);
    when(resolutionManifest.getResolvedReferenceCatalogEntity()).thenReturn(catalogWrapper);
    when(resolutionManifest.getIsPassthroughFacade()).thenReturn(true);

    PolarisResolvedPathWrapper catalogRoleWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisEntity catalogRoleEntity = createEntity(catalogRoleName, PolarisEntityType.CATALOG_ROLE);
    when(catalogRoleWrapper.getRawLeafEntity()).thenReturn(catalogRoleEntity);
    when(resolutionManifest.getResolvedPath(eq(catalogRoleName))).thenReturn(catalogRoleWrapper);

    PolarisResolvedPathWrapper existingPathWrapper = mock(PolarisResolvedPathWrapper.class);
    when(existingPathWrapper.getRawFullPath()).thenReturn(List.of(catalogEntity));
    when(resolutionManifest.getResolvedPath(
            identifier, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ANY_SUBTYPE))
        .thenReturn(existingPathWrapper);
    when(existingPathWrapper.getRawLeafEntity()).thenReturn(catalogEntity);

    GenerateEntityIdResult idResult = mock(GenerateEntityIdResult.class);
    when(idResult.getId()).thenReturn(3L);
    when(metaStoreManager.generateNewEntityId(any())).thenReturn(idResult);
    EntityResult tableCreateResult = mock(EntityResult.class);
    when(metaStoreManager.createEntityIfNotExists(any(), any(), any()))
        .thenReturn(tableCreateResult);
    when(tableCreateResult.isSuccess()).thenReturn(false);

    when(resolutionManifest.getResolvedPath(identifier)).thenReturn(existingPathWrapper);
    when(existingPathWrapper.getRawLeafEntity()).thenReturn(catalogEntity);

    assertThatThrownBy(
            () ->
                adminService.grantPrivilegeOnTableToRole(
                    catalogName, catalogRoleName, identifier, privilege))
        .isInstanceOf(RuntimeException.class)
        .hasMessage(
            "Failed to create or find table entity 'test-table' in federated catalog 'test-catalog'");
  }

  private PolarisEntity createEntity(String name, PolarisEntityType type) {
    return new PolarisEntity.Builder()
        .setName(name)
        .setType(type)
        .setId(1L)
        .setCatalogId(1L)
        .setCreateTimestamp(System.currentTimeMillis())
        .build();
  }

  private PolarisEntity createEntity(String name, PolarisEntityType type, long id) {
    return new PolarisEntity.Builder()
        .setName(name)
        .setType(type)
        .setId(id)
        .setCatalogId(1L)
        .setCreateTimestamp(System.currentTimeMillis())
        .build();
  }

  private PolarisEntity createEntity(String name, PolarisEntityType type, long id, long parentId) {
    return new PolarisEntity.Builder()
        .setName(name)
        .setType(type)
        .setId(id)
        .setCatalogId(1L)
        .setParentId(parentId)
        .setCreateTimestamp(System.currentTimeMillis())
        .build();
  }

  private PolarisEntity createEntity(
      String name, PolarisEntityType type, PolarisEntitySubType subType, long id, long parentId) {
    return new PolarisEntity.Builder()
        .setName(name)
        .setType(type)
        .setSubType(subType)
        .setId(id)
        .setCatalogId(1L)
        .setParentId(parentId)
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
    PolarisResolvedPathWrapper catalogWrapper = mock(PolarisResolvedPathWrapper.class);
    when(catalogWrapper.getRawLeafEntity()).thenReturn(catalogEntity);
    when(resolutionManifest.getResolvedReferenceCatalogEntity()).thenReturn(catalogWrapper);

    PolarisResolvedPathWrapper catalogRoleWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisEntity catalogRoleEntity =
        createEntity(catalogRoleName, PolarisEntityType.CATALOG_ROLE, 2L);
    when(catalogRoleWrapper.getRawLeafEntity()).thenReturn(catalogRoleEntity);
    when(resolutionManifest.getResolvedPath(eq(catalogRoleName))).thenReturn(catalogRoleWrapper);

    PolarisEntity namespaceEntity =
        createEntity(namespace.levels()[0], PolarisEntityType.NAMESPACE, 3L, 1L);
    List<PolarisEntity> fullPath = List.of(catalogEntity, namespaceEntity);
    when(resolvedPathWrapper.getRawFullPath()).thenReturn(fullPath);
    when(resolvedPathWrapper.getRawParentPath()).thenReturn(List.of(catalogEntity));
    when(resolvedPathWrapper.getRawLeafEntity()).thenReturn(namespaceEntity);
    when(resolvedPathWrapper.isFullyResolvedNamespace(eq(catalogName), eq(namespace)))
        .thenReturn(true);
    when(resolutionManifest.getResolvedPath(eq(namespace))).thenReturn(resolvedPathWrapper);
  }
}
