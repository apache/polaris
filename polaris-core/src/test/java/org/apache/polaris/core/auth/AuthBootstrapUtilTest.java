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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.GenerateEntityIdResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class AuthBootstrapUtilTest {

  @Mock private PolarisMetaStoreManager metaStoreManager;
  @Mock private PolarisCallContext callContext;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  void testEnsurePrincipalRoleViewerExists_WhenRoleAlreadyExists() {
    // Given: principal_role_viewer role already exists
    PrincipalRoleEntity existingRole =
        new PrincipalRoleEntity.Builder()
            .setId(100L)
            .setName(PolarisEntityConstants.getNameOfPrincipalRoleViewerRole())
            .build();
    EntityResult existingRoleResult = new EntityResult(existingRole);

    when(metaStoreManager.readEntityByName(
            eq(callContext),
            isNull(),
            eq(PolarisEntityType.PRINCIPAL_ROLE),
            eq(PolarisEntitySubType.NULL_SUBTYPE),
            eq(PolarisEntityConstants.getNameOfPrincipalRoleViewerRole())))
        .thenReturn(existingRoleResult);

    // When: we ensure the role exists
    AuthBootstrapUtil.ensurePrincipalRoleViewerExists(metaStoreManager, callContext);

    // Then: role creation should be skipped
    verify(metaStoreManager, never()).createEntityIfNotExists(any(), any(), any());
    verify(metaStoreManager, never())
        .grantPrivilegeOnSecurableToRole(any(), any(), any(), any(), any());
  }

  @Test
  void testEnsurePrincipalRoleViewerExists_WhenRoleDoesNotExist() {
    // Given: principal_role_viewer role does not exist
    EntityResult notFoundResult =
        new EntityResult(EntityResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    when(metaStoreManager.readEntityByName(
            eq(callContext),
            isNull(),
            eq(PolarisEntityType.PRINCIPAL_ROLE),
            eq(PolarisEntitySubType.NULL_SUBTYPE),
            eq(PolarisEntityConstants.getNameOfPrincipalRoleViewerRole())))
        .thenReturn(notFoundResult);

    // Root container exists
    PolarisBaseEntity rootContainer =
        new PolarisBaseEntity(
            0L,
            1L,
            PolarisEntityType.ROOT,
            PolarisEntitySubType.NULL_SUBTYPE,
            1L,
            PolarisEntityConstants.getRootContainerName());
    EntityResult rootContainerResult = new EntityResult(rootContainer);
    when(metaStoreManager.readEntityByName(
            eq(callContext),
            isNull(),
            eq(PolarisEntityType.ROOT),
            eq(PolarisEntitySubType.NULL_SUBTYPE),
            eq(PolarisEntityConstants.getRootContainerName())))
        .thenReturn(rootContainerResult);

    // ID generation works
    when(metaStoreManager.generateNewEntityId(eq(callContext)))
        .thenReturn(new GenerateEntityIdResult(200L));

    // When: we ensure the role exists
    AuthBootstrapUtil.ensurePrincipalRoleViewerExists(metaStoreManager, callContext);

    // Then: role should be created and privilege granted
    verify(metaStoreManager, times(1))
        .createEntityIfNotExists(eq(callContext), isNull(), any(PrincipalRoleEntity.class));
    verify(metaStoreManager, times(1))
        .grantPrivilegeOnSecurableToRole(
            eq(callContext),
            any(PrincipalRoleEntity.class),
            isNull(),
            eq(rootContainer),
            eq(PolarisPrivilege.PRINCIPAL_ROLE_LIST));
  }

  @Test
  void testEnsurePrincipalRoleViewerExists_IsIdempotent() {
    // Given: role exists on first call, then doesn't exist on second call (hypothetical)
    PrincipalRoleEntity existingRole =
        new PrincipalRoleEntity.Builder()
            .setId(100L)
            .setName(PolarisEntityConstants.getNameOfPrincipalRoleViewerRole())
            .build();
    EntityResult existingRoleResult = new EntityResult(existingRole);

    when(metaStoreManager.readEntityByName(
            eq(callContext),
            isNull(),
            eq(PolarisEntityType.PRINCIPAL_ROLE),
            eq(PolarisEntitySubType.NULL_SUBTYPE),
            eq(PolarisEntityConstants.getNameOfPrincipalRoleViewerRole())))
        .thenReturn(existingRoleResult);

    // When: we call it multiple times
    AuthBootstrapUtil.ensurePrincipalRoleViewerExists(metaStoreManager, callContext);
    AuthBootstrapUtil.ensurePrincipalRoleViewerExists(metaStoreManager, callContext);
    AuthBootstrapUtil.ensurePrincipalRoleViewerExists(metaStoreManager, callContext);

    // Then: role creation should never happen
    verify(metaStoreManager, never()).createEntityIfNotExists(any(), any(), any());
  }

  @Test
  void testEnsurePrincipalRoleViewerExists_UsesCorrectRoleName() {
    // Given: role doesn't exist
    EntityResult notFoundResult =
        new EntityResult(EntityResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    when(metaStoreManager.readEntityByName(
            eq(callContext),
            isNull(),
            eq(PolarisEntityType.PRINCIPAL_ROLE),
            eq(PolarisEntitySubType.NULL_SUBTYPE),
            eq(PolarisEntityConstants.getNameOfPrincipalRoleViewerRole())))
        .thenReturn(notFoundResult);

    // Root container exists
    PolarisBaseEntity rootContainer =
        new PolarisBaseEntity(
            0L,
            1L,
            PolarisEntityType.ROOT,
            PolarisEntitySubType.NULL_SUBTYPE,
            1L,
            PolarisEntityConstants.getRootContainerName());
    EntityResult rootContainerResult = new EntityResult(rootContainer);
    when(metaStoreManager.readEntityByName(
            eq(callContext),
            isNull(),
            eq(PolarisEntityType.ROOT),
            eq(PolarisEntitySubType.NULL_SUBTYPE),
            eq(PolarisEntityConstants.getRootContainerName())))
        .thenReturn(rootContainerResult);

    when(metaStoreManager.generateNewEntityId(eq(callContext)))
        .thenReturn(new GenerateEntityIdResult(200L));

    // When: we ensure the role exists
    AuthBootstrapUtil.ensurePrincipalRoleViewerExists(metaStoreManager, callContext);

    // Then: verify correct role name is used
    verify(metaStoreManager)
        .createEntityIfNotExists(
            eq(callContext),
            isNull(),
            argThat(
                entity ->
                    entity instanceof PrincipalRoleEntity
                        && entity
                            .getName()
                            .equals(PolarisEntityConstants.getNameOfPrincipalRoleViewerRole())));
  }
}
