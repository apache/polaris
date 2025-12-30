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

import jakarta.annotation.Priority;
import jakarta.decorator.Decorator;
import jakarta.decorator.Delegate;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.polaris.core.admin.model.CreatePrincipalRoleRequest;
import org.apache.polaris.core.admin.model.GrantCatalogRoleRequest;
import org.apache.polaris.core.admin.model.PrincipalRole;
import org.apache.polaris.core.admin.model.UpdatePrincipalRoleRequest;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.admin.api.PolarisPrincipalRolesApiService;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.events.PolarisEventType;
import org.apache.polaris.service.events.listeners.PolarisEventListener;

@Decorator
@Priority(1000)
public class PolarisPrincipalRolesEventServiceDelegator implements PolarisPrincipalRolesApiService {

  @Inject @Delegate PolarisPrincipalRolesApiService delegate;
  @Inject PolarisEventListener polarisEventListener;
  @Inject PolarisEventMetadataFactory eventMetadataFactory;

  @Override
  public Response createPrincipalRole(
      CreatePrincipalRoleRequest request,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.BEFORE_CREATE_PRINCIPAL_ROLE, eventMetadataFactory.create())
            .attribute(EventAttributes.CREATE_PRINCIPAL_ROLE_REQUEST, request)
            .build());
    Response resp = delegate.createPrincipalRole(request, realmContext, securityContext);
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.AFTER_CREATE_PRINCIPAL_ROLE, eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_ROLE, (PrincipalRole) resp.getEntity())
            .build());
    return resp;
  }

  @Override
  public Response deletePrincipalRole(
      String principalRoleName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.BEFORE_DELETE_PRINCIPAL_ROLE, eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_ROLE_NAME, principalRoleName)
            .build());
    Response resp = delegate.deletePrincipalRole(principalRoleName, realmContext, securityContext);
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.AFTER_DELETE_PRINCIPAL_ROLE, eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_ROLE_NAME, principalRoleName)
            .build());
    return resp;
  }

  @Override
  public Response getPrincipalRole(
      String principalRoleName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.BEFORE_GET_PRINCIPAL_ROLE, eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_ROLE_NAME, principalRoleName)
            .build());
    Response resp = delegate.getPrincipalRole(principalRoleName, realmContext, securityContext);
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.AFTER_GET_PRINCIPAL_ROLE, eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_ROLE, (PrincipalRole) resp.getEntity())
            .build());
    return resp;
  }

  @Override
  public Response updatePrincipalRole(
      String principalRoleName,
      UpdatePrincipalRoleRequest updateRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.BEFORE_UPDATE_PRINCIPAL_ROLE, eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_ROLE_NAME, principalRoleName)
            .attribute(EventAttributes.UPDATE_PRINCIPAL_ROLE_REQUEST, updateRequest)
            .build());
    Response resp =
        delegate.updatePrincipalRole(
            principalRoleName, updateRequest, realmContext, securityContext);
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.AFTER_UPDATE_PRINCIPAL_ROLE, eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_ROLE, (PrincipalRole) resp.getEntity())
            .build());
    return resp;
  }

  @Override
  public Response listPrincipalRoles(RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.BEFORE_LIST_PRINCIPAL_ROLES, eventMetadataFactory.create())
            .build());
    Response resp = delegate.listPrincipalRoles(realmContext, securityContext);
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.AFTER_LIST_PRINCIPAL_ROLES, eventMetadataFactory.create())
            .build());
    return resp;
  }

  @Override
  public Response assignCatalogRoleToPrincipalRole(
      String principalRoleName,
      String catalogName,
      GrantCatalogRoleRequest request,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.BEFORE_ASSIGN_CATALOG_ROLE_TO_PRINCIPAL_ROLE,
                eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_ROLE_NAME, principalRoleName)
            .attribute(EventAttributes.CATALOG_NAME, catalogName)
            .attribute(EventAttributes.CATALOG_ROLE_NAME, request.getCatalogRole().getName())
            .build());
    Response resp =
        delegate.assignCatalogRoleToPrincipalRole(
            principalRoleName, catalogName, request, realmContext, securityContext);
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.AFTER_ASSIGN_CATALOG_ROLE_TO_PRINCIPAL_ROLE,
                eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_ROLE_NAME, principalRoleName)
            .attribute(EventAttributes.CATALOG_NAME, catalogName)
            .attribute(EventAttributes.CATALOG_ROLE_NAME, request.getCatalogRole().getName())
            .build());
    return resp;
  }

  @Override
  public Response revokeCatalogRoleFromPrincipalRole(
      String principalRoleName,
      String catalogName,
      String catalogRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.BEFORE_REVOKE_CATALOG_ROLE_FROM_PRINCIPAL_ROLE,
                eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_ROLE_NAME, principalRoleName)
            .attribute(EventAttributes.CATALOG_NAME, catalogName)
            .attribute(EventAttributes.CATALOG_ROLE_NAME, catalogRoleName)
            .build());
    Response resp =
        delegate.revokeCatalogRoleFromPrincipalRole(
            principalRoleName, catalogName, catalogRoleName, realmContext, securityContext);
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.AFTER_REVOKE_CATALOG_ROLE_FROM_PRINCIPAL_ROLE,
                eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_ROLE_NAME, principalRoleName)
            .attribute(EventAttributes.CATALOG_NAME, catalogName)
            .attribute(EventAttributes.CATALOG_ROLE_NAME, catalogRoleName)
            .build());
    return resp;
  }

  @Override
  public Response listAssigneePrincipalsForPrincipalRole(
      String principalRoleName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.BEFORE_LIST_ASSIGNEE_PRINCIPALS_FOR_PRINCIPAL_ROLE,
                eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_ROLE_NAME, principalRoleName)
            .build());
    Response resp =
        delegate.listAssigneePrincipalsForPrincipalRole(
            principalRoleName, realmContext, securityContext);
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.AFTER_LIST_ASSIGNEE_PRINCIPALS_FOR_PRINCIPAL_ROLE,
                eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_ROLE_NAME, principalRoleName)
            .build());
    return resp;
  }

  @Override
  public Response listCatalogRolesForPrincipalRole(
      String principalRoleName,
      String catalogName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.BEFORE_LIST_CATALOG_ROLES_FOR_PRINCIPAL_ROLE,
                eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_ROLE_NAME, principalRoleName)
            .attribute(EventAttributes.CATALOG_NAME, catalogName)
            .build());
    Response resp =
        delegate.listCatalogRolesForPrincipalRole(
            principalRoleName, catalogName, realmContext, securityContext);
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.AFTER_LIST_CATALOG_ROLES_FOR_PRINCIPAL_ROLE,
                eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_ROLE_NAME, principalRoleName)
            .attribute(EventAttributes.CATALOG_NAME, catalogName)
            .build());
    return resp;
  }
}
