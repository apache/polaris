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
import org.apache.polaris.service.events.AttributeMap;
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
        new PolarisEvent(
            PolarisEventType.BEFORE_CREATE_PRINCIPAL_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap().put(EventAttributes.CREATE_PRINCIPAL_ROLE_REQUEST, request)));
    Response resp = delegate.createPrincipalRole(request, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_CREATE_PRINCIPAL_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.PRINCIPAL_ROLE, (PrincipalRole) resp.getEntity())));
    return resp;
  }

  @Override
  public Response deletePrincipalRole(
      String principalRoleName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_DELETE_PRINCIPAL_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap().put(EventAttributes.PRINCIPAL_ROLE_NAME, principalRoleName)));
    Response resp = delegate.deletePrincipalRole(principalRoleName, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_DELETE_PRINCIPAL_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap().put(EventAttributes.PRINCIPAL_ROLE_NAME, principalRoleName)));
    return resp;
  }

  @Override
  public Response getPrincipalRole(
      String principalRoleName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_GET_PRINCIPAL_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap().put(EventAttributes.PRINCIPAL_ROLE_NAME, principalRoleName)));
    Response resp = delegate.getPrincipalRole(principalRoleName, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_GET_PRINCIPAL_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.PRINCIPAL_ROLE, (PrincipalRole) resp.getEntity())));
    return resp;
  }

  @Override
  public Response updatePrincipalRole(
      String principalRoleName,
      UpdatePrincipalRoleRequest updateRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_UPDATE_PRINCIPAL_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.PRINCIPAL_ROLE_NAME, principalRoleName)
                .put(EventAttributes.UPDATE_PRINCIPAL_ROLE_REQUEST, updateRequest)));
    Response resp =
        delegate.updatePrincipalRole(
            principalRoleName, updateRequest, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_UPDATE_PRINCIPAL_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.PRINCIPAL_ROLE, (PrincipalRole) resp.getEntity())));
    return resp;
  }

  @Override
  public Response listPrincipalRoles(RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_LIST_PRINCIPAL_ROLES,
            eventMetadataFactory.create(),
            new AttributeMap()));
    Response resp = delegate.listPrincipalRoles(realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_LIST_PRINCIPAL_ROLES,
            eventMetadataFactory.create(),
            new AttributeMap()));
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
        new PolarisEvent(
            PolarisEventType.BEFORE_ASSIGN_CATALOG_ROLE_TO_PRINCIPAL_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.PRINCIPAL_ROLE_NAME, principalRoleName)
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.CATALOG_ROLE_NAME, request.getCatalogRole().getName())));
    Response resp =
        delegate.assignCatalogRoleToPrincipalRole(
            principalRoleName, catalogName, request, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_ASSIGN_CATALOG_ROLE_TO_PRINCIPAL_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.PRINCIPAL_ROLE_NAME, principalRoleName)
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.CATALOG_ROLE_NAME, request.getCatalogRole().getName())));
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
        new PolarisEvent(
            PolarisEventType.BEFORE_REVOKE_CATALOG_ROLE_FROM_PRINCIPAL_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.PRINCIPAL_ROLE_NAME, principalRoleName)
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.CATALOG_ROLE_NAME, catalogRoleName)));
    Response resp =
        delegate.revokeCatalogRoleFromPrincipalRole(
            principalRoleName, catalogName, catalogRoleName, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_REVOKE_CATALOG_ROLE_FROM_PRINCIPAL_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.PRINCIPAL_ROLE_NAME, principalRoleName)
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.CATALOG_ROLE_NAME, catalogRoleName)));
    return resp;
  }

  @Override
  public Response listAssigneePrincipalsForPrincipalRole(
      String principalRoleName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_LIST_ASSIGNEE_PRINCIPALS_FOR_PRINCIPAL_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap().put(EventAttributes.PRINCIPAL_ROLE_NAME, principalRoleName)));
    Response resp =
        delegate.listAssigneePrincipalsForPrincipalRole(
            principalRoleName, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_LIST_ASSIGNEE_PRINCIPALS_FOR_PRINCIPAL_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap().put(EventAttributes.PRINCIPAL_ROLE_NAME, principalRoleName)));
    return resp;
  }

  @Override
  public Response listCatalogRolesForPrincipalRole(
      String principalRoleName,
      String catalogName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_LIST_CATALOG_ROLES_FOR_PRINCIPAL_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.PRINCIPAL_ROLE_NAME, principalRoleName)
                .put(EventAttributes.CATALOG_NAME, catalogName)));
    Response resp =
        delegate.listCatalogRolesForPrincipalRole(
            principalRoleName, catalogName, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_LIST_CATALOG_ROLES_FOR_PRINCIPAL_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.PRINCIPAL_ROLE_NAME, principalRoleName)
                .put(EventAttributes.CATALOG_NAME, catalogName)));
    return resp;
  }
}
