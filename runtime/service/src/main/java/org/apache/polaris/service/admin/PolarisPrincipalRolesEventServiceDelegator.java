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
import org.apache.polaris.service.events.PrincipalRolesServiceEvents;
import org.apache.polaris.service.events.listeners.PolarisEventListener;

@Decorator
@Priority(1000)
public class PolarisPrincipalRolesEventServiceDelegator implements PolarisPrincipalRolesApiService {

  @Inject @Delegate PolarisPrincipalRolesApiService delegate;
  @Inject PolarisEventListener polarisEventListener;

  @Override
  public Response createPrincipalRole(
      CreatePrincipalRoleRequest request,
      RealmContext realmContext,
      SecurityContext securityContext) {
    // TODO: After changing the API response, we should change this to emit the corresponding event.
    return delegate.createPrincipalRole(request, realmContext, securityContext);
  }

  @Override
  public Response deletePrincipalRole(
      String principalRoleName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onBeforeDeletePrincipalRole(
        new PrincipalRolesServiceEvents.BeforeDeletePrincipalRoleEvent(principalRoleName));
    Response resp = delegate.deletePrincipalRole(principalRoleName, realmContext, securityContext);
    polarisEventListener.onAfterDeletePrincipalRole(
        new PrincipalRolesServiceEvents.AfterDeletePrincipalRoleEvent(principalRoleName));
    return resp;
  }

  @Override
  public Response getPrincipalRole(
      String principalRoleName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onBeforeGetPrincipalRole(
        new PrincipalRolesServiceEvents.BeforeGetPrincipalRoleEvent(principalRoleName));
    Response resp = delegate.getPrincipalRole(principalRoleName, realmContext, securityContext);
    polarisEventListener.onAfterGetPrincipalRole(
        new PrincipalRolesServiceEvents.AfterGetPrincipalRoleEvent(
            (PrincipalRole) resp.getEntity()));
    return resp;
  }

  @Override
  public Response updatePrincipalRole(
      String principalRoleName,
      UpdatePrincipalRoleRequest updateRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onBeforeUpdatePrincipalRole(
        new PrincipalRolesServiceEvents.BeforeUpdatePrincipalRoleEvent(
            principalRoleName, updateRequest));
    Response resp =
        delegate.updatePrincipalRole(
            principalRoleName, updateRequest, realmContext, securityContext);
    polarisEventListener.onAfterUpdatePrincipalRole(
        new PrincipalRolesServiceEvents.AfterUpdatePrincipalRoleEvent(
            (PrincipalRole) resp.getEntity()));
    return resp;
  }

  @Override
  public Response listPrincipalRoles(RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onBeforeListPrincipalRoles(
        new PrincipalRolesServiceEvents.BeforeListPrincipalRolesEvent());
    Response resp = delegate.listPrincipalRoles(realmContext, securityContext);
    polarisEventListener.onAfterListPrincipalRoles(
        new PrincipalRolesServiceEvents.AfterListPrincipalRolesEvent());
    return resp;
  }

  @Override
  public Response assignCatalogRoleToPrincipalRole(
      String principalRoleName,
      String catalogName,
      GrantCatalogRoleRequest request,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onBeforeAssignCatalogRoleToPrincipalRole(
        new PrincipalRolesServiceEvents.BeforeAssignCatalogRoleToPrincipalRoleEvent(
            principalRoleName, catalogName, request.getCatalogRole().getName()));
    Response resp =
        delegate.assignCatalogRoleToPrincipalRole(
            principalRoleName, catalogName, request, realmContext, securityContext);
    polarisEventListener.onAfterAssignCatalogRoleToPrincipalRole(
        new PrincipalRolesServiceEvents.AfterAssignCatalogRoleToPrincipalRoleEvent(
            principalRoleName, catalogName, request.getCatalogRole().getName()));
    return resp;
  }

  @Override
  public Response revokeCatalogRoleFromPrincipalRole(
      String principalRoleName,
      String catalogName,
      String catalogRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onBeforeRevokeCatalogRoleFromPrincipalRole(
        new PrincipalRolesServiceEvents.BeforeRevokeCatalogRoleFromPrincipalRoleEvent(
            principalRoleName, catalogName, catalogRoleName));
    Response resp =
        delegate.revokeCatalogRoleFromPrincipalRole(
            principalRoleName, catalogName, catalogRoleName, realmContext, securityContext);
    polarisEventListener.onAfterRevokeCatalogRoleFromPrincipalRole(
        new PrincipalRolesServiceEvents.AfterRevokeCatalogRoleFromPrincipalRoleEvent(
            principalRoleName, catalogName, catalogRoleName));
    return resp;
  }

  @Override
  public Response listAssigneePrincipalsForPrincipalRole(
      String principalRoleName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onBeforeListAssigneePrincipalsForPrincipalRole(
        new PrincipalRolesServiceEvents.BeforeListAssigneePrincipalsForPrincipalRoleEvent(
            principalRoleName));
    Response resp =
        delegate.listAssigneePrincipalsForPrincipalRole(
            principalRoleName, realmContext, securityContext);
    polarisEventListener.onAfterListAssigneePrincipalsForPrincipalRole(
        new PrincipalRolesServiceEvents.AfterListAssigneePrincipalsForPrincipalRoleEvent(
            principalRoleName));
    return resp;
  }

  @Override
  public Response listCatalogRolesForPrincipalRole(
      String principalRoleName,
      String catalogName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onBeforeListCatalogRolesForPrincipalRole(
        new PrincipalRolesServiceEvents.BeforeListCatalogRolesForPrincipalRoleEvent(
            principalRoleName, catalogName));
    Response resp =
        delegate.listCatalogRolesForPrincipalRole(
            principalRoleName, catalogName, realmContext, securityContext);
    polarisEventListener.onAfterListCatalogRolesForPrincipalRole(
        new PrincipalRolesServiceEvents.AfterListCatalogRolesForPrincipalRoleEvent(
            principalRoleName, catalogName));
    return resp;
  }
}
