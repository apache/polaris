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
import org.apache.polaris.core.admin.model.CreatePrincipalRequest;
import org.apache.polaris.core.admin.model.GrantPrincipalRoleRequest;
import org.apache.polaris.core.admin.model.Principal;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.ResetPrincipalRequest;
import org.apache.polaris.core.admin.model.UpdatePrincipalRequest;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.admin.api.PolarisPrincipalsApiService;
import org.apache.polaris.service.events.PrincipalsServiceEvents;
import org.apache.polaris.service.events.listeners.PolarisEventListener;

@Decorator
@Priority(1000)
public class PolarisPrincipalsEventServiceDelegator implements PolarisPrincipalsApiService {

  @Inject @Delegate PolarisPrincipalsApiService delegate;
  @Inject PolarisEventListener polarisEventListener;

  @Override
  public Response createPrincipal(
      CreatePrincipalRequest request, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onBeforeCreatePrincipal(
        new PrincipalsServiceEvents.BeforeCreatePrincipalEvent(request.getPrincipal().getName()));
    Response resp = delegate.createPrincipal(request, realmContext, securityContext);
    polarisEventListener.onAfterCreatePrincipal(
        new PrincipalsServiceEvents.AfterCreatePrincipalEvent(
            ((PrincipalWithCredentials) resp.getEntity()).getPrincipal()));
    return resp;
  }

  @Override
  public Response resetCredentials(
      String principalName,
      ResetPrincipalRequest resetPrincipalRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onBeforeResetCredentials(
        new PrincipalsServiceEvents.BeforeResetCredentialsEvent(principalName));
    Response resp =
        delegate.resetCredentials(
            principalName, resetPrincipalRequest, realmContext, securityContext);
    polarisEventListener.onAfterResetCredentials(
        new PrincipalsServiceEvents.AfterResetCredentialsEvent(
            ((PrincipalWithCredentials) resp.getEntity()).getPrincipal()));
    return resp;
  }

  @Override
  public Response deletePrincipal(
      String principalName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onBeforeDeletePrincipal(
        new PrincipalsServiceEvents.BeforeDeletePrincipalEvent(principalName));
    Response resp = delegate.deletePrincipal(principalName, realmContext, securityContext);
    polarisEventListener.onAfterDeletePrincipal(
        new PrincipalsServiceEvents.AfterDeletePrincipalEvent(principalName));
    return resp;
  }

  @Override
  public Response getPrincipal(
      String principalName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onBeforeGetPrincipal(
        new PrincipalsServiceEvents.BeforeGetPrincipalEvent(principalName));
    Response resp = delegate.getPrincipal(principalName, realmContext, securityContext);
    polarisEventListener.onAfterGetPrincipal(
        new PrincipalsServiceEvents.AfterGetPrincipalEvent((Principal) resp.getEntity()));
    return resp;
  }

  @Override
  public Response updatePrincipal(
      String principalName,
      UpdatePrincipalRequest updateRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onBeforeUpdatePrincipal(
        new PrincipalsServiceEvents.BeforeUpdatePrincipalEvent(principalName, updateRequest));
    Response resp =
        delegate.updatePrincipal(principalName, updateRequest, realmContext, securityContext);
    polarisEventListener.onAfterUpdatePrincipal(
        new PrincipalsServiceEvents.AfterUpdatePrincipalEvent((Principal) resp.getEntity()));
    return resp;
  }

  @Override
  public Response rotateCredentials(
      String principalName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onBeforeRotateCredentials(
        new PrincipalsServiceEvents.BeforeRotateCredentialsEvent(principalName));
    Response resp = delegate.rotateCredentials(principalName, realmContext, securityContext);
    PrincipalWithCredentials principalWithCredentials = (PrincipalWithCredentials) resp.getEntity();
    polarisEventListener.onAfterRotateCredentials(
        new PrincipalsServiceEvents.AfterRotateCredentialsEvent(
            principalWithCredentials.getPrincipal()));
    return resp;
  }

  @Override
  public Response listPrincipals(RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onBeforeListPrincipals(
        new PrincipalsServiceEvents.BeforeListPrincipalsEvent());
    Response resp = delegate.listPrincipals(realmContext, securityContext);
    polarisEventListener.onAfterListPrincipals(
        new PrincipalsServiceEvents.AfterListPrincipalsEvent());
    return resp;
  }

  @Override
  public Response assignPrincipalRole(
      String principalName,
      GrantPrincipalRoleRequest request,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onBeforeAssignPrincipalRole(
        new PrincipalsServiceEvents.BeforeAssignPrincipalRoleEvent(
            principalName, request.getPrincipalRole()));
    Response resp =
        delegate.assignPrincipalRole(principalName, request, realmContext, securityContext);
    polarisEventListener.onAfterAssignPrincipalRole(
        new PrincipalsServiceEvents.AfterAssignPrincipalRoleEvent(
            principalName, request.getPrincipalRole()));
    return resp;
  }

  @Override
  public Response revokePrincipalRole(
      String principalName,
      String principalRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onBeforeRevokePrincipalRole(
        new PrincipalsServiceEvents.BeforeRevokePrincipalRoleEvent(
            principalName, principalRoleName));
    Response resp =
        delegate.revokePrincipalRole(
            principalName, principalRoleName, realmContext, securityContext);
    polarisEventListener.onAfterRevokePrincipalRole(
        new PrincipalsServiceEvents.AfterRevokePrincipalRoleEvent(
            principalName, principalRoleName));
    return resp;
  }

  @Override
  public Response listPrincipalRolesAssigned(
      String principalName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onBeforeListAssignedPrincipalRoles(
        new PrincipalsServiceEvents.BeforeListAssignedPrincipalRolesEvent(principalName));
    Response resp =
        delegate.listPrincipalRolesAssigned(principalName, realmContext, securityContext);
    polarisEventListener.onAfterListAssignedPrincipalRoles(
        new PrincipalsServiceEvents.AfterListAssignedPrincipalRolesEvent(principalName));
    return resp;
  }
}
