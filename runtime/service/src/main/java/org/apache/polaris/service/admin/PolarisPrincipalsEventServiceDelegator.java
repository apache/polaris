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
import org.apache.polaris.core.admin.model.UpdatePrincipalRequest;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.admin.api.PolarisPrincipalsApiService;
import org.apache.polaris.service.events.PolarisEventListener;
import org.apache.polaris.service.events.PrincipalsServiceEvents;

@Decorator
@Priority(1000)
public class PolarisPrincipalsEventServiceDelegator implements PolarisPrincipalsApiService {

  @Inject @Delegate PolarisPrincipalsApiService delegate;
  @Inject PolarisEventListener polarisEventListener;

  @Override
  public Response createPrincipal(
      CreatePrincipalRequest request, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onBeforePrincipalCreate(new PrincipalsServiceEvents.BeforePrincipalCreateEvent(request.getPrincipal().getName()));
    Response resp = delegate.createPrincipal(request, realmContext, securityContext);
    polarisEventListener.onAfterPrincipalCreate(new PrincipalsServiceEvents.AfterPrincipalCreateEvent(resp.readEntity(Principal.class)));
    return resp;
  }

  @Override
  public Response deletePrincipal(
      String principalName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onBeforePrincipalDelete(new PrincipalsServiceEvents.BeforePrincipalDeleteEvent(principalName));
    Response resp = delegate.deletePrincipal(principalName, realmContext, securityContext);
    polarisEventListener.onAfterPrincipalDelete(new PrincipalsServiceEvents.AfterPrincipalDeleteEvent(principalName));
    return resp;
  }

  @Override
  public Response getPrincipal(
      String principalName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onBeforePrincipalGet(new PrincipalsServiceEvents.BeforePrincipalGetEvent(principalName));
    Response resp = delegate.getPrincipal(principalName, realmContext, securityContext);
    polarisEventListener.onAfterPrincipalGet(new PrincipalsServiceEvents.AfterPrincipalGetEvent(resp.readEntity(Principal.class)));
    return resp;
  }

  @Override
  public Response updatePrincipal(
      String principalName,
      UpdatePrincipalRequest updateRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onBeforePrincipalUpdate(new PrincipalsServiceEvents.BeforePrincipalUpdateEvent(principalName, updateRequest));
    Response resp = delegate.updatePrincipal(principalName, updateRequest, realmContext, securityContext);
    polarisEventListener.onAfterPrincipalUpdate(new PrincipalsServiceEvents.AfterPrincipalUpdateEvent(resp.readEntity(Principal.class)));
    return resp;
  }

  @Override
  public Response rotateCredentials(
      String principalName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onBeforeCredentialsRotate(new PrincipalsServiceEvents.BeforeCredentialsRotateEvent(principalName));
    Response resp = delegate.rotateCredentials(principalName, realmContext, securityContext);
    PrincipalWithCredentials principalWithCredentials = resp.readEntity(PrincipalWithCredentials.class);
    polarisEventListener.onAfterCredentialsRotate(new PrincipalsServiceEvents.AfterCredentialsRotateEvent(principalWithCredentials.getPrincipal()));
    return resp;
  }

  @Override
  public Response listPrincipals(RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onBeforePrincipalsList(new PrincipalsServiceEvents.BeforePrincipalsListEvent());
    Response resp = delegate.listPrincipals(realmContext, securityContext);
    polarisEventListener.onAfterPrincipalsList(new PrincipalsServiceEvents.AfterPrincipalsListEvent());
    return resp;
  }

  @Override
  public Response assignPrincipalRole(
      String principalName,
      GrantPrincipalRoleRequest request,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onBeforeAssignPrincipalRole(new PrincipalsServiceEvents.BeforeAssignPrincipalRoleEvent(principalName, request.getPrincipalRole()));
    Response resp = delegate.assignPrincipalRole(principalName, request, realmContext, securityContext);
    polarisEventListener.onAfterAssignPrincipalRole(new PrincipalsServiceEvents.AfterAssignPrincipalRoleEvent(principalName, request.getPrincipalRole()));
    return resp;
  }

  @Override
  public Response revokePrincipalRole(
      String principalName,
      String principalRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onBeforeRevokePrincipalRole(new PrincipalsServiceEvents.BeforeRevokePrincipalRoleEvent(principalName, principalRoleName));
    Response resp = delegate.revokePrincipalRole(principalName, principalRoleName, realmContext, securityContext);
    polarisEventListener.onAfterRevokePrincipalRole(new PrincipalsServiceEvents.AfterRevokePrincipalRoleEvent(principalName, principalRoleName));
    return resp;
  }

  @Override
  public Response listPrincipalRolesAssigned(
      String principalName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onBeforePrincipalRolesAssignedList(new PrincipalsServiceEvents.BeforePrincipalRolesAssignedListEvent(principalName));
    Response resp = delegate.listPrincipalRolesAssigned(principalName, realmContext, securityContext);
    polarisEventListener.onAfterPrincipalRolesAssignedList(new PrincipalsServiceEvents.AfterPrincipalRolesAssignedListEvent(principalName));
    return resp;
  }
}
