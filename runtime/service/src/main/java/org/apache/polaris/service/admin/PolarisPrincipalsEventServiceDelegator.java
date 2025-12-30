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
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.events.PolarisEventType;
import org.apache.polaris.service.events.listeners.PolarisEventListener;

@Decorator
@Priority(1000)
public class PolarisPrincipalsEventServiceDelegator implements PolarisPrincipalsApiService {

  @Inject @Delegate PolarisPrincipalsApiService delegate;
  @Inject PolarisEventListener polarisEventListener;
  @Inject PolarisEventMetadataFactory eventMetadataFactory;

  @Override
  public Response createPrincipal(
      CreatePrincipalRequest request, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.BEFORE_CREATE_PRINCIPAL, eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_NAME, request.getPrincipal().getName())
            .build());
    Response resp = delegate.createPrincipal(request, realmContext, securityContext);
    polarisEventListener.onEvent(
        PolarisEvent.builder(PolarisEventType.AFTER_CREATE_PRINCIPAL, eventMetadataFactory.create())
            .attribute(
                EventAttributes.PRINCIPAL,
                ((PrincipalWithCredentials) resp.getEntity()).getPrincipal())
            .build());
    return resp;
  }

  @Override
  public Response resetCredentials(
      String principalName,
      ResetPrincipalRequest resetPrincipalRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.BEFORE_RESET_CREDENTIALS, eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_NAME, principalName)
            .build());
    Response resp =
        delegate.resetCredentials(
            principalName, resetPrincipalRequest, realmContext, securityContext);
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.AFTER_RESET_CREDENTIALS, eventMetadataFactory.create())
            .attribute(
                EventAttributes.PRINCIPAL,
                ((PrincipalWithCredentials) resp.getEntity()).getPrincipal())
            .build());
    return resp;
  }

  @Override
  public Response deletePrincipal(
      String principalName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.BEFORE_DELETE_PRINCIPAL, eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_NAME, principalName)
            .build());
    Response resp = delegate.deletePrincipal(principalName, realmContext, securityContext);
    polarisEventListener.onEvent(
        PolarisEvent.builder(PolarisEventType.AFTER_DELETE_PRINCIPAL, eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_NAME, principalName)
            .build());
    return resp;
  }

  @Override
  public Response getPrincipal(
      String principalName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onEvent(
        PolarisEvent.builder(PolarisEventType.BEFORE_GET_PRINCIPAL, eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_NAME, principalName)
            .build());
    Response resp = delegate.getPrincipal(principalName, realmContext, securityContext);
    polarisEventListener.onEvent(
        PolarisEvent.builder(PolarisEventType.AFTER_GET_PRINCIPAL, eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL, (Principal) resp.getEntity())
            .build());
    return resp;
  }

  @Override
  public Response updatePrincipal(
      String principalName,
      UpdatePrincipalRequest updateRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.BEFORE_UPDATE_PRINCIPAL, eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_NAME, principalName)
            .attribute(EventAttributes.UPDATE_PRINCIPAL_REQUEST, updateRequest)
            .build());
    Response resp =
        delegate.updatePrincipal(principalName, updateRequest, realmContext, securityContext);
    polarisEventListener.onEvent(
        PolarisEvent.builder(PolarisEventType.AFTER_UPDATE_PRINCIPAL, eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL, (Principal) resp.getEntity())
            .build());
    return resp;
  }

  @Override
  public Response rotateCredentials(
      String principalName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.BEFORE_ROTATE_CREDENTIALS, eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_NAME, principalName)
            .build());
    Response resp = delegate.rotateCredentials(principalName, realmContext, securityContext);
    PrincipalWithCredentials principalWithCredentials = (PrincipalWithCredentials) resp.getEntity();
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.AFTER_ROTATE_CREDENTIALS, eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL, principalWithCredentials.getPrincipal())
            .build());
    return resp;
  }

  @Override
  public Response listPrincipals(RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onEvent(
        PolarisEvent.builder(PolarisEventType.BEFORE_LIST_PRINCIPALS, eventMetadataFactory.create())
            .build());
    Response resp = delegate.listPrincipals(realmContext, securityContext);
    polarisEventListener.onEvent(
        PolarisEvent.builder(PolarisEventType.AFTER_LIST_PRINCIPALS, eventMetadataFactory.create())
            .build());
    return resp;
  }

  @Override
  public Response assignPrincipalRole(
      String principalName,
      GrantPrincipalRoleRequest request,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.BEFORE_ASSIGN_PRINCIPAL_ROLE, eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_NAME, principalName)
            .attribute(EventAttributes.PRINCIPAL_ROLE, request.getPrincipalRole())
            .build());
    Response resp =
        delegate.assignPrincipalRole(principalName, request, realmContext, securityContext);
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.AFTER_ASSIGN_PRINCIPAL_ROLE, eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_NAME, principalName)
            .attribute(EventAttributes.PRINCIPAL_ROLE, request.getPrincipalRole())
            .build());
    return resp;
  }

  @Override
  public Response revokePrincipalRole(
      String principalName,
      String principalRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.BEFORE_REVOKE_PRINCIPAL_ROLE, eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_NAME, principalName)
            .attribute(EventAttributes.PRINCIPAL_ROLE_NAME, principalRoleName)
            .build());
    Response resp =
        delegate.revokePrincipalRole(
            principalName, principalRoleName, realmContext, securityContext);
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.AFTER_REVOKE_PRINCIPAL_ROLE, eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_NAME, principalName)
            .attribute(EventAttributes.PRINCIPAL_ROLE_NAME, principalRoleName)
            .build());
    return resp;
  }

  @Override
  public Response listPrincipalRolesAssigned(
      String principalName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.BEFORE_LIST_ASSIGNED_PRINCIPAL_ROLES,
                eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_NAME, principalName)
            .build());
    Response resp =
        delegate.listPrincipalRolesAssigned(principalName, realmContext, securityContext);
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.AFTER_LIST_ASSIGNED_PRINCIPAL_ROLES, eventMetadataFactory.create())
            .attribute(EventAttributes.PRINCIPAL_NAME, principalName)
            .build());
    return resp;
  }
}
