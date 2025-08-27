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
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.polaris.core.admin.model.CreatePrincipalRequest;
import org.apache.polaris.core.admin.model.GrantPrincipalRoleRequest;
import org.apache.polaris.core.admin.model.UpdatePrincipalRequest;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.admin.api.PolarisPrincipalsApiService;

@Decorator
@Priority(1000)
@Alternative
public class PolarisPrincipalsEventServiceDelegator implements PolarisPrincipalsApiService {

  @Inject @Delegate PolarisPrincipalsApiService delegate;

  @Override
  public Response createPrincipal(
      CreatePrincipalRequest request, RealmContext realmContext, SecurityContext securityContext) {
    return delegate.createPrincipal(request, realmContext, securityContext);
  }

  @Override
  public Response deletePrincipal(
      String principalName, RealmContext realmContext, SecurityContext securityContext) {
    return delegate.deletePrincipal(principalName, realmContext, securityContext);
  }

  @Override
  public Response getPrincipal(
      String principalName, RealmContext realmContext, SecurityContext securityContext) {
    return delegate.getPrincipal(principalName, realmContext, securityContext);
  }

  @Override
  public Response updatePrincipal(
      String principalName,
      UpdatePrincipalRequest updateRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return delegate.updatePrincipal(principalName, updateRequest, realmContext, securityContext);
  }

  @Override
  public Response rotateCredentials(
      String principalName, RealmContext realmContext, SecurityContext securityContext) {
    return delegate.rotateCredentials(principalName, realmContext, securityContext);
  }

  @Override
  public Response listPrincipals(RealmContext realmContext, SecurityContext securityContext) {
    return delegate.listPrincipals(realmContext, securityContext);
  }

  @Override
  public Response assignPrincipalRole(
      String principalName,
      GrantPrincipalRoleRequest request,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return delegate.assignPrincipalRole(principalName, request, realmContext, securityContext);
  }

  @Override
  public Response revokePrincipalRole(
      String principalName,
      String principalRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return delegate.revokePrincipalRole(
        principalName, principalRoleName, realmContext, securityContext);
  }

  @Override
  public Response listPrincipalRolesAssigned(
      String principalName, RealmContext realmContext, SecurityContext securityContext) {
    return delegate.listPrincipalRolesAssigned(principalName, realmContext, securityContext);
  }
}
