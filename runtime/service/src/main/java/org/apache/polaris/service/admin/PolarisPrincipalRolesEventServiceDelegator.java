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
import org.apache.polaris.core.admin.model.CreatePrincipalRoleRequest;
import org.apache.polaris.core.admin.model.GrantCatalogRoleRequest;
import org.apache.polaris.core.admin.model.UpdatePrincipalRoleRequest;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.admin.api.PolarisPrincipalRolesApiService;

@Decorator
@Priority(1000)
@Alternative
public class PolarisPrincipalRolesEventServiceDelegator implements PolarisPrincipalRolesApiService {

  @Inject @Delegate PolarisPrincipalRolesApiService delegate;

  @Override
  public Response createPrincipalRole(
      CreatePrincipalRoleRequest request,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return delegate.createPrincipalRole(request, realmContext, securityContext);
  }

  @Override
  public Response deletePrincipalRole(
      String principalRoleName, RealmContext realmContext, SecurityContext securityContext) {
    return delegate.deletePrincipalRole(principalRoleName, realmContext, securityContext);
  }

  @Override
  public Response getPrincipalRole(
      String principalRoleName, RealmContext realmContext, SecurityContext securityContext) {
    return delegate.getPrincipalRole(principalRoleName, realmContext, securityContext);
  }

  @Override
  public Response updatePrincipalRole(
      String principalRoleName,
      UpdatePrincipalRoleRequest updateRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return delegate.updatePrincipalRole(
        principalRoleName, updateRequest, realmContext, securityContext);
  }

  @Override
  public Response listPrincipalRoles(RealmContext realmContext, SecurityContext securityContext) {
    return delegate.listPrincipalRoles(realmContext, securityContext);
  }

  @Override
  public Response assignCatalogRoleToPrincipalRole(
      String principalRoleName,
      String catalogName,
      GrantCatalogRoleRequest request,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return delegate.assignCatalogRoleToPrincipalRole(
        principalRoleName, catalogName, request, realmContext, securityContext);
  }

  @Override
  public Response revokeCatalogRoleFromPrincipalRole(
      String principalRoleName,
      String catalogName,
      String catalogRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return delegate.revokeCatalogRoleFromPrincipalRole(
        principalRoleName, catalogName, catalogRoleName, realmContext, securityContext);
  }

  @Override
  public Response listAssigneePrincipalsForPrincipalRole(
      String principalRoleName, RealmContext realmContext, SecurityContext securityContext) {
    return delegate.listAssigneePrincipalsForPrincipalRole(
        principalRoleName, realmContext, securityContext);
  }

  @Override
  public Response listCatalogRolesForPrincipalRole(
      String principalRoleName,
      String catalogName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return delegate.listCatalogRolesForPrincipalRole(
        principalRoleName, catalogName, realmContext, securityContext);
  }
}
