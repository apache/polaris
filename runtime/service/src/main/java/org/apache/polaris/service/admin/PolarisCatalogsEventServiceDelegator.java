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
import org.apache.polaris.core.admin.model.AddGrantRequest;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.CreateCatalogRoleRequest;
import org.apache.polaris.core.admin.model.RevokeGrantRequest;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.admin.model.UpdateCatalogRoleRequest;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.admin.api.PolarisCatalogsApiService;

@Decorator
@Priority(1000)
public class PolarisCatalogsEventServiceDelegator implements PolarisCatalogsApiService {

  @Inject @Delegate PolarisCatalogsApiService delegate;

  @Override
  public Response createCatalog(
      CreateCatalogRequest request, RealmContext realmContext, SecurityContext securityContext) {
    return delegate.createCatalog(request, realmContext, securityContext);
  }

  @Override
  public Response deleteCatalog(
      String catalogName, RealmContext realmContext, SecurityContext securityContext) {
    return delegate.deleteCatalog(catalogName, realmContext, securityContext);
  }

  @Override
  public Response getCatalog(
      String catalogName, RealmContext realmContext, SecurityContext securityContext) {
    return delegate.getCatalog(catalogName, realmContext, securityContext);
  }

  @Override
  public Response updateCatalog(
      String catalogName,
      UpdateCatalogRequest updateRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return delegate.updateCatalog(catalogName, updateRequest, realmContext, securityContext);
  }

  @Override
  public Response listCatalogs(RealmContext realmContext, SecurityContext securityContext) {
    return delegate.listCatalogs(realmContext, securityContext);
  }

  @Override
  public Response createCatalogRole(
      String catalogName,
      CreateCatalogRoleRequest request,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return delegate.createCatalogRole(catalogName, request, realmContext, securityContext);
  }

  @Override
  public Response deleteCatalogRole(
      String catalogName,
      String catalogRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return delegate.deleteCatalogRole(catalogName, catalogRoleName, realmContext, securityContext);
  }

  @Override
  public Response getCatalogRole(
      String catalogName,
      String catalogRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return delegate.getCatalogRole(catalogName, catalogRoleName, realmContext, securityContext);
  }

  @Override
  public Response updateCatalogRole(
      String catalogName,
      String catalogRoleName,
      UpdateCatalogRoleRequest updateRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return delegate.updateCatalogRole(
        catalogName, catalogRoleName, updateRequest, realmContext, securityContext);
  }

  @Override
  public Response listCatalogRoles(
      String catalogName, RealmContext realmContext, SecurityContext securityContext) {
    return delegate.listCatalogRoles(catalogName, realmContext, securityContext);
  }

  @Override
  public Response addGrantToCatalogRole(
      String catalogName,
      String catalogRoleName,
      AddGrantRequest grantRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return delegate.addGrantToCatalogRole(
        catalogName, catalogRoleName, grantRequest, realmContext, securityContext);
  }

  @Override
  public Response revokeGrantFromCatalogRole(
      String catalogName,
      String catalogRoleName,
      Boolean cascade,
      RevokeGrantRequest grantRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return delegate.revokeGrantFromCatalogRole(
        catalogName, catalogRoleName, cascade, grantRequest, realmContext, securityContext);
  }

  @Override
  public Response listAssigneePrincipalRolesForCatalogRole(
      String catalogName,
      String catalogRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return delegate.listAssigneePrincipalRolesForCatalogRole(
        catalogName, catalogRoleName, realmContext, securityContext);
  }

  @Override
  public Response listGrantsForCatalogRole(
      String catalogName,
      String catalogRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return delegate.listGrantsForCatalogRole(
        catalogName, catalogRoleName, realmContext, securityContext);
  }
}
