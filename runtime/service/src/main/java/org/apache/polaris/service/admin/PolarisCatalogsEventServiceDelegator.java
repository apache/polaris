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
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.CreateCatalogRoleRequest;
import org.apache.polaris.core.admin.model.RevokeGrantRequest;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.admin.model.UpdateCatalogRoleRequest;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.admin.api.PolarisCatalogsApiService;
import org.apache.polaris.service.events.CatalogsServiceEvents;
import org.apache.polaris.service.events.listeners.PolarisEventListener;

@Decorator
@Priority(1000)
public class PolarisCatalogsEventServiceDelegator implements PolarisCatalogsApiService {

  @Inject @Delegate PolarisCatalogsApiService delegate;
  @Inject PolarisEventListener polarisEventListener;

  @Override
  public Response createCatalog(
      CreateCatalogRequest request, RealmContext realmContext, SecurityContext securityContext) {
    // TODO: After changing the API response, we should change this to emit the corresponding event.
    return delegate.createCatalog(request, realmContext, securityContext);
  }

  @Override
  public Response deleteCatalog(
      String catalogName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onBeforeDeleteCatalog(
        new CatalogsServiceEvents.BeforeDeleteCatalogEvent(catalogName));
    Response resp = delegate.deleteCatalog(catalogName, realmContext, securityContext);
    polarisEventListener.onAfterDeleteCatalog(
        new CatalogsServiceEvents.AfterDeleteCatalogEvent(catalogName));
    return resp;
  }

  @Override
  public Response getCatalog(
      String catalogName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onBeforeGetCatalog(
        new CatalogsServiceEvents.BeforeGetCatalogEvent(catalogName));
    Response resp = delegate.getCatalog(catalogName, realmContext, securityContext);
    polarisEventListener.onAfterGetCatalog(
        new CatalogsServiceEvents.AfterGetCatalogEvent((Catalog) resp.getEntity()));
    return resp;
  }

  @Override
  public Response updateCatalog(
      String catalogName,
      UpdateCatalogRequest updateRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onBeforeUpdateCatalog(
        new CatalogsServiceEvents.BeforeUpdateCatalogEvent(catalogName, updateRequest));
    Response resp =
        delegate.updateCatalog(catalogName, updateRequest, realmContext, securityContext);
    polarisEventListener.onAfterUpdateCatalog(
        new CatalogsServiceEvents.AfterUpdateCatalogEvent((Catalog) resp.getEntity()));
    return resp;
  }

  @Override
  public Response listCatalogs(RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onBeforeListCatalogs(new CatalogsServiceEvents.BeforeListCatalogsEvent());
    Response resp = delegate.listCatalogs(realmContext, securityContext);
    polarisEventListener.onAfterListCatalogs(new CatalogsServiceEvents.AfterListCatalogsEvent());
    return resp;
  }

  @Override
  public Response createCatalogRole(
      String catalogName,
      CreateCatalogRoleRequest request,
      RealmContext realmContext,
      SecurityContext securityContext) {
    // TODO: After changing the API response, we should change this to emit the corresponding event.
    return delegate.createCatalogRole(catalogName, request, realmContext, securityContext);
  }

  @Override
  public Response deleteCatalogRole(
      String catalogName,
      String catalogRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onBeforeDeleteCatalogRole(
        new CatalogsServiceEvents.BeforeDeleteCatalogRoleEvent(catalogName, catalogRoleName));
    Response resp =
        delegate.deleteCatalogRole(catalogName, catalogRoleName, realmContext, securityContext);
    polarisEventListener.onAfterDeleteCatalogRole(
        new CatalogsServiceEvents.AfterDeleteCatalogRoleEvent(catalogName, catalogRoleName));
    return resp;
  }

  @Override
  public Response getCatalogRole(
      String catalogName,
      String catalogRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onBeforeGetCatalogRole(
        new CatalogsServiceEvents.BeforeGetCatalogRoleEvent(catalogName, catalogRoleName));
    Response resp =
        delegate.getCatalogRole(catalogName, catalogRoleName, realmContext, securityContext);
    polarisEventListener.onAfterGetCatalogRole(
        new CatalogsServiceEvents.AfterGetCatalogRoleEvent(
            catalogName, (CatalogRole) resp.getEntity()));
    return resp;
  }

  @Override
  public Response updateCatalogRole(
      String catalogName,
      String catalogRoleName,
      UpdateCatalogRoleRequest updateRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onBeforeUpdateCatalogRole(
        new CatalogsServiceEvents.BeforeUpdateCatalogRoleEvent(
            catalogName, catalogRoleName, updateRequest));
    Response resp =
        delegate.updateCatalogRole(
            catalogName, catalogRoleName, updateRequest, realmContext, securityContext);
    polarisEventListener.onAfterUpdateCatalogRole(
        new CatalogsServiceEvents.AfterUpdateCatalogRoleEvent(
            catalogName, (CatalogRole) resp.getEntity()));
    return resp;
  }

  @Override
  public Response listCatalogRoles(
      String catalogName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onAfterListCatalogRoles(
        new CatalogsServiceEvents.AfterListCatalogRolesEvent(catalogName));
    Response resp = delegate.listCatalogRoles(catalogName, realmContext, securityContext);
    polarisEventListener.onBeforeListCatalogRoles(
        new CatalogsServiceEvents.BeforeListCatalogRolesEvent(catalogName));
    return resp;
  }

  @Override
  public Response addGrantToCatalogRole(
      String catalogName,
      String catalogRoleName,
      AddGrantRequest grantRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    // TODO: After changing the API response, we should change this to emit the corresponding event.
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
    // TODO: After changing the API response, we should change this to emit the corresponding event.
    return delegate.revokeGrantFromCatalogRole(
        catalogName, catalogRoleName, cascade, grantRequest, realmContext, securityContext);
  }

  @Override
  public Response listAssigneePrincipalRolesForCatalogRole(
      String catalogName,
      String catalogRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onBeforeListAssigneePrincipalRolesForCatalogRole(
        new CatalogsServiceEvents.BeforeListAssigneePrincipalRolesForCatalogRoleEvent(
            catalogName, catalogRoleName));
    Response resp =
        delegate.listAssigneePrincipalRolesForCatalogRole(
            catalogName, catalogRoleName, realmContext, securityContext);
    polarisEventListener.onAfterListAssigneePrincipalRolesForCatalogRole(
        new CatalogsServiceEvents.AfterListAssigneePrincipalRolesForCatalogRoleEvent(
            catalogName, catalogRoleName));
    return resp;
  }

  @Override
  public Response listGrantsForCatalogRole(
      String catalogName,
      String catalogRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onBeforeListGrantsForCatalogRole(
        new CatalogsServiceEvents.BeforeListGrantsForCatalogRoleEvent(
            catalogName, catalogRoleName));
    Response resp =
        delegate.listGrantsForCatalogRole(
            catalogName, catalogRoleName, realmContext, securityContext);
    polarisEventListener.onAfterListGrantsForCatalogRole(
        new CatalogsServiceEvents.AfterListGrantsForCatalogRoleEvent(catalogName, catalogRoleName));
    return resp;
  }
}
