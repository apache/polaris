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
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.CreateCatalogRoleRequest;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.NamespaceGrant;
import org.apache.polaris.core.admin.model.PolicyGrant;
import org.apache.polaris.core.admin.model.RevokeGrantRequest;
import org.apache.polaris.core.admin.model.TableGrant;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.admin.model.UpdateCatalogRoleRequest;
import org.apache.polaris.core.admin.model.ViewGrant;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisPrivilege;
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
    polarisEventListener.onBeforeCreateCatalog(
        new CatalogsServiceEvents.BeforeCreateCatalogEvent(request.getCatalog().getName()));
    Response resp = delegate.createCatalog(request, realmContext, securityContext);
    polarisEventListener.onAfterCreateCatalog(
        new CatalogsServiceEvents.AfterCreateCatalogEvent((Catalog) resp.getEntity()));
    return resp;
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
    polarisEventListener.onBeforeCreateCatalogRole(
        new CatalogsServiceEvents.BeforeCreateCatalogRoleEvent(
            catalogName, request.getCatalogRole().getName()));
    Response resp = delegate.createCatalogRole(catalogName, request, realmContext, securityContext);
    polarisEventListener.onAfterCreateCatalogRole(
        new CatalogsServiceEvents.AfterCreateCatalogRoleEvent(
            catalogName, (CatalogRole) resp.getEntity()));
    return resp;
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
    polarisEventListener.onBeforeAddGrantToCatalogRole(
        new CatalogsServiceEvents.BeforeAddGrantToCatalogRoleEvent(
            catalogName, catalogRoleName, grantRequest));
    Response resp =
        delegate.addGrantToCatalogRole(
            catalogName, catalogRoleName, grantRequest, realmContext, securityContext);
    GrantResource grantResource = grantRequest.getGrant();
    polarisEventListener.onAfterAddGrantToCatalogRole(
        new CatalogsServiceEvents.AfterAddGrantToCatalogRoleEvent(
            catalogName,
            catalogRoleName,
            getPrivilegeFromGrantResource(grantResource),
            grantResource));
    return resp;
  }

  @Override
  public Response revokeGrantFromCatalogRole(
      String catalogName,
      String catalogRoleName,
      Boolean cascade,
      RevokeGrantRequest grantRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onBeforeRevokeGrantFromCatalogRole(
        new CatalogsServiceEvents.BeforeRevokeGrantFromCatalogRoleEvent(
            catalogName, catalogRoleName, grantRequest, cascade));
    Response resp =
        delegate.revokeGrantFromCatalogRole(
            catalogName, catalogRoleName, cascade, grantRequest, realmContext, securityContext);
    GrantResource grantResource = grantRequest.getGrant();
    polarisEventListener.onAfterRevokeGrantFromCatalogRole(
        new CatalogsServiceEvents.AfterRevokeGrantFromCatalogRoleEvent(
            catalogName,
            catalogRoleName,
            getPrivilegeFromGrantResource(grantResource),
            grantResource,
            cascade));
    return resp;
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

  private PolarisPrivilege getPrivilegeFromGrantResource(GrantResource grantResource) {
    return switch (grantResource) {
      case ViewGrant viewGrant -> PolarisPrivilege.valueOf(viewGrant.getPrivilege().toString());
      case TableGrant tableGrant -> PolarisPrivilege.valueOf(tableGrant.getPrivilege().toString());
      case NamespaceGrant namespaceGrant ->
          PolarisPrivilege.valueOf(namespaceGrant.getPrivilege().toString());
      case CatalogGrant catalogGrant ->
          PolarisPrivilege.valueOf(catalogGrant.getPrivilege().toString());
      case PolicyGrant policyGrant ->
          PolarisPrivilege.valueOf(policyGrant.getPrivilege().toString());
      default -> null;
    };
  }
}
