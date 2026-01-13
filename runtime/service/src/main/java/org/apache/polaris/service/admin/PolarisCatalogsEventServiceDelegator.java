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
import org.apache.polaris.service.events.AttributeMap;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.events.PolarisEventType;
import org.apache.polaris.service.events.listeners.PolarisEventListener;

@Decorator
@Priority(1000)
public class PolarisCatalogsEventServiceDelegator implements PolarisCatalogsApiService {

  @Inject @Delegate PolarisCatalogsApiService delegate;
  @Inject PolarisEventListener polarisEventListener;
  @Inject PolarisEventMetadataFactory eventMetadataFactory;

  @Override
  public Response createCatalog(
      CreateCatalogRequest request, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_CREATE_CATALOG,
            eventMetadataFactory.create(),
            new AttributeMap().put(EventAttributes.CATALOG_NAME, request.getCatalog().getName())));
    Response resp = delegate.createCatalog(request, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_CREATE_CATALOG,
            eventMetadataFactory.create(),
            new AttributeMap().put(EventAttributes.CATALOG, (Catalog) resp.getEntity())));
    return resp;
  }

  @Override
  public Response deleteCatalog(
      String catalogName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_DELETE_CATALOG,
            eventMetadataFactory.create(),
            new AttributeMap().put(EventAttributes.CATALOG_NAME, catalogName)));
    Response resp = delegate.deleteCatalog(catalogName, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_DELETE_CATALOG,
            eventMetadataFactory.create(),
            new AttributeMap().put(EventAttributes.CATALOG_NAME, catalogName)));
    return resp;
  }

  @Override
  public Response getCatalog(
      String catalogName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_GET_CATALOG,
            eventMetadataFactory.create(),
            new AttributeMap().put(EventAttributes.CATALOG_NAME, catalogName)));
    Response resp = delegate.getCatalog(catalogName, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_GET_CATALOG,
            eventMetadataFactory.create(),
            new AttributeMap().put(EventAttributes.CATALOG, (Catalog) resp.getEntity())));
    return resp;
  }

  @Override
  public Response updateCatalog(
      String catalogName,
      UpdateCatalogRequest updateRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_UPDATE_CATALOG,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.UPDATE_CATALOG_REQUEST, updateRequest)));
    Response resp =
        delegate.updateCatalog(catalogName, updateRequest, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_UPDATE_CATALOG,
            eventMetadataFactory.create(),
            new AttributeMap().put(EventAttributes.CATALOG, (Catalog) resp.getEntity())));
    return resp;
  }

  @Override
  public Response listCatalogs(RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_LIST_CATALOGS,
            eventMetadataFactory.create(),
            new AttributeMap()));
    Response resp = delegate.listCatalogs(realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_LIST_CATALOGS,
            eventMetadataFactory.create(),
            new AttributeMap()));
    return resp;
  }

  @Override
  public Response createCatalogRole(
      String catalogName,
      CreateCatalogRoleRequest request,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_CREATE_CATALOG_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.CATALOG_ROLE_NAME, request.getCatalogRole().getName())));
    Response resp = delegate.createCatalogRole(catalogName, request, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_CREATE_CATALOG_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.CATALOG_ROLE, (CatalogRole) resp.getEntity())));
    return resp;
  }

  @Override
  public Response deleteCatalogRole(
      String catalogName,
      String catalogRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_DELETE_CATALOG_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.CATALOG_ROLE_NAME, catalogRoleName)));
    Response resp =
        delegate.deleteCatalogRole(catalogName, catalogRoleName, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_DELETE_CATALOG_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.CATALOG_ROLE_NAME, catalogRoleName)));
    return resp;
  }

  @Override
  public Response getCatalogRole(
      String catalogName,
      String catalogRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_GET_CATALOG_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.CATALOG_ROLE_NAME, catalogRoleName)));
    Response resp =
        delegate.getCatalogRole(catalogName, catalogRoleName, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_GET_CATALOG_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.CATALOG_ROLE, (CatalogRole) resp.getEntity())));
    return resp;
  }

  @Override
  public Response updateCatalogRole(
      String catalogName,
      String catalogRoleName,
      UpdateCatalogRoleRequest updateRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_UPDATE_CATALOG_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.CATALOG_ROLE_NAME, catalogRoleName)
                .put(EventAttributes.UPDATE_CATALOG_ROLE_REQUEST, updateRequest)));
    Response resp =
        delegate.updateCatalogRole(
            catalogName, catalogRoleName, updateRequest, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_UPDATE_CATALOG_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.CATALOG_ROLE, (CatalogRole) resp.getEntity())));
    return resp;
  }

  @Override
  public Response listCatalogRoles(
      String catalogName, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_LIST_CATALOG_ROLES,
            eventMetadataFactory.create(),
            new AttributeMap().put(EventAttributes.CATALOG_NAME, catalogName)));
    Response resp = delegate.listCatalogRoles(catalogName, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_LIST_CATALOG_ROLES,
            eventMetadataFactory.create(),
            new AttributeMap().put(EventAttributes.CATALOG_NAME, catalogName)));
    return resp;
  }

  @Override
  public Response addGrantToCatalogRole(
      String catalogName,
      String catalogRoleName,
      AddGrantRequest grantRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_ADD_GRANT_TO_CATALOG_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.CATALOG_ROLE_NAME, catalogRoleName)
                .put(EventAttributes.ADD_GRANT_REQUEST, grantRequest)));
    Response resp =
        delegate.addGrantToCatalogRole(
            catalogName, catalogRoleName, grantRequest, realmContext, securityContext);
    GrantResource grantResource = grantRequest.getGrant();
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_ADD_GRANT_TO_CATALOG_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.CATALOG_ROLE_NAME, catalogRoleName)
                .put(EventAttributes.PRIVILEGE, getPrivilegeFromGrantResource(grantResource))
                .put(EventAttributes.GRANT_RESOURCE, grantResource)));
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
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_REVOKE_GRANT_FROM_CATALOG_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.CATALOG_ROLE_NAME, catalogRoleName)
                .put(EventAttributes.REVOKE_GRANT_REQUEST, grantRequest)
                .put(EventAttributes.CASCADE, cascade)));
    Response resp =
        delegate.revokeGrantFromCatalogRole(
            catalogName, catalogRoleName, cascade, grantRequest, realmContext, securityContext);
    GrantResource grantResource = grantRequest.getGrant();
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_REVOKE_GRANT_FROM_CATALOG_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.CATALOG_ROLE_NAME, catalogRoleName)
                .put(EventAttributes.PRIVILEGE, getPrivilegeFromGrantResource(grantResource))
                .put(EventAttributes.GRANT_RESOURCE, grantResource)
                .put(EventAttributes.CASCADE, cascade)));
    return resp;
  }

  @Override
  public Response listAssigneePrincipalRolesForCatalogRole(
      String catalogName,
      String catalogRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_LIST_ASSIGNEE_PRINCIPAL_ROLES_FOR_CATALOG_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.CATALOG_ROLE_NAME, catalogRoleName)));
    Response resp =
        delegate.listAssigneePrincipalRolesForCatalogRole(
            catalogName, catalogRoleName, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_LIST_ASSIGNEE_PRINCIPAL_ROLES_FOR_CATALOG_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.CATALOG_ROLE_NAME, catalogRoleName)));
    return resp;
  }

  @Override
  public Response listGrantsForCatalogRole(
      String catalogName,
      String catalogRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_LIST_GRANTS_FOR_CATALOG_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.CATALOG_ROLE_NAME, catalogRoleName)));
    Response resp =
        delegate.listGrantsForCatalogRole(
            catalogName, catalogRoleName, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_LIST_GRANTS_FOR_CATALOG_ROLE,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.CATALOG_ROLE_NAME, catalogRoleName)));
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
