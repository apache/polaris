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

import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisConfiguration;
import org.apache.polaris.core.admin.model.AddGrantRequest;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.CatalogRoles;
import org.apache.polaris.core.admin.model.Catalogs;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.CreateCatalogRoleRequest;
import org.apache.polaris.core.admin.model.CreatePrincipalRequest;
import org.apache.polaris.core.admin.model.CreatePrincipalRoleRequest;
import org.apache.polaris.core.admin.model.GrantCatalogRoleRequest;
import org.apache.polaris.core.admin.model.GrantPrincipalRoleRequest;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.GrantResources;
import org.apache.polaris.core.admin.model.NamespaceGrant;
import org.apache.polaris.core.admin.model.Principal;
import org.apache.polaris.core.admin.model.PrincipalRole;
import org.apache.polaris.core.admin.model.PrincipalRoles;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.Principals;
import org.apache.polaris.core.admin.model.RevokeGrantRequest;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.admin.model.TableGrant;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.admin.model.UpdateCatalogRoleRequest;
import org.apache.polaris.core.admin.model.UpdatePrincipalRequest;
import org.apache.polaris.core.admin.model.UpdatePrincipalRoleRequest;
import org.apache.polaris.core.admin.model.ViewGrant;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.CatalogRoleEntity;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.admin.api.PolarisCatalogsApiService;
import org.apache.polaris.service.admin.api.PolarisPrincipalRolesApiService;
import org.apache.polaris.service.admin.api.PolarisPrincipalsApiService;
import org.apache.polaris.service.config.RealmEntityManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Concrete implementation of the Polaris API services */
public class PolarisServiceImpl
    implements PolarisCatalogsApiService,
        PolarisPrincipalsApiService,
        PolarisPrincipalRolesApiService {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisServiceImpl.class);
  private final RealmEntityManagerFactory entityManagerFactory;
  private final PolarisAuthorizer polarisAuthorizer;
  private final MetaStoreManagerFactory metaStoreManagerFactory;

  @Inject
  public PolarisServiceImpl(
      RealmEntityManagerFactory entityManagerFactory,
      MetaStoreManagerFactory metaStoreManagerFactory,
      PolarisAuthorizer polarisAuthorizer) {
    this.entityManagerFactory = entityManagerFactory;
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.polarisAuthorizer = polarisAuthorizer;
  }

  private PolarisAdminService newAdminService(SecurityContext securityContext) {
    CallContext callContext = CallContext.getCurrentContext();
    AuthenticatedPolarisPrincipal authenticatedPrincipal =
        (AuthenticatedPolarisPrincipal) securityContext.getUserPrincipal();
    if (authenticatedPrincipal == null) {
      throw new NotAuthorizedException("Failed to find authenticatedPrincipal in SecurityContext");
    }

    PolarisEntityManager entityManager =
        entityManagerFactory.getOrCreateEntityManager(callContext.getRealmContext());
    PolarisMetaStoreManager metaStoreManager =
        metaStoreManagerFactory.getOrCreateMetaStoreManager(callContext.getRealmContext());
    return new PolarisAdminService(
        callContext, entityManager, metaStoreManager, authenticatedPrincipal, polarisAuthorizer);
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response createCatalog(CreateCatalogRequest request, SecurityContext securityContext) {
    PolarisAdminService adminService = newAdminService(securityContext);
    Catalog catalog = request.getCatalog();
    validateStorageConfig(catalog.getStorageConfigInfo());
    Catalog newCatalog =
        new CatalogEntity(adminService.createCatalog(CatalogEntity.fromCatalog(catalog)))
            .asCatalog();
    LOGGER.info("Created new catalog {}", newCatalog);
    return Response.status(Response.Status.CREATED).build();
  }

  private void validateStorageConfig(StorageConfigInfo storageConfigInfo) {
    CallContext callContext = CallContext.getCurrentContext();
    PolarisCallContext polarisCallContext = callContext.getPolarisCallContext();
    List<String> allowedStorageTypes =
        polarisCallContext
            .getConfigurationStore()
            .getConfiguration(
                polarisCallContext, PolarisConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES);
    if (!allowedStorageTypes.contains(storageConfigInfo.getStorageType().name())) {
      LOGGER
          .atWarn()
          .addKeyValue("storageConfig", storageConfigInfo)
          .log("Disallowed storage type in catalog");
      throw new IllegalArgumentException(
          "Unsupported storage type: " + storageConfigInfo.getStorageType());
    }
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response deleteCatalog(String catalogName, SecurityContext securityContext) {
    PolarisAdminService adminService = newAdminService(securityContext);
    adminService.deleteCatalog(catalogName);
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response getCatalog(String catalogName, SecurityContext securityContext) {
    PolarisAdminService adminService = newAdminService(securityContext);
    return Response.ok(adminService.getCatalog(catalogName).asCatalog()).build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response updateCatalog(
      String catalogName, UpdateCatalogRequest updateRequest, SecurityContext securityContext) {
    PolarisAdminService adminService = newAdminService(securityContext);
    if (updateRequest.getStorageConfigInfo() != null) {
      validateStorageConfig(updateRequest.getStorageConfigInfo());
    }
    return Response.ok(adminService.updateCatalog(catalogName, updateRequest).asCatalog()).build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response listCatalogs(SecurityContext securityContext) {
    PolarisAdminService adminService = newAdminService(securityContext);
    List<Catalog> catalogList =
        adminService.listCatalogs().stream()
            .map(CatalogEntity::new)
            .map(CatalogEntity::asCatalog)
            .toList();
    Catalogs catalogs = new Catalogs(catalogList);
    LOGGER.debug("listCatalogs returning: {}", catalogs);
    return Response.ok(catalogs).build();
  }

  /** From PolarisPrincipalsApiService */
  @Override
  public Response createPrincipal(CreatePrincipalRequest request, SecurityContext securityContext) {
    PolarisAdminService adminService = newAdminService(securityContext);
    PrincipalEntity principal = PrincipalEntity.fromPrincipal(request.getPrincipal());
    if (Boolean.TRUE.equals(request.getCredentialRotationRequired())) {
      principal =
          new PrincipalEntity.Builder(principal).setCredentialRotationRequiredState().build();
    }
    PrincipalWithCredentials createdPrincipal = adminService.createPrincipal(principal);
    LOGGER.info("Created new principal {}", createdPrincipal);
    return Response.status(Response.Status.CREATED).entity(createdPrincipal).build();
  }

  /** From PolarisPrincipalsApiService */
  @Override
  public Response deletePrincipal(String principalName, SecurityContext securityContext) {
    PolarisAdminService adminService = newAdminService(securityContext);
    adminService.deletePrincipal(principalName);
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  /** From PolarisPrincipalsApiService */
  @Override
  public Response getPrincipal(String principalName, SecurityContext securityContext) {
    PolarisAdminService adminService = newAdminService(securityContext);
    return Response.ok(adminService.getPrincipal(principalName).asPrincipal()).build();
  }

  /** From PolarisPrincipalsApiService */
  @Override
  public Response updatePrincipal(
      String principalName, UpdatePrincipalRequest updateRequest, SecurityContext securityContext) {
    PolarisAdminService adminService = newAdminService(securityContext);
    return Response.ok(adminService.updatePrincipal(principalName, updateRequest).asPrincipal())
        .build();
  }

  /** From PolarisPrincipalsApiService */
  @Override
  public Response rotateCredentials(String principalName, SecurityContext securityContext) {
    PolarisAdminService adminService = newAdminService(securityContext);
    return Response.ok(adminService.rotateCredentials(principalName)).build();
  }

  /** From PolarisPrincipalsApiService */
  @Override
  public Response listPrincipals(SecurityContext securityContext) {
    PolarisAdminService adminService = newAdminService(securityContext);
    List<Principal> principalList =
        adminService.listPrincipals().stream()
            .map(PrincipalEntity::new)
            .map(PrincipalEntity::asPrincipal)
            .toList();
    Principals principals = new Principals(principalList);
    LOGGER.debug("listPrincipals returning: {}", principals);
    return Response.ok(principals).build();
  }

  /** From PolarisPrincipalRolesApiService */
  @Override
  public Response createPrincipalRole(
      CreatePrincipalRoleRequest request, SecurityContext securityContext) {
    PolarisAdminService adminService = newAdminService(securityContext);
    PrincipalRole newPrincipalRole =
        new PrincipalRoleEntity(
                adminService.createPrincipalRole(
                    PrincipalRoleEntity.fromPrincipalRole(request.getPrincipalRole())))
            .asPrincipalRole();
    LOGGER.info("Created new principalRole {}", newPrincipalRole);
    return Response.status(Response.Status.CREATED).build();
  }

  /** From PolarisPrincipalRolesApiService */
  @Override
  public Response deletePrincipalRole(String principalRoleName, SecurityContext securityContext) {
    PolarisAdminService adminService = newAdminService(securityContext);
    adminService.deletePrincipalRole(principalRoleName);
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  /** From PolarisPrincipalRolesApiService */
  @Override
  public Response getPrincipalRole(String principalRoleName, SecurityContext securityContext) {
    PolarisAdminService adminService = newAdminService(securityContext);
    return Response.ok(adminService.getPrincipalRole(principalRoleName).asPrincipalRole()).build();
  }

  /** From PolarisPrincipalRolesApiService */
  @Override
  public Response updatePrincipalRole(
      String principalRoleName,
      UpdatePrincipalRoleRequest updateRequest,
      SecurityContext securityContext) {
    PolarisAdminService adminService = newAdminService(securityContext);
    return Response.ok(
            adminService.updatePrincipalRole(principalRoleName, updateRequest).asPrincipalRole())
        .build();
  }

  /** From PolarisPrincipalRolesApiService */
  @Override
  public Response listPrincipalRoles(SecurityContext securityContext) {
    PolarisAdminService adminService = newAdminService(securityContext);
    List<PrincipalRole> principalRoleList =
        adminService.listPrincipalRoles().stream()
            .map(PrincipalRoleEntity::new)
            .map(PrincipalRoleEntity::asPrincipalRole)
            .toList();
    PrincipalRoles principalRoles = new PrincipalRoles(principalRoleList);
    LOGGER.debug("listPrincipalRoles returning: {}", principalRoles);
    return Response.ok(principalRoles).build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response createCatalogRole(
      String catalogName, CreateCatalogRoleRequest request, SecurityContext securityContext) {
    PolarisAdminService adminService = newAdminService(securityContext);
    CatalogRole newCatalogRole =
        new CatalogRoleEntity(
                adminService.createCatalogRole(
                    catalogName, CatalogRoleEntity.fromCatalogRole(request.getCatalogRole())))
            .asCatalogRole();
    LOGGER.info("Created new catalogRole {}", newCatalogRole);
    return Response.status(Response.Status.CREATED).build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response deleteCatalogRole(
      String catalogName, String catalogRoleName, SecurityContext securityContext) {
    PolarisAdminService adminService = newAdminService(securityContext);
    adminService.deleteCatalogRole(catalogName, catalogRoleName);
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response getCatalogRole(
      String catalogName, String catalogRoleName, SecurityContext securityContext) {
    PolarisAdminService adminService = newAdminService(securityContext);
    return Response.ok(adminService.getCatalogRole(catalogName, catalogRoleName).asCatalogRole())
        .build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response updateCatalogRole(
      String catalogName,
      String catalogRoleName,
      UpdateCatalogRoleRequest updateRequest,
      SecurityContext securityContext) {
    PolarisAdminService adminService = newAdminService(securityContext);
    return Response.ok(
            adminService
                .updateCatalogRole(catalogName, catalogRoleName, updateRequest)
                .asCatalogRole())
        .build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response listCatalogRoles(String catalogName, SecurityContext securityContext) {
    PolarisAdminService adminService = newAdminService(securityContext);
    List<CatalogRole> catalogRoleList =
        adminService.listCatalogRoles(catalogName).stream()
            .map(CatalogRoleEntity::new)
            .map(CatalogRoleEntity::asCatalogRole)
            .toList();
    CatalogRoles catalogRoles = new CatalogRoles(catalogRoleList);
    LOGGER.debug("listCatalogRoles returning: {}", catalogRoles);
    return Response.ok(catalogRoles).build();
  }

  /** From PolarisPrincipalsApiService */
  @Override
  public Response assignPrincipalRole(
      String principalName, GrantPrincipalRoleRequest request, SecurityContext securityContext) {
    LOGGER.info(
        "Assigning principalRole {} to principal {}",
        request.getPrincipalRole().getName(),
        principalName);
    PolarisAdminService adminService = newAdminService(securityContext);
    adminService.assignPrincipalRole(principalName, request.getPrincipalRole().getName());
    return Response.status(Response.Status.CREATED).build();
  }

  /** From PolarisPrincipalsApiService */
  @Override
  public Response revokePrincipalRole(
      String principalName, String principalRoleName, SecurityContext securityContext) {
    LOGGER.info("Revoking principalRole {} from principal {}", principalRoleName, principalName);
    PolarisAdminService adminService = newAdminService(securityContext);
    adminService.revokePrincipalRole(principalName, principalRoleName);
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  /** From PolarisPrincipalsApiService */
  @Override
  public Response listPrincipalRolesAssigned(
      String principalName, SecurityContext securityContext) {
    PolarisAdminService adminService = newAdminService(securityContext);
    List<PrincipalRole> principalRoleList =
        adminService.listPrincipalRolesAssigned(principalName).stream()
            .map(PrincipalRoleEntity::new)
            .map(PrincipalRoleEntity::asPrincipalRole)
            .toList();
    PrincipalRoles principalRoles = new PrincipalRoles(principalRoleList);
    LOGGER.debug("listPrincipalRolesAssigned returning: {}", principalRoles);
    return Response.ok(principalRoles).build();
  }

  /** From PolarisPrincipalRolesApiService */
  @Override
  public Response assignCatalogRoleToPrincipalRole(
      String principalRoleName,
      String catalogName,
      GrantCatalogRoleRequest request,
      SecurityContext securityContext) {
    LOGGER.info(
        "Assigning catalogRole {} in catalog {} to principalRole {}",
        request.getCatalogRole().getName(),
        catalogName,
        principalRoleName);
    PolarisAdminService adminService = newAdminService(securityContext);
    adminService.assignCatalogRoleToPrincipalRole(
        principalRoleName, catalogName, request.getCatalogRole().getName());
    return Response.status(Response.Status.CREATED).build();
  }

  /** From PolarisPrincipalRolesApiService */
  @Override
  public Response revokeCatalogRoleFromPrincipalRole(
      String principalRoleName,
      String catalogName,
      String catalogRoleName,
      SecurityContext securityContext) {
    LOGGER.info(
        "Revoking catalogRole {} in catalog {} from principalRole {}",
        catalogRoleName,
        catalogName,
        principalRoleName);
    PolarisAdminService adminService = newAdminService(securityContext);
    adminService.revokeCatalogRoleFromPrincipalRole(
        principalRoleName, catalogName, catalogRoleName);
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  /** From PolarisPrincipalRolesApiService */
  @Override
  public Response listAssigneePrincipalsForPrincipalRole(
      String principalRoleName, SecurityContext securityContext) {
    PolarisAdminService adminService = newAdminService(securityContext);
    List<Principal> principalList =
        adminService.listAssigneePrincipalsForPrincipalRole(principalRoleName).stream()
            .map(PrincipalEntity::new)
            .map(PrincipalEntity::asPrincipal)
            .toList();
    Principals principals = new Principals(principalList);
    LOGGER.debug("listAssigneePrincipalsForPrincipalRole returning: {}", principals);
    return Response.ok(principals).build();
  }

  /** From PolarisPrincipalRolesApiService */
  @Override
  public Response listCatalogRolesForPrincipalRole(
      String principalRoleName, String catalogName, SecurityContext securityContext) {
    PolarisAdminService adminService = newAdminService(securityContext);
    List<CatalogRole> catalogRoleList =
        adminService.listCatalogRolesForPrincipalRole(principalRoleName, catalogName).stream()
            .map(CatalogRoleEntity::new)
            .map(CatalogRoleEntity::asCatalogRole)
            .toList();
    CatalogRoles catalogRoles = new CatalogRoles(catalogRoleList);
    LOGGER.debug("listCatalogRolesForPrincipalRole returning: {}", catalogRoles);
    return Response.ok(catalogRoles).build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response addGrantToCatalogRole(
      String catalogName,
      String catalogRoleName,
      AddGrantRequest grantRequest,
      SecurityContext securityContext) {
    LOGGER.info(
        "Adding grant {} to catalogRole {} in catalog {}",
        grantRequest,
        catalogRoleName,
        catalogName);
    PolarisAdminService adminService = newAdminService(securityContext);
    switch (grantRequest.getGrant()) {
        // The per-securable-type Privilege enums must be exact String match for a subset of all
        // PolarisPrivilege values.
      case ViewGrant viewGrant:
        {
          PolarisPrivilege privilege =
              PolarisPrivilege.valueOf(viewGrant.getPrivilege().toString());
          String viewName = viewGrant.getViewName();
          String[] namespaceParts = viewGrant.getNamespace().toArray(new String[0]);
          adminService.grantPrivilegeOnViewToRole(
              catalogName,
              catalogRoleName,
              TableIdentifier.of(Namespace.of(namespaceParts), viewName),
              privilege);
          break;
        }
      case TableGrant tableGrant:
        {
          PolarisPrivilege privilege =
              PolarisPrivilege.valueOf(tableGrant.getPrivilege().toString());
          String tableName = tableGrant.getTableName();
          String[] namespaceParts = tableGrant.getNamespace().toArray(new String[0]);
          adminService.grantPrivilegeOnTableToRole(
              catalogName,
              catalogRoleName,
              TableIdentifier.of(Namespace.of(namespaceParts), tableName),
              privilege);
          break;
        }
      case NamespaceGrant namespaceGrant:
        {
          PolarisPrivilege privilege =
              PolarisPrivilege.valueOf(namespaceGrant.getPrivilege().toString());
          String[] namespaceParts = namespaceGrant.getNamespace().toArray(new String[0]);
          adminService.grantPrivilegeOnNamespaceToRole(
              catalogName, catalogRoleName, Namespace.of(namespaceParts), privilege);
          break;
        }
      case CatalogGrant catalogGrant:
        {
          PolarisPrivilege privilege =
              PolarisPrivilege.valueOf(catalogGrant.getPrivilege().toString());
          adminService.grantPrivilegeOnCatalogToRole(catalogName, catalogRoleName, privilege);
          break;
        }
      default:
        LOGGER
            .atWarn()
            .addKeyValue("catalog", catalogName)
            .addKeyValue("role", catalogRoleName)
            .log("Don't know how to handle privilege grant: {}", grantRequest);
        return Response.status(Response.Status.BAD_REQUEST).build();
    }
    return Response.status(Response.Status.CREATED).build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response revokeGrantFromCatalogRole(
      String catalogName,
      String catalogRoleName,
      Boolean cascade,
      RevokeGrantRequest grantRequest,
      SecurityContext securityContext) {
    LOGGER.info(
        "Revoking grant {} from catalogRole {} in catalog {}",
        grantRequest,
        catalogRoleName,
        catalogName);
    if (cascade != null && cascade) {
      LOGGER.warn("Tried to use unimplemented 'cascade' feature when revoking grants.");
      return Response.status(501).build(); // not implemented
    }

    PolarisAdminService adminService = newAdminService(securityContext);
    switch (grantRequest.getGrant()) {
        // The per-securable-type Privilege enums must be exact String match for a subset of all
        // PolarisPrivilege values.
      case ViewGrant viewGrant:
        {
          PolarisPrivilege privilege =
              PolarisPrivilege.valueOf(viewGrant.getPrivilege().toString());
          String viewName = viewGrant.getViewName();
          String[] namespaceParts = viewGrant.getNamespace().toArray(new String[0]);
          adminService.revokePrivilegeOnViewFromRole(
              catalogName,
              catalogRoleName,
              TableIdentifier.of(Namespace.of(namespaceParts), viewName),
              privilege);
          break;
        }
      case TableGrant tableGrant:
        {
          PolarisPrivilege privilege =
              PolarisPrivilege.valueOf(tableGrant.getPrivilege().toString());
          String tableName = tableGrant.getTableName();
          String[] namespaceParts = tableGrant.getNamespace().toArray(new String[0]);
          adminService.revokePrivilegeOnTableFromRole(
              catalogName,
              catalogRoleName,
              TableIdentifier.of(Namespace.of(namespaceParts), tableName),
              privilege);
          break;
        }
      case NamespaceGrant namespaceGrant:
        {
          PolarisPrivilege privilege =
              PolarisPrivilege.valueOf(namespaceGrant.getPrivilege().toString());
          String[] namespaceParts = namespaceGrant.getNamespace().toArray(new String[0]);
          adminService.revokePrivilegeOnNamespaceFromRole(
              catalogName, catalogRoleName, Namespace.of(namespaceParts), privilege);
          break;
        }
      case CatalogGrant catalogGrant:
        {
          PolarisPrivilege privilege =
              PolarisPrivilege.valueOf(catalogGrant.getPrivilege().toString());
          adminService.revokePrivilegeOnCatalogFromRole(catalogName, catalogRoleName, privilege);
          break;
        }
      default:
        LOGGER
            .atWarn()
            .addKeyValue("catalog", catalogName)
            .addKeyValue("role", catalogRoleName)
            .log("Don't know how to handle privilege revocation: {}", grantRequest);
        return Response.status(Response.Status.BAD_REQUEST).build();
    }
    return Response.status(Response.Status.CREATED).build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response listAssigneePrincipalRolesForCatalogRole(
      String catalogName, String catalogRoleName, SecurityContext securityContext) {
    PolarisAdminService adminService = newAdminService(securityContext);
    List<PrincipalRole> principalRoleList =
        adminService.listAssigneePrincipalRolesForCatalogRole(catalogName, catalogRoleName).stream()
            .map(PrincipalRoleEntity::new)
            .map(PrincipalRoleEntity::asPrincipalRole)
            .toList();
    PrincipalRoles principalRoles = new PrincipalRoles(principalRoleList);
    LOGGER.debug("listAssigneePrincipalRolesForCatalogRole returning: {}", principalRoles);
    return Response.ok(principalRoles).build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response listGrantsForCatalogRole(
      String catalogName, String catalogRoleName, SecurityContext securityContext) {
    PolarisAdminService adminService = newAdminService(securityContext);
    List<GrantResource> grantList =
        adminService.listGrantsForCatalogRole(catalogName, catalogRoleName);
    GrantResources grantResources = new GrantResources(grantList);
    return Response.ok(grantResources).build();
  }
}
