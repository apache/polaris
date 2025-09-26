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

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.polaris.core.admin.model.AddGrantRequest;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.CatalogRoles;
import org.apache.polaris.core.admin.model.Catalogs;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.CreateCatalogRoleRequest;
import org.apache.polaris.core.admin.model.CreatePrincipalRequest;
import org.apache.polaris.core.admin.model.CreatePrincipalRoleRequest;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.GrantCatalogRoleRequest;
import org.apache.polaris.core.admin.model.GrantPrincipalRoleRequest;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.GrantResources;
import org.apache.polaris.core.admin.model.NamespaceGrant;
import org.apache.polaris.core.admin.model.PolicyGrant;
import org.apache.polaris.core.admin.model.Principal;
import org.apache.polaris.core.admin.model.PrincipalRole;
import org.apache.polaris.core.admin.model.PrincipalRoles;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.Principals;
import org.apache.polaris.core.admin.model.ResetPrincipalRequest;
import org.apache.polaris.core.admin.model.RevokeGrantRequest;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.admin.model.TableGrant;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.admin.model.UpdateCatalogRoleRequest;
import org.apache.polaris.core.admin.model.UpdatePrincipalRequest;
import org.apache.polaris.core.admin.model.UpdatePrincipalRoleRequest;
import org.apache.polaris.core.admin.model.ViewGrant;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.CatalogRoleEntity;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.PrivilegeResult;
import org.apache.polaris.service.admin.api.PolarisCatalogsApiService;
import org.apache.polaris.service.admin.api.PolarisPrincipalRolesApiService;
import org.apache.polaris.service.admin.api.PolarisPrincipalsApiService;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.types.PolicyIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Concrete implementation of the Polaris API services */
@RequestScoped
public class PolarisServiceImpl
    implements PolarisCatalogsApiService,
        PolarisPrincipalsApiService,
        PolarisPrincipalRolesApiService {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisServiceImpl.class);
  private final RealmConfig realmConfig;
  private final ReservedProperties reservedProperties;
  private final PolarisAdminService adminService;

  @Inject
  public PolarisServiceImpl(
      RealmConfig realmConfig,
      ReservedProperties reservedProperties,
      PolarisAdminService adminService) {
    this.realmConfig = realmConfig;
    this.reservedProperties = reservedProperties;
    this.adminService = adminService;
  }

  private static Response toResponse(BaseResult result, Response.Status successStatus) {
    if (!result.isSuccess()) {
      ErrorResponse icebergErrorResponse =
          ErrorResponse.builder()
              .responseCode(Response.Status.BAD_REQUEST.getStatusCode())
              .withType(result.getReturnStatus().toString())
              .withMessage("Operation failed: " + result.getReturnStatus().toString())
              .build();
      return Response.status(Response.Status.BAD_REQUEST)
          .type(MediaType.APPLICATION_JSON_TYPE)
          .entity(icebergErrorResponse)
          .build();
    }
    return Response.status(successStatus).build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response createCatalog(
      CreateCatalogRequest request, RealmContext realmContext, SecurityContext securityContext) {
    Catalog catalog = request.getCatalog();
    validateStorageConfig(catalog.getStorageConfigInfo());
    validateExternalCatalog(catalog);
    validateCatalogProperties(catalog.getProperties());
    Catalog newCatalog = CatalogEntity.of(adminService.createCatalog(request)).asCatalog();
    LOGGER.info("Created new catalog {}", newCatalog);
    return Response.status(Response.Status.CREATED).entity(newCatalog).build();
  }

  private void validateClientId(String clientId) {
    if (!clientId.matches("^[0-9a-f]{16}$")) {
      throw new IllegalArgumentException("Invalid clientId format");
    }
  }

  private void validateClientSecret(String clientSecret) {
    if (!clientSecret.matches("^[0-9a-f]{32}$")) {
      throw new IllegalArgumentException("Invalid clientSecret format");
    }
  }

  private void validateStorageConfig(StorageConfigInfo storageConfigInfo) {
    List<String> allowedStorageTypes =
        realmConfig.getConfig(FeatureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES);
    if (!allowedStorageTypes.contains(storageConfigInfo.getStorageType().name())) {
      LOGGER
          .atWarn()
          .addKeyValue("storageConfig", storageConfigInfo)
          .log("Disallowed storage type in catalog");
      throw new IllegalArgumentException(
          "Unsupported storage type: " + storageConfigInfo.getStorageType());
    }

    if (!realmConfig.getConfig(FeatureConfiguration.ALLOW_SETTING_S3_ENDPOINTS)) {
      if (storageConfigInfo instanceof AwsStorageConfigInfo s3Config) {
        if (s3Config.getEndpoint() != null
            || s3Config.getStsEndpoint() != null
            || s3Config.getEndpointInternal() != null) {
          throw new IllegalArgumentException("Explicitly setting S3 endpoints is not allowed.");
        }
      }
    }
  }

  private void validateExternalCatalog(Catalog catalog) {
    if (catalog.getType() == Catalog.TypeEnum.EXTERNAL) {
      if (catalog instanceof ExternalCatalog externalCatalog) {
        ConnectionConfigInfo connectionConfigInfo = externalCatalog.getConnectionConfigInfo();
        if (connectionConfigInfo != null) {
          validateConnectionConfigInfo(connectionConfigInfo);
          validateAuthenticationParameters(connectionConfigInfo.getAuthenticationParameters());
        }
      }
    }
  }

  private void validateCatalogProperties(Map<String, String> catalogProperties) {
    if (catalogProperties != null) {
      if (!realmConfig.getConfig(
              FeatureConfiguration.ALLOW_SETTING_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS)
          && catalogProperties.containsKey(
              FeatureConfiguration.ENABLE_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS
                  .catalogConfig())) {

        throw new IllegalArgumentException(
            String.format(
                "Explicitly setting %s is not allowed.",
                FeatureConfiguration.ENABLE_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS
                    .catalogConfig()));
      }
    }
  }

  private void validateConnectionConfigInfo(ConnectionConfigInfo connectionConfigInfo) {

    String connectionType = connectionConfigInfo.getConnectionType().name();
    List<String> supportedConnectionTypes =
        realmConfig.getConfig(FeatureConfiguration.SUPPORTED_CATALOG_CONNECTION_TYPES).stream()
            .map(s -> s.toUpperCase(Locale.ROOT))
            .toList();
    if (!supportedConnectionTypes.contains(connectionType)) {
      throw new IllegalStateException("Unsupported connection type: " + connectionType);
    }
  }

  private void validateAuthenticationParameters(AuthenticationParameters authenticationParameters) {

    String authenticationType = authenticationParameters.getAuthenticationType().name();
    List<String> supportedAuthenticationTypes =
        realmConfig
            .getConfig(FeatureConfiguration.SUPPORTED_EXTERNAL_CATALOG_AUTHENTICATION_TYPES)
            .stream()
            .map(s -> s.toUpperCase(Locale.ROOT))
            .toList();
    if (!supportedAuthenticationTypes.contains(authenticationType)) {
      throw new IllegalStateException("Unsupported authentication type: " + authenticationType);
    }
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response deleteCatalog(
      String catalogName, RealmContext realmContext, SecurityContext securityContext) {
    adminService.deleteCatalog(catalogName);
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response getCatalog(
      String catalogName, RealmContext realmContext, SecurityContext securityContext) {
    return Response.ok(adminService.getCatalog(catalogName).asCatalog()).build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response updateCatalog(
      String catalogName,
      UpdateCatalogRequest updateRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    if (updateRequest.getStorageConfigInfo() != null) {
      validateStorageConfig(updateRequest.getStorageConfigInfo());
    }
    validateCatalogProperties(updateRequest.getProperties());
    return Response.ok(adminService.updateCatalog(catalogName, updateRequest).asCatalog()).build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response listCatalogs(RealmContext realmContext, SecurityContext securityContext) {
    List<Catalog> catalogList = adminService.listCatalogs();
    Catalogs catalogs = new Catalogs(catalogList);
    LOGGER.debug("listCatalogs returning: {}", catalogs);
    return Response.ok(catalogs).build();
  }

  /** From PolarisPrincipalsApiService */
  @Override
  public Response createPrincipal(
      CreatePrincipalRequest request, RealmContext realmContext, SecurityContext securityContext) {
    PrincipalEntity principal =
        new PrincipalEntity.Builder()
            .setName(request.getPrincipal().getName())
            .setClientId(request.getPrincipal().getClientId())
            .setProperties(
                reservedProperties.removeReservedProperties(request.getPrincipal().getProperties()))
            .build();
    if (Boolean.TRUE.equals(request.getCredentialRotationRequired())) {
      principal =
          new PrincipalEntity.Builder(principal).setCredentialRotationRequiredState().build();
    }
    PrincipalWithCredentials createdPrincipal = adminService.createPrincipal(principal);
    LOGGER.info("Created new principal {}", createdPrincipal);
    return Response.status(Response.Status.CREATED).entity(createdPrincipal).build();
  }

  @Override
  public Response resetCredentials(
      String principalName,
      ResetPrincipalRequest resetPrincipalRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    ResetPrincipalRequest safeResetPrincipalRequest =
        (resetPrincipalRequest != null)
            ? resetPrincipalRequest
            : new ResetPrincipalRequest(null, null);

    if (safeResetPrincipalRequest.getClientId() != null) {
      validateClientId(safeResetPrincipalRequest.getClientId());
    }
    if (safeResetPrincipalRequest.getClientSecret() != null) {
      validateClientSecret(safeResetPrincipalRequest.getClientSecret());
    }
    return Response.ok(adminService.resetCredentials(principalName, safeResetPrincipalRequest))
        .build();
  }

  /** From PolarisPrincipalsApiService */
  @Override
  public Response deletePrincipal(
      String principalName, RealmContext realmContext, SecurityContext securityContext) {
    adminService.deletePrincipal(principalName);
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  /** From PolarisPrincipalsApiService */
  @Override
  public Response getPrincipal(
      String principalName, RealmContext realmContext, SecurityContext securityContext) {
    return Response.ok(adminService.getPrincipal(principalName).asPrincipal()).build();
  }

  /** From PolarisPrincipalsApiService */
  @Override
  public Response updatePrincipal(
      String principalName,
      UpdatePrincipalRequest updateRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return Response.ok(adminService.updatePrincipal(principalName, updateRequest).asPrincipal())
        .build();
  }

  /** From PolarisPrincipalsApiService */
  @Override
  public Response rotateCredentials(
      String principalName, RealmContext realmContext, SecurityContext securityContext) {
    return Response.ok(adminService.rotateCredentials(principalName)).build();
  }

  /** From PolarisPrincipalsApiService */
  @Override
  public Response listPrincipals(RealmContext realmContext, SecurityContext securityContext) {
    List<Principal> principalList = adminService.listPrincipals();
    Principals principals = new Principals(principalList);
    LOGGER.debug("listPrincipals returning: {}", principals);
    return Response.ok(principals).build();
  }

  /** From PolarisPrincipalRolesApiService */
  @Override
  public Response createPrincipalRole(
      CreatePrincipalRoleRequest request,
      RealmContext realmContext,
      SecurityContext securityContext) {
    PrincipalRoleEntity entity =
        new PrincipalRoleEntity.Builder()
            .setName(request.getPrincipalRole().getName())
            .setProperties(
                reservedProperties.removeReservedProperties(
                    request.getPrincipalRole().getProperties()))
            .build();
    PrincipalRole newPrincipalRole =
        new PrincipalRoleEntity(adminService.createPrincipalRole(entity)).asPrincipalRole();
    LOGGER.info("Created new principalRole {}", newPrincipalRole);
    return Response.status(Response.Status.CREATED).entity(newPrincipalRole).build();
  }

  /** From PolarisPrincipalRolesApiService */
  @Override
  public Response deletePrincipalRole(
      String principalRoleName, RealmContext realmContext, SecurityContext securityContext) {
    adminService.deletePrincipalRole(principalRoleName);
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  /** From PolarisPrincipalRolesApiService */
  @Override
  public Response getPrincipalRole(
      String principalRoleName, RealmContext realmContext, SecurityContext securityContext) {
    return Response.ok(adminService.getPrincipalRole(principalRoleName).asPrincipalRole()).build();
  }

  /** From PolarisPrincipalRolesApiService */
  @Override
  public Response updatePrincipalRole(
      String principalRoleName,
      UpdatePrincipalRoleRequest updateRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return Response.ok(
            adminService.updatePrincipalRole(principalRoleName, updateRequest).asPrincipalRole())
        .build();
  }

  /** From PolarisPrincipalRolesApiService */
  @Override
  public Response listPrincipalRoles(RealmContext realmContext, SecurityContext securityContext) {
    List<PrincipalRole> principalRoleList = adminService.listPrincipalRoles();
    PrincipalRoles principalRoles = new PrincipalRoles(principalRoleList);
    LOGGER.debug("listPrincipalRoles returning: {}", principalRoles);
    return Response.ok(principalRoles).build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response createCatalogRole(
      String catalogName,
      CreateCatalogRoleRequest request,
      RealmContext realmContext,
      SecurityContext securityContext) {
    CatalogRoleEntity entity =
        new CatalogRoleEntity.Builder()
            .setName(request.getCatalogRole().getName())
            .setProperties(
                reservedProperties.removeReservedProperties(
                    request.getCatalogRole().getProperties()))
            .build();
    CatalogRole newCatalogRole =
        new CatalogRoleEntity(adminService.createCatalogRole(catalogName, entity)).asCatalogRole();
    LOGGER.info("Created new catalogRole {}", newCatalogRole);
    return Response.status(Response.Status.CREATED).entity(newCatalogRole).build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response deleteCatalogRole(
      String catalogName,
      String catalogRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    adminService.deleteCatalogRole(catalogName, catalogRoleName);
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response getCatalogRole(
      String catalogName,
      String catalogRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return Response.ok(adminService.getCatalogRole(catalogName, catalogRoleName).asCatalogRole())
        .build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response updateCatalogRole(
      String catalogName,
      String catalogRoleName,
      UpdateCatalogRoleRequest updateRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return Response.ok(
            adminService
                .updateCatalogRole(catalogName, catalogRoleName, updateRequest)
                .asCatalogRole())
        .build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response listCatalogRoles(
      String catalogName, RealmContext realmContext, SecurityContext securityContext) {
    List<CatalogRole> catalogRoleList = adminService.listCatalogRoles(catalogName);
    CatalogRoles catalogRoles = new CatalogRoles(catalogRoleList);
    LOGGER.debug("listCatalogRoles returning: {}", catalogRoles);
    return Response.ok(catalogRoles).build();
  }

  /** From PolarisPrincipalsApiService */
  @Override
  public Response assignPrincipalRole(
      String principalName,
      GrantPrincipalRoleRequest request,
      RealmContext realmContext,
      SecurityContext securityContext) {
    LOGGER.info(
        "Assigning principalRole {} to principal {}",
        request.getPrincipalRole().getName(),
        principalName);
    PrivilegeResult result =
        adminService.assignPrincipalRole(principalName, request.getPrincipalRole().getName());
    return toResponse(result, Response.Status.CREATED);
  }

  /** From PolarisPrincipalsApiService */
  @Override
  public Response revokePrincipalRole(
      String principalName,
      String principalRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    LOGGER.info("Revoking principalRole {} from principal {}", principalRoleName, principalName);
    PrivilegeResult result = adminService.revokePrincipalRole(principalName, principalRoleName);
    return toResponse(result, Response.Status.NO_CONTENT);
  }

  /** From PolarisPrincipalsApiService */
  @Override
  public Response listPrincipalRolesAssigned(
      String principalName, RealmContext realmContext, SecurityContext securityContext) {
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
      RealmContext realmContext,
      SecurityContext securityContext) {
    LOGGER.info(
        "Assigning catalogRole {} in catalog {} to principalRole {}",
        request.getCatalogRole().getName(),
        catalogName,
        principalRoleName);
    PrivilegeResult result =
        adminService.assignCatalogRoleToPrincipalRole(
            principalRoleName, catalogName, request.getCatalogRole().getName());
    return toResponse(result, Response.Status.CREATED);
  }

  /** From PolarisPrincipalRolesApiService */
  @Override
  public Response revokeCatalogRoleFromPrincipalRole(
      String principalRoleName,
      String catalogName,
      String catalogRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    LOGGER.info(
        "Revoking catalogRole {} in catalog {} from principalRole {}",
        catalogRoleName,
        catalogName,
        principalRoleName);
    PrivilegeResult result =
        adminService.revokeCatalogRoleFromPrincipalRole(
            principalRoleName, catalogName, catalogRoleName);
    return toResponse(result, Response.Status.NO_CONTENT);
  }

  /** From PolarisPrincipalRolesApiService */
  @Override
  public Response listAssigneePrincipalsForPrincipalRole(
      String principalRoleName, RealmContext realmContext, SecurityContext securityContext) {
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
      String principalRoleName,
      String catalogName,
      RealmContext realmContext,
      SecurityContext securityContext) {
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
      RealmContext realmContext,
      SecurityContext securityContext) {
    LOGGER.info(
        "Adding grant {} to catalogRole {} in catalog {}",
        grantRequest,
        catalogRoleName,
        catalogName);
    PrivilegeResult result;
    PolarisPrivilege privilege;
    switch (grantRequest.getGrant()) {
      // The per-securable-type Privilege enums must be exact String match for a subset of all
      // PolarisPrivilege values.
      case ViewGrant viewGrant:
        {
          privilege = PolarisPrivilege.valueOf(viewGrant.getPrivilege().toString());
          String viewName = viewGrant.getViewName();
          String[] namespaceParts = viewGrant.getNamespace().toArray(new String[0]);
          result =
              adminService.grantPrivilegeOnViewToRole(
                  catalogName,
                  catalogRoleName,
                  TableIdentifier.of(Namespace.of(namespaceParts), viewName),
                  privilege);
          break;
        }
      case TableGrant tableGrant:
        {
          privilege = PolarisPrivilege.valueOf(tableGrant.getPrivilege().toString());
          String tableName = tableGrant.getTableName();
          String[] namespaceParts = tableGrant.getNamespace().toArray(new String[0]);
          result =
              adminService.grantPrivilegeOnTableToRole(
                  catalogName,
                  catalogRoleName,
                  TableIdentifier.of(Namespace.of(namespaceParts), tableName),
                  privilege);
          break;
        }
      case NamespaceGrant namespaceGrant:
        {
          privilege = PolarisPrivilege.valueOf(namespaceGrant.getPrivilege().toString());
          String[] namespaceParts = namespaceGrant.getNamespace().toArray(new String[0]);
          result =
              adminService.grantPrivilegeOnNamespaceToRole(
                  catalogName, catalogRoleName, Namespace.of(namespaceParts), privilege);
          break;
        }
      case CatalogGrant catalogGrant:
        {
          privilege = PolarisPrivilege.valueOf(catalogGrant.getPrivilege().toString());
          result =
              adminService.grantPrivilegeOnCatalogToRole(catalogName, catalogRoleName, privilege);
          break;
        }
      case PolicyGrant policyGrant:
        {
          privilege = PolarisPrivilege.valueOf(policyGrant.getPrivilege().toString());
          String policyName = policyGrant.getPolicyName();
          String[] namespaceParts = policyGrant.getNamespace().toArray(new String[0]);
          result =
              adminService.grantPrivilegeOnPolicyToRole(
                  catalogName,
                  catalogRoleName,
                  new PolicyIdentifier(Namespace.of(namespaceParts), policyName),
                  privilege);
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
    return toResponse(result, Response.Status.CREATED);
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response revokeGrantFromCatalogRole(
      String catalogName,
      String catalogRoleName,
      Boolean cascade,
      RevokeGrantRequest grantRequest,
      RealmContext realmContext,
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

    PrivilegeResult result;
    PolarisPrivilege privilege;
    switch (grantRequest.getGrant()) {
      // The per-securable-type Privilege enums must be exact String match for a subset of all
      // PolarisPrivilege values.
      case ViewGrant viewGrant:
        {
          privilege = PolarisPrivilege.valueOf(viewGrant.getPrivilege().toString());
          String viewName = viewGrant.getViewName();
          String[] namespaceParts = viewGrant.getNamespace().toArray(new String[0]);
          result =
              adminService.revokePrivilegeOnViewFromRole(
                  catalogName,
                  catalogRoleName,
                  TableIdentifier.of(Namespace.of(namespaceParts), viewName),
                  privilege);
          break;
        }
      case TableGrant tableGrant:
        {
          privilege = PolarisPrivilege.valueOf(tableGrant.getPrivilege().toString());
          String tableName = tableGrant.getTableName();
          String[] namespaceParts = tableGrant.getNamespace().toArray(new String[0]);
          result =
              adminService.revokePrivilegeOnTableFromRole(
                  catalogName,
                  catalogRoleName,
                  TableIdentifier.of(Namespace.of(namespaceParts), tableName),
                  privilege);
          break;
        }
      case NamespaceGrant namespaceGrant:
        {
          privilege = PolarisPrivilege.valueOf(namespaceGrant.getPrivilege().toString());
          String[] namespaceParts = namespaceGrant.getNamespace().toArray(new String[0]);
          result =
              adminService.revokePrivilegeOnNamespaceFromRole(
                  catalogName, catalogRoleName, Namespace.of(namespaceParts), privilege);
          break;
        }
      case CatalogGrant catalogGrant:
        {
          privilege = PolarisPrivilege.valueOf(catalogGrant.getPrivilege().toString());
          result =
              adminService.revokePrivilegeOnCatalogFromRole(
                  catalogName, catalogRoleName, privilege);
          break;
        }
      case PolicyGrant policyGrant:
        {
          privilege = PolarisPrivilege.valueOf(policyGrant.getPrivilege().toString());
          String policyName = policyGrant.getPolicyName();
          String[] namespaceParts = policyGrant.getNamespace().toArray(new String[0]);
          result =
              adminService.revokePrivilegeOnPolicyFromRole(
                  catalogName,
                  catalogRoleName,
                  new PolicyIdentifier(Namespace.of(namespaceParts), policyName),
                  privilege);
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
    return toResponse(result, Response.Status.CREATED);
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response listAssigneePrincipalRolesForCatalogRole(
      String catalogName,
      String catalogRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
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
      String catalogName,
      String catalogRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    List<GrantResource> grantList =
        adminService.listGrantsForCatalogRole(catalogName, catalogRoleName);
    GrantResources grantResources = new GrantResources(grantList);
    return Response.ok(grantResources).build();
  }
}
