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
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.util.List;
import java.util.Locale;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.polaris.core.admin.model.AddGrantRequest;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
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
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.CatalogRoleEntity;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.core.secrets.UserSecretsManagerFactory;
import org.apache.polaris.service.admin.api.PolarisCatalogsApiService;
import org.apache.polaris.service.admin.api.PolarisPrincipalRolesApiService;
import org.apache.polaris.service.admin.api.PolarisPrincipalsApiService;
import org.apache.polaris.service.config.RealmEntityManagerFactory;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.events.AfterAddGrantToCatalogRoleEvent;
import org.apache.polaris.service.events.AfterAssignPrincipalRoleEvent;
import org.apache.polaris.service.events.AfterCatalogCreatedEvent;
import org.apache.polaris.service.events.AfterCatalogDeletedEvent;
import org.apache.polaris.service.events.AfterCatalogGetEvent;
import org.apache.polaris.service.events.AfterCatalogListEvent;
import org.apache.polaris.service.events.AfterCatalogRoleAssignToPrincipalRoleEvent;
import org.apache.polaris.service.events.AfterCatalogRoleCreateEvent;
import org.apache.polaris.service.events.AfterCatalogRoleDeleteEvent;
import org.apache.polaris.service.events.AfterCatalogRoleGetEvent;
import org.apache.polaris.service.events.AfterCatalogRoleRevokeFromPrincipalRoleEvent;
import org.apache.polaris.service.events.AfterCatalogRoleUpdateEvent;
import org.apache.polaris.service.events.AfterCatalogRolesListEvent;
import org.apache.polaris.service.events.AfterCatalogUpdatedEvent;
import org.apache.polaris.service.events.AfterCredentialsRotateEvent;
import org.apache.polaris.service.events.AfterListAssigneePrincipalRolesForCatalogRoleEvent;
import org.apache.polaris.service.events.AfterListAssigneePrincipalsForPrincipalRoleEvent;
import org.apache.polaris.service.events.AfterListCatalogRolesForPrincipalRoleEvent;
import org.apache.polaris.service.events.AfterListGrantsForCatalogRoleEvent;
import org.apache.polaris.service.events.AfterPrincipalCreateEvent;
import org.apache.polaris.service.events.AfterPrincipalDeleteEvent;
import org.apache.polaris.service.events.AfterPrincipalGetEvent;
import org.apache.polaris.service.events.AfterPrincipalRoleCreateEvent;
import org.apache.polaris.service.events.AfterPrincipalRoleDeleteEvent;
import org.apache.polaris.service.events.AfterPrincipalRoleGetEvent;
import org.apache.polaris.service.events.AfterPrincipalRoleUpdateEvent;
import org.apache.polaris.service.events.AfterPrincipalRolesAssignedListEvent;
import org.apache.polaris.service.events.AfterPrincipalRolesListEvent;
import org.apache.polaris.service.events.AfterPrincipalUpdateEvent;
import org.apache.polaris.service.events.AfterPrincipalsListEvent;
import org.apache.polaris.service.events.AfterRevokeGrantFromCatalogRoleEvent;
import org.apache.polaris.service.events.AfterRevokePrincipalRoleEvent;
import org.apache.polaris.service.events.BeforeAddGrantToCatalogRoleEvent;
import org.apache.polaris.service.events.BeforeAssignPrincipalRoleEvent;
import org.apache.polaris.service.events.BeforeCatalogCreatedEvent;
import org.apache.polaris.service.events.BeforeCatalogDeletedEvent;
import org.apache.polaris.service.events.BeforeCatalogGetEvent;
import org.apache.polaris.service.events.BeforeCatalogListEvent;
import org.apache.polaris.service.events.BeforeCatalogRoleAssignToPrincipalRoleEvent;
import org.apache.polaris.service.events.BeforeCatalogRoleCreateEvent;
import org.apache.polaris.service.events.BeforeCatalogRoleDeleteEvent;
import org.apache.polaris.service.events.BeforeCatalogRoleGetEvent;
import org.apache.polaris.service.events.BeforeCatalogRoleRevokeFromPrincipalRoleEvent;
import org.apache.polaris.service.events.BeforeCatalogRoleUpdateEvent;
import org.apache.polaris.service.events.BeforeCatalogRolesListEvent;
import org.apache.polaris.service.events.BeforeCatalogUpdatedEvent;
import org.apache.polaris.service.events.BeforeCredentialsRotateEvent;
import org.apache.polaris.service.events.BeforeListAssigneePrincipalRolesForCatalogRoleEvent;
import org.apache.polaris.service.events.BeforeListAssigneePrincipalsForPrincipalRoleEvent;
import org.apache.polaris.service.events.BeforeListCatalogRolesForPrincipalRoleEvent;
import org.apache.polaris.service.events.BeforeListGrantsForCatalogRoleEvent;
import org.apache.polaris.service.events.BeforePrincipalCreateEvent;
import org.apache.polaris.service.events.BeforePrincipalDeleteEvent;
import org.apache.polaris.service.events.BeforePrincipalGetEvent;
import org.apache.polaris.service.events.BeforePrincipalRoleCreateEvent;
import org.apache.polaris.service.events.BeforePrincipalRoleDeleteEvent;
import org.apache.polaris.service.events.BeforePrincipalRoleGetEvent;
import org.apache.polaris.service.events.BeforePrincipalRoleUpdateEvent;
import org.apache.polaris.service.events.BeforePrincipalRolesAssignedListEvent;
import org.apache.polaris.service.events.BeforePrincipalRolesListEvent;
import org.apache.polaris.service.events.BeforePrincipalUpdateEvent;
import org.apache.polaris.service.events.BeforePrincipalsListEvent;
import org.apache.polaris.service.events.BeforeRevokeGrantFromCatalogRoleEvent;
import org.apache.polaris.service.events.BeforeRevokePrincipalRoleEvent;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventListener;
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
  private final RealmEntityManagerFactory entityManagerFactory;
  private final PolarisAuthorizer polarisAuthorizer;
  private final MetaStoreManagerFactory metaStoreManagerFactory;
  private final UserSecretsManagerFactory userSecretsManagerFactory;
  private final CallContext callContext;
  private final ReservedProperties reservedProperties;
  private final PolarisEventListener polarisEventListener;

  @Inject
  public PolarisServiceImpl(
      RealmEntityManagerFactory entityManagerFactory,
      MetaStoreManagerFactory metaStoreManagerFactory,
      UserSecretsManagerFactory userSecretsManagerFactory,
      PolarisAuthorizer polarisAuthorizer,
      CallContext callContext,
      ReservedProperties reservedProperties,
      PolarisEventListener polarisEventListener) {
    this.entityManagerFactory = entityManagerFactory;
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.userSecretsManagerFactory = userSecretsManagerFactory;
    this.polarisAuthorizer = polarisAuthorizer;
    this.callContext = callContext;
    this.reservedProperties = reservedProperties;
    this.polarisEventListener = polarisEventListener;
    // FIXME: This is a hack to set the current context for downstream calls.
    CallContext.setCurrentContext(callContext);
  }

  private PolarisAdminService newAdminService(
      RealmContext realmContext, SecurityContext securityContext) {
    AuthenticatedPolarisPrincipal authenticatedPrincipal =
        (AuthenticatedPolarisPrincipal) securityContext.getUserPrincipal();
    if (authenticatedPrincipal == null) {
      throw new NotAuthorizedException("Failed to find authenticatedPrincipal in SecurityContext");
    }

    PolarisEntityManager entityManager =
        entityManagerFactory.getOrCreateEntityManager(realmContext);
    PolarisMetaStoreManager metaStoreManager =
        metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);
    UserSecretsManager userSecretsManager =
        userSecretsManagerFactory.getOrCreateUserSecretsManager(realmContext);
    return new PolarisAdminService(
        callContext,
        entityManager,
        metaStoreManager,
        userSecretsManager,
        securityContext,
        polarisAuthorizer,
        reservedProperties);
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response createCatalog(
      CreateCatalogRequest request, RealmContext realmContext, SecurityContext securityContext) {
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforeCatalogCreated(
        new BeforeCatalogCreatedEvent(eventId, request.getCatalog().getName()));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    Catalog catalog = request.getCatalog();
    validateStorageConfig(catalog.getStorageConfigInfo());
    validateExternalCatalog(catalog);
    Catalog newCatalog = new CatalogEntity(adminService.createCatalog(request)).asCatalog();
    LOGGER.info("Created new catalog {}", newCatalog);
    polarisEventListener.onAfterCatalogCreated(
        new AfterCatalogCreatedEvent(eventId, newCatalog), callContext);
    return Response.status(Response.Status.CREATED).build();
  }

  private void validateStorageConfig(StorageConfigInfo storageConfigInfo) {
    List<String> allowedStorageTypes =
        callContext
            .getRealmConfig()
            .getConfig(FeatureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES);
    if (!allowedStorageTypes.contains(storageConfigInfo.getStorageType().name())) {
      LOGGER
          .atWarn()
          .addKeyValue("storageConfig", storageConfigInfo)
          .log("Disallowed storage type in catalog");
      throw new IllegalArgumentException(
          "Unsupported storage type: " + storageConfigInfo.getStorageType());
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

  private void validateConnectionConfigInfo(ConnectionConfigInfo connectionConfigInfo) {

    String connectionType = connectionConfigInfo.getConnectionType().name();
    List<String> supportedConnectionTypes =
        callContext
            .getRealmConfig()
            .getConfig(FeatureConfiguration.SUPPORTED_CATALOG_CONNECTION_TYPES)
            .stream()
            .map(s -> s.toUpperCase(Locale.ROOT))
            .toList();
    if (!supportedConnectionTypes.contains(connectionType)) {
      throw new IllegalStateException("Unsupported connection type: " + connectionType);
    }
  }

  private void validateAuthenticationParameters(AuthenticationParameters authenticationParameters) {

    String authenticationType = authenticationParameters.getAuthenticationType().name();
    List<String> supportedAuthenticationTypes =
        callContext
            .getRealmConfig()
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
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforeCatalogDeleted(
        new BeforeCatalogDeletedEvent(eventId, catalogName));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    adminService.deleteCatalog(catalogName);
    polarisEventListener.onAfterCatalogDeleted(
        new AfterCatalogDeletedEvent(eventId, catalogName), callContext);
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response getCatalog(
      String catalogName, RealmContext realmContext, SecurityContext securityContext) {
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforeCatalogGet(new BeforeCatalogGetEvent(eventId, catalogName));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    Catalog catalog = adminService.getCatalog(catalogName).asCatalog();
    Response resp = Response.ok(catalog).build();
    polarisEventListener.onAfterCatalogGet(new AfterCatalogGetEvent(eventId, catalog), callContext);
    return resp;
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response updateCatalog(
      String catalogName,
      UpdateCatalogRequest updateRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforeCatalogUpdated(
        new BeforeCatalogUpdatedEvent(eventId, catalogName, updateRequest));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    if (updateRequest.getStorageConfigInfo() != null) {
      validateStorageConfig(updateRequest.getStorageConfigInfo());
    }
    Catalog catalog = adminService.updateCatalog(catalogName, updateRequest).asCatalog();
    polarisEventListener.onAfterCatalogUpdated(
        new AfterCatalogUpdatedEvent(eventId, catalog), callContext);
    return Response.ok(catalog).build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response listCatalogs(RealmContext realmContext, SecurityContext securityContext) {
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforeCatalogList(new BeforeCatalogListEvent(eventId));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    List<Catalog> catalogList =
        adminService.listCatalogs().stream()
            .map(CatalogEntity::new)
            .map(CatalogEntity::asCatalog)
            .toList();
    Catalogs catalogs = new Catalogs(catalogList);
    LOGGER.debug("listCatalogs returning: {}", catalogs);
    polarisEventListener.onAfterCatalogList(new AfterCatalogListEvent(eventId), callContext);
    return Response.ok(catalogs).build();
  }

  /** From PolarisPrincipalsApiService */
  @Override
  public Response createPrincipal(
      CreatePrincipalRequest request, RealmContext realmContext, SecurityContext securityContext) {
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforePrincipalCreate(
        new BeforePrincipalCreateEvent(eventId, request.getPrincipal().getName()));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
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
    polarisEventListener.onAfterPrincipalCreate(
        new AfterPrincipalCreateEvent(eventId, createdPrincipal.getPrincipal()), callContext);
    return Response.status(Response.Status.CREATED).entity(createdPrincipal).build();
  }

  /** From PolarisPrincipalsApiService */
  @Override
  public Response deletePrincipal(
      String principalName, RealmContext realmContext, SecurityContext securityContext) {
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforePrincipalDelete(
        new BeforePrincipalDeleteEvent(eventId, principalName));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    adminService.deletePrincipal(principalName);
    polarisEventListener.onAfterPrincipalDelete(
        new AfterPrincipalDeleteEvent(eventId, principalName), callContext);
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  /** From PolarisPrincipalsApiService */
  @Override
  public Response getPrincipal(
      String principalName, RealmContext realmContext, SecurityContext securityContext) {
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforePrincipalGet(new BeforePrincipalGetEvent(eventId, principalName));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    Principal principal = adminService.getPrincipal(principalName).asPrincipal();
    Response resp = Response.ok(principal).build();
    polarisEventListener.onAfterPrincipalGet(
        new AfterPrincipalGetEvent(eventId, principal), callContext);
    return resp;
  }

  /** From PolarisPrincipalsApiService */
  @Override
  public Response updatePrincipal(
      String principalName,
      UpdatePrincipalRequest updateRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforePrincipalUpdate(
        new BeforePrincipalUpdateEvent(eventId, principalName, updateRequest));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    Principal updatedPrincipal =
        adminService.updatePrincipal(principalName, updateRequest).asPrincipal();
    polarisEventListener.onAfterPrincipalUpdate(
        new AfterPrincipalUpdateEvent(eventId, updatedPrincipal), callContext);
    return Response.ok(updatedPrincipal).build();
  }

  /** From PolarisPrincipalsApiService */
  @Override
  public Response rotateCredentials(
      String principalName, RealmContext realmContext, SecurityContext securityContext) {
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforeCredentialsRotate(
        new BeforeCredentialsRotateEvent(principalName, eventId));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    PrincipalWithCredentials rotatedPrincipal = adminService.rotateCredentials(principalName);
    polarisEventListener.onAfterCredentialsRotate(
        new AfterCredentialsRotateEvent(eventId, rotatedPrincipal), callContext);
    return Response.ok(rotatedPrincipal).build();
  }

  /** From PolarisPrincipalsApiService */
  @Override
  public Response listPrincipals(RealmContext realmContext, SecurityContext securityContext) {
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforePrincipalsList(new BeforePrincipalsListEvent(eventId));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    List<Principal> principalList =
        adminService.listPrincipals().stream()
            .map(PrincipalEntity::new)
            .map(PrincipalEntity::asPrincipal)
            .toList();
    Principals principals = new Principals(principalList);
    LOGGER.debug("listPrincipals returning: {}", principals);
    polarisEventListener.onAfterPrincipalsList(new AfterPrincipalsListEvent(eventId), callContext);
    return Response.ok(principals).build();
  }

  /** From PolarisPrincipalRolesApiService */
  @Override
  public Response createPrincipalRole(
      CreatePrincipalRoleRequest request,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforePrincipalRoleCreate(
        new BeforePrincipalRoleCreateEvent(eventId, request));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
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
    polarisEventListener.onAfterPrincipalRoleCreate(
        new AfterPrincipalRoleCreateEvent(eventId, newPrincipalRole), callContext);
    return Response.status(Response.Status.CREATED).build();
  }

  /** From PolarisPrincipalRolesApiService */
  @Override
  public Response deletePrincipalRole(
      String principalRoleName, RealmContext realmContext, SecurityContext securityContext) {
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforePrincipalRoleDelete(
        new BeforePrincipalRoleDeleteEvent(principalRoleName, eventId));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    adminService.deletePrincipalRole(principalRoleName);
    polarisEventListener.onAfterPrincipalRoleDelete(
        new AfterPrincipalRoleDeleteEvent(principalRoleName, eventId), callContext);
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  /** From PolarisPrincipalRolesApiService */
  @Override
  public Response getPrincipalRole(
      String principalRoleName, RealmContext realmContext, SecurityContext securityContext) {
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforePrincipalRoleGet(
        new BeforePrincipalRoleGetEvent(principalRoleName, eventId));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    PrincipalRole principalRole =
        adminService.getPrincipalRole(principalRoleName).asPrincipalRole();
    polarisEventListener.onAfterPrincipalRoleGet(
        new AfterPrincipalRoleGetEvent(eventId, principalRole), callContext);
    return Response.ok(principalRole).build();
  }

  /** From PolarisPrincipalRolesApiService */
  @Override
  public Response updatePrincipalRole(
      String principalRoleName,
      UpdatePrincipalRoleRequest updateRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforePrincipalRoleUpdate(
        new BeforePrincipalRoleUpdateEvent(eventId, principalRoleName, updateRequest));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    PrincipalRole updatedPrincipalRole =
        adminService.updatePrincipalRole(principalRoleName, updateRequest).asPrincipalRole();
    polarisEventListener.onAfterPrincipalRoleUpdate(
        new AfterPrincipalRoleUpdateEvent(eventId, updatedPrincipalRole), callContext);
    return Response.ok(updatedPrincipalRole).build();
  }

  /** From PolarisPrincipalRolesApiService */
  @Override
  public Response listPrincipalRoles(RealmContext realmContext, SecurityContext securityContext) {
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforePrincipalRolesList(new BeforePrincipalRolesListEvent(eventId));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    List<PrincipalRole> principalRoleList =
        adminService.listPrincipalRoles().stream()
            .map(PrincipalRoleEntity::new)
            .map(PrincipalRoleEntity::asPrincipalRole)
            .toList();
    PrincipalRoles principalRoles = new PrincipalRoles(principalRoleList);
    LOGGER.debug("listPrincipalRoles returning: {}", principalRoles);
    polarisEventListener.onAfterPrincipalRolesList(
        new AfterPrincipalRolesListEvent(eventId), callContext);
    return Response.ok(principalRoles).build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response createCatalogRole(
      String catalogName,
      CreateCatalogRoleRequest request,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforeCatalogRoleCreate(
        new BeforeCatalogRoleCreateEvent(catalogName, request.getCatalogRole().getName(), eventId));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
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
    polarisEventListener.onAfterCatalogRoleCreate(
        new AfterCatalogRoleCreateEvent(eventId, catalogName, newCatalogRole), callContext);
    return Response.status(Response.Status.CREATED).build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response deleteCatalogRole(
      String catalogName,
      String catalogRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforeCatalogRoleDelete(
        new BeforeCatalogRoleDeleteEvent(catalogName, catalogRoleName, eventId));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    adminService.deleteCatalogRole(catalogName, catalogRoleName);
    polarisEventListener.onAfterCatalogRoleDelete(
        new AfterCatalogRoleDeleteEvent(catalogName, catalogRoleName, eventId), callContext);
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response getCatalogRole(
      String catalogName,
      String catalogRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforeCatalogRoleGet(
        new BeforeCatalogRoleGetEvent(catalogName, catalogRoleName, eventId));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    CatalogRole catalogRole =
        adminService.getCatalogRole(catalogName, catalogRoleName).asCatalogRole();
    Response resp = Response.ok(catalogRole).build();
    polarisEventListener.onAfterCatalogRoleGet(
        new AfterCatalogRoleGetEvent(eventId, catalogName, catalogRole), callContext);
    return resp;
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response updateCatalogRole(
      String catalogName,
      String catalogRoleName,
      UpdateCatalogRoleRequest updateRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforeCatalogRoleUpdate(
        new BeforeCatalogRoleUpdateEvent(eventId, catalogName, catalogRoleName, updateRequest));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    CatalogRole updatedCatalogRole =
        adminService.updateCatalogRole(catalogName, catalogRoleName, updateRequest).asCatalogRole();
    polarisEventListener.onAfterCatalogRoleUpdate(
        new AfterCatalogRoleUpdateEvent(eventId, catalogName, updatedCatalogRole), callContext);
    return Response.ok(updatedCatalogRole).build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response listCatalogRoles(
      String catalogName, RealmContext realmContext, SecurityContext securityContext) {
    LOGGER.info("Listing catalog roles for catalog {}", catalogName);
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforeCatalogRolesList(
        new BeforeCatalogRolesListEvent(catalogName, eventId));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    List<CatalogRole> catalogRoleList =
        adminService.listCatalogRoles(catalogName).stream()
            .map(CatalogRoleEntity::new)
            .map(CatalogRoleEntity::asCatalogRole)
            .toList();
    CatalogRoles catalogRoles = new CatalogRoles(catalogRoleList);
    polarisEventListener.onAfterCatalogRolesList(
        new AfterCatalogRolesListEvent(catalogName, eventId), callContext);
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
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforeAssignPrincipalRole(
        new BeforeAssignPrincipalRoleEvent(eventId, principalName, request.getPrincipalRole()));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    adminService.assignPrincipalRole(principalName, request.getPrincipalRole().getName());
    polarisEventListener.onAfterAssignPrincipalRole(
        new AfterAssignPrincipalRoleEvent(eventId, principalName, request.getPrincipalRole()),
        callContext);
    return Response.status(Response.Status.CREATED).build();
  }

  /** From PolarisPrincipalsApiService */
  @Override
  public Response revokePrincipalRole(
      String principalName,
      String principalRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    LOGGER.info("Revoking principalRole {} from principal {}", principalRoleName, principalName);
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforeRevokePrincipalRole(
        new BeforeRevokePrincipalRoleEvent(principalName, principalRoleName, eventId));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    adminService.revokePrincipalRole(principalName, principalRoleName);
    polarisEventListener.onAfterRevokePrincipalRole(
        new AfterRevokePrincipalRoleEvent(principalName, principalRoleName, eventId), callContext);
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  /** From PolarisPrincipalsApiService */
  @Override
  public Response listPrincipalRolesAssigned(
      String principalName, RealmContext realmContext, SecurityContext securityContext) {
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforePrincipalRolesAssignedList(
        new BeforePrincipalRolesAssignedListEvent(principalName, eventId));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    List<PrincipalRole> principalRoleList =
        adminService.listPrincipalRolesAssigned(principalName).stream()
            .map(PrincipalRoleEntity::new)
            .map(PrincipalRoleEntity::asPrincipalRole)
            .toList();
    PrincipalRoles principalRoles = new PrincipalRoles(principalRoleList);
    LOGGER.debug("listPrincipalRolesAssigned returning: {}", principalRoles);
    polarisEventListener.onAfterPrincipalRolesAssignedList(
        new AfterPrincipalRolesAssignedListEvent(principalName, eventId), callContext);
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
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforeCatalogRoleAssignToPrincipalRole(
        new BeforeCatalogRoleAssignToPrincipalRoleEvent(
            eventId, principalRoleName, catalogName, request.getCatalogRole()));
    LOGGER.info(
        "Assigning catalogRole {} in catalog {} to principalRole {}",
        request.getCatalogRole().getName(),
        catalogName,
        principalRoleName);
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    adminService.assignCatalogRoleToPrincipalRole(
        principalRoleName, catalogName, request.getCatalogRole().getName());
    polarisEventListener.onAfterCatalogRoleAssignToPrincipalRole(
        new AfterCatalogRoleAssignToPrincipalRoleEvent(
            principalRoleName, catalogName, request.getCatalogRole().getName(), eventId),
        callContext);
    return Response.status(Response.Status.CREATED).build();
  }

  /** From PolarisPrincipalRolesApiService */
  @Override
  public Response revokeCatalogRoleFromPrincipalRole(
      String principalRoleName,
      String catalogName,
      String catalogRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforeCatalogRoleRevokeFromPrincipalRole(
        new BeforeCatalogRoleRevokeFromPrincipalRoleEvent(
            principalRoleName, catalogName, catalogRoleName, eventId));
    LOGGER.info(
        "Revoking catalogRole {} in catalog {} from principalRole {}",
        catalogRoleName,
        catalogName,
        principalRoleName);
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    adminService.revokeCatalogRoleFromPrincipalRole(
        principalRoleName, catalogName, catalogRoleName);
    polarisEventListener.onAfterCatalogRoleRevokeFromPrincipalRole(
        new AfterCatalogRoleRevokeFromPrincipalRoleEvent(
            principalRoleName, catalogName, catalogRoleName, eventId),
        callContext);
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  /** From PolarisPrincipalRolesApiService */
  @Override
  public Response listAssigneePrincipalsForPrincipalRole(
      String principalRoleName, RealmContext realmContext, SecurityContext securityContext) {
    LOGGER.info("Listing assignee principals for principalRole {}", principalRoleName);
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforeListAssigneePrincipalsForPrincipalRole(
        new BeforeListAssigneePrincipalsForPrincipalRoleEvent(principalRoleName, eventId));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    List<Principal> principalList =
        adminService.listAssigneePrincipalsForPrincipalRole(principalRoleName).stream()
            .map(PrincipalEntity::new)
            .map(PrincipalEntity::asPrincipal)
            .toList();
    Principals principals = new Principals(principalList);
    polarisEventListener.onAfterListAssigneePrincipalsForPrincipalRole(
        new AfterListAssigneePrincipalsForPrincipalRoleEvent(principalRoleName, eventId),
        callContext);
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
    LOGGER.info(
        "Listing catalog roles for principalRole {} in catalog {}", principalRoleName, catalogName);
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforeListCatalogRolesForPrincipalRole(
        new BeforeListCatalogRolesForPrincipalRoleEvent(principalRoleName, catalogName, eventId));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    List<CatalogRole> catalogRoleList =
        adminService.listCatalogRolesForPrincipalRole(principalRoleName, catalogName).stream()
            .map(CatalogRoleEntity::new)
            .map(CatalogRoleEntity::asCatalogRole)
            .toList();
    CatalogRoles catalogRoles = new CatalogRoles(catalogRoleList);
    polarisEventListener.onAfterListCatalogRolesForPrincipalRole(
        new AfterListCatalogRolesForPrincipalRoleEvent(principalRoleName, catalogName, eventId),
        callContext);
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
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforeAddGrantToCatalogRole(
        new BeforeAddGrantToCatalogRoleEvent(eventId, catalogName, catalogRoleName, grantRequest));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    PolarisPrivilege privilege;
    switch (grantRequest.getGrant()) {
      // The per-securable-type Privilege enums must be exact String match for a subset of all
      // PolarisPrivilege values.
      case ViewGrant viewGrant:
        {
          privilege = PolarisPrivilege.valueOf(viewGrant.getPrivilege().toString());
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
          privilege = PolarisPrivilege.valueOf(tableGrant.getPrivilege().toString());
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
          privilege = PolarisPrivilege.valueOf(namespaceGrant.getPrivilege().toString());
          String[] namespaceParts = namespaceGrant.getNamespace().toArray(new String[0]);
          adminService.grantPrivilegeOnNamespaceToRole(
              catalogName, catalogRoleName, Namespace.of(namespaceParts), privilege);
          break;
        }
      case CatalogGrant catalogGrant:
        {
          privilege = PolarisPrivilege.valueOf(catalogGrant.getPrivilege().toString());
          adminService.grantPrivilegeOnCatalogToRole(catalogName, catalogRoleName, privilege);
          break;
        }
      case PolicyGrant policyGrant:
        {
          privilege = PolarisPrivilege.valueOf(policyGrant.getPrivilege().toString());
          String policyName = policyGrant.getPolicyName();
          String[] namespaceParts = policyGrant.getNamespace().toArray(new String[0]);
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
    polarisEventListener.onAfterAddGrantToCatalogRole(
        new AfterAddGrantToCatalogRoleEvent(
            eventId, catalogName, catalogRoleName, privilege, grantRequest.getGrant()),
        callContext);
    return Response.status(Response.Status.CREATED).build();
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
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforeRevokeGrantFromCatalogRole(
        new BeforeRevokeGrantFromCatalogRoleEvent(
            eventId, catalogName, catalogRoleName, grantRequest, cascade));
    if (cascade != null && cascade) {
      LOGGER.warn("Tried to use unimplemented 'cascade' feature when revoking grants.");
      return Response.status(501).build(); // not implemented
    }

    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    PolarisPrivilege privilege;
    switch (grantRequest.getGrant()) {
      // The per-securable-type Privilege enums must be exact String match for a subset of all
      // PolarisPrivilege values.
      case ViewGrant viewGrant:
        {
          privilege = PolarisPrivilege.valueOf(viewGrant.getPrivilege().toString());
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
          privilege = PolarisPrivilege.valueOf(tableGrant.getPrivilege().toString());
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
          privilege = PolarisPrivilege.valueOf(namespaceGrant.getPrivilege().toString());
          String[] namespaceParts = namespaceGrant.getNamespace().toArray(new String[0]);
          adminService.revokePrivilegeOnNamespaceFromRole(
              catalogName, catalogRoleName, Namespace.of(namespaceParts), privilege);
          break;
        }
      case CatalogGrant catalogGrant:
        {
          privilege = PolarisPrivilege.valueOf(catalogGrant.getPrivilege().toString());
          adminService.revokePrivilegeOnCatalogFromRole(catalogName, catalogRoleName, privilege);
          break;
        }
      case PolicyGrant policyGrant:
        {
          privilege = PolarisPrivilege.valueOf(policyGrant.getPrivilege().toString());
          String policyName = policyGrant.getPolicyName();
          String[] namespaceParts = policyGrant.getNamespace().toArray(new String[0]);
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
    polarisEventListener.onAfterRevokeGrantFromCatalogRole(
        new AfterRevokeGrantFromCatalogRoleEvent(
            eventId, catalogName, catalogRoleName, privilege, grantRequest.getGrant(), cascade),
        callContext);
    return Response.status(Response.Status.CREATED).build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response listAssigneePrincipalRolesForCatalogRole(
      String catalogName,
      String catalogRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    LOGGER.info(
        "Listing assignee principal roles for catalog role {} in catalog {}",
        catalogRoleName,
        catalogName);
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforeListAssigneePrincipalRolesForCatalogRole(
        new BeforeListAssigneePrincipalRolesForCatalogRoleEvent(
            catalogName, catalogRoleName, eventId));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    List<PrincipalRole> principalRoleList =
        adminService.listAssigneePrincipalRolesForCatalogRole(catalogName, catalogRoleName).stream()
            .map(PrincipalRoleEntity::new)
            .map(PrincipalRoleEntity::asPrincipalRole)
            .toList();
    PrincipalRoles principalRoles = new PrincipalRoles(principalRoleList);
    LOGGER.debug("listAssigneePrincipalRolesForCatalogRole returning: {}", principalRoles);
    polarisEventListener.onAfterListAssigneePrincipalRolesForCatalogRole(
        new AfterListAssigneePrincipalRolesForCatalogRoleEvent(
            catalogName, catalogRoleName, eventId),
        callContext);
    return Response.ok(principalRoles).build();
  }

  /** From PolarisCatalogsApiService */
  @Override
  public Response listGrantsForCatalogRole(
      String catalogName,
      String catalogRoleName,
      RealmContext realmContext,
      SecurityContext securityContext) {
    LOGGER.info("Listing grants for catalog role {} in catalog {}", catalogRoleName, catalogName);
    String eventId = PolarisEvent.createEventId();
    polarisEventListener.onBeforeListGrantsForCatalogRole(
        new BeforeListGrantsForCatalogRoleEvent(catalogName, catalogRoleName, eventId));
    PolarisAdminService adminService = newAdminService(realmContext, securityContext);
    List<GrantResource> grantList =
        adminService.listGrantsForCatalogRole(catalogName, catalogRoleName);
    GrantResources grantResources = new GrantResources(grantList);
    LOGGER.debug("listGrantsForCatalogRole returning: {}", grantResources);
    polarisEventListener.onAfterListGrantsForCatalogRole(
        new AfterListGrantsForCatalogRoleEvent(catalogName, catalogRoleName, eventId), callContext);
    return Response.ok(grantResources).build();
  }
}
