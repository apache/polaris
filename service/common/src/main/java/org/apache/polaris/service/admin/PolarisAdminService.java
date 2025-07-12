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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.core.SecurityContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.BearerAuthenticationParameters;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.NamespaceGrant;
import org.apache.polaris.core.admin.model.NamespacePrivilege;
import org.apache.polaris.core.admin.model.OAuthClientCredentialsParameters;
import org.apache.polaris.core.admin.model.PolicyGrant;
import org.apache.polaris.core.admin.model.PolicyPrivilege;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.PrincipalWithCredentialsCredentials;
import org.apache.polaris.core.admin.model.TableGrant;
import org.apache.polaris.core.admin.model.TablePrivilege;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.admin.model.UpdateCatalogRoleRequest;
import org.apache.polaris.core.admin.model.UpdatePrincipalRequest;
import org.apache.polaris.core.admin.model.UpdatePrincipalRoleRequest;
import org.apache.polaris.core.admin.model.ViewGrant;
import org.apache.polaris.core.admin.model.ViewPrivilege;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.connection.AuthenticationParametersDpo;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.CatalogRoleEntity;
import org.apache.polaris.core.entity.NamespaceEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.entity.table.federated.FederatedEntities;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.dao.entity.CreateCatalogResult;
import org.apache.polaris.core.persistence.dao.entity.CreatePrincipalResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.LoadGrantsResult;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.core.policy.PolicyEntity;
import org.apache.polaris.core.policy.exceptions.NoSuchPolicyException;
import org.apache.polaris.core.secrets.UserSecretReference;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.apache.polaris.service.catalog.common.CatalogHandler;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.types.PolicyIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Just as an Iceberg Catalog represents the logical model of Iceberg business logic to manage
 * Namespaces, Tables and Views, abstracted away from Iceberg REST objects, this class represents
 * the logical model for managing realm-level Catalogs, Principals, Roles, and Grants.
 *
 * <p>Different API implementors could expose different REST, gRPC, etc., interfaces that delegate
 * to this logical model without being tightly coupled to a single frontend protocol, and can
 * provide different implementations of PolarisEntityManager to abstract away the implementation of
 * the persistence layer.
 */
public class PolarisAdminService {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisAdminService.class);

  private final CallContext callContext;
  private final PolarisEntityManager entityManager;
  private final SecurityContext securityContext;
  private final AuthenticatedPolarisPrincipal authenticatedPrincipal;
  private final PolarisAuthorizer authorizer;
  private final PolarisMetaStoreManager metaStoreManager;
  private final UserSecretsManager userSecretsManager;
  private final ReservedProperties reservedProperties;

  // Initialized in the authorize methods.
  private PolarisResolutionManifest resolutionManifest = null;

  public PolarisAdminService(
      @NotNull CallContext callContext,
      @NotNull PolarisEntityManager entityManager,
      @NotNull PolarisMetaStoreManager metaStoreManager,
      @NotNull UserSecretsManager userSecretsManager,
      @NotNull SecurityContext securityContext,
      @NotNull PolarisAuthorizer authorizer,
      @NotNull ReservedProperties reservedProperties) {
    this.callContext = callContext;
    this.entityManager = entityManager;
    this.metaStoreManager = metaStoreManager;
    this.securityContext = securityContext;
    PolarisDiagnostics diagServices = callContext.getPolarisCallContext().getDiagServices();
    diagServices.checkNotNull(securityContext, "null_security_context");
    diagServices.checkNotNull(securityContext.getUserPrincipal(), "null_security_context");
    diagServices.check(
        securityContext.getUserPrincipal() instanceof AuthenticatedPolarisPrincipal,
        "unexpected_principal_type",
        "class={}",
        securityContext.getUserPrincipal().getClass().getName());
    this.authenticatedPrincipal =
        (AuthenticatedPolarisPrincipal) securityContext.getUserPrincipal();
    this.authorizer = authorizer;
    this.userSecretsManager = userSecretsManager;
    this.reservedProperties = reservedProperties;
  }

  private PolarisCallContext getCurrentPolarisContext() {
    return callContext.getPolarisCallContext();
  }

  private UserSecretsManager getUserSecretsManager() {
    return userSecretsManager;
  }

  private Optional<CatalogEntity> findCatalogByName(String name) {
    return Optional.ofNullable(resolutionManifest.getResolvedReferenceCatalogEntity())
        .map(path -> CatalogEntity.of(path.getRawLeafEntity()));
  }

  private Optional<PrincipalEntity> findPrincipalByName(String name) {
    return Optional.ofNullable(
            resolutionManifest.getResolvedTopLevelEntity(name, PolarisEntityType.PRINCIPAL))
        .map(path -> PrincipalEntity.of(path.getRawLeafEntity()));
  }

  private Optional<PrincipalRoleEntity> findPrincipalRoleByName(String name) {
    return Optional.ofNullable(
            resolutionManifest.getResolvedTopLevelEntity(name, PolarisEntityType.PRINCIPAL_ROLE))
        .map(path -> PrincipalRoleEntity.of(path.getRawLeafEntity()));
  }

  private Optional<CatalogRoleEntity> findCatalogRoleByName(String catalogName, String name) {
    return Optional.ofNullable(resolutionManifest.getResolvedPath(name))
        .map(path -> CatalogRoleEntity.of(path.getRawLeafEntity()));
  }

  private void authorizeBasicRootOperationOrThrow(PolarisAuthorizableOperation op) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(
            callContext, securityContext, null /* referenceCatalogName */);
    resolutionManifest.resolveAll();
    PolarisResolvedPathWrapper rootContainerWrapper =
        resolutionManifest.getResolvedRootContainerEntityAsPath();
    authorizer.authorizeOrThrow(
        callContext,
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedPrincipalRoleEntities(),
        op,
        rootContainerWrapper,
        null /* secondary */);
  }

  private void authorizeBasicTopLevelEntityOperationOrThrow(
      PolarisAuthorizableOperation op, String topLevelEntityName, PolarisEntityType entityType) {
    String referenceCatalogName =
        entityType == PolarisEntityType.CATALOG ? topLevelEntityName : null;
    authorizeBasicTopLevelEntityOperationOrThrow(
        op, topLevelEntityName, entityType, referenceCatalogName);
  }

  private void authorizeBasicTopLevelEntityOperationOrThrow(
      PolarisAuthorizableOperation op,
      String topLevelEntityName,
      PolarisEntityType entityType,
      @Nullable String referenceCatalogName) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, securityContext, referenceCatalogName);
    resolutionManifest.addTopLevelName(topLevelEntityName, entityType, false /* isOptional */);
    ResolverStatus status = resolutionManifest.resolveAll();
    if (status.getStatus() == ResolverStatus.StatusEnum.ENTITY_COULD_NOT_BE_RESOLVED) {
      throw new NotFoundException(
          "TopLevelEntity of type %s does not exist: %s", entityType, topLevelEntityName);
    }
    PolarisResolvedPathWrapper topLevelEntityWrapper =
        resolutionManifest.getResolvedTopLevelEntity(topLevelEntityName, entityType);

    // TODO: If we do add more "self" privilege operations for PRINCIPAL targets this should
    // be extracted into an EnumSet and/or pushed down into PolarisAuthorizer.
    if (topLevelEntityWrapper.getResolvedLeafEntity().getEntity().getId()
            == authenticatedPrincipal.getPrincipalEntity().getId()
        && (op.equals(PolarisAuthorizableOperation.ROTATE_CREDENTIALS)
            || op.equals(PolarisAuthorizableOperation.RESET_CREDENTIALS))) {
      LOGGER
          .atDebug()
          .addKeyValue("principalName", topLevelEntityName)
          .log("Allowing rotate own credentials");
      return;
    }
    authorizer.authorizeOrThrow(
        callContext,
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        topLevelEntityWrapper,
        null /* secondary */);
  }

  private void authorizeBasicCatalogRoleOperationOrThrow(
      PolarisAuthorizableOperation op, String catalogName, String catalogRoleName) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, securityContext, catalogName);
    resolutionManifest.addPath(
        new ResolverPath(List.of(catalogRoleName), PolarisEntityType.CATALOG_ROLE),
        catalogRoleName);
    resolutionManifest.resolveAll();
    PolarisResolvedPathWrapper target = resolutionManifest.getResolvedPath(catalogRoleName, true);
    if (target == null) {
      throw new NotFoundException("CatalogRole does not exist: %s", catalogRoleName);
    }
    authorizer.authorizeOrThrow(
        callContext,
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        target,
        null /* secondary */);
  }

  private void authorizeGrantOnRootContainerToPrincipalRoleOperationOrThrow(
      PolarisAuthorizableOperation op, String principalRoleName) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, securityContext, null);
    resolutionManifest.addTopLevelName(
        principalRoleName, PolarisEntityType.PRINCIPAL_ROLE, false /* isOptional */);
    ResolverStatus status = resolutionManifest.resolveAll();

    if (status.getStatus() == ResolverStatus.StatusEnum.ENTITY_COULD_NOT_BE_RESOLVED) {
      throw new NotFoundException(
          "Entity %s not found when trying to grant on root to %s",
          status.getFailedToResolvedEntityName(), principalRoleName);
    }

    // TODO: Merge this method into authorizeGrantOnTopLevelEntityToPrincipalRoleOperationOrThrow
    // once we remove any special handling logic for the rootContainer.
    PolarisResolvedPathWrapper rootContainerWrapper =
        resolutionManifest.getResolvedRootContainerEntityAsPath();
    PolarisResolvedPathWrapper principalRoleWrapper =
        resolutionManifest.getResolvedTopLevelEntity(
            principalRoleName, PolarisEntityType.PRINCIPAL_ROLE);

    authorizer.authorizeOrThrow(
        callContext,
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        rootContainerWrapper,
        principalRoleWrapper);
  }

  private void authorizeGrantOnTopLevelEntityToPrincipalRoleOperationOrThrow(
      PolarisAuthorizableOperation op,
      String topLevelEntityName,
      PolarisEntityType topLevelEntityType,
      String principalRoleName) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, securityContext, null);
    resolutionManifest.addTopLevelName(
        topLevelEntityName, topLevelEntityType, false /* isOptional */);
    resolutionManifest.addTopLevelName(
        principalRoleName, PolarisEntityType.PRINCIPAL_ROLE, false /* isOptional */);
    ResolverStatus status = resolutionManifest.resolveAll();

    if (status.getStatus() == ResolverStatus.StatusEnum.ENTITY_COULD_NOT_BE_RESOLVED) {
      throw new NotFoundException(
          "Entity %s not found when trying to assign %s of type %s to %s",
          status.getFailedToResolvedEntityName(),
          topLevelEntityName,
          topLevelEntityType,
          principalRoleName);
    }

    PolarisResolvedPathWrapper topLevelEntityWrapper =
        resolutionManifest.getResolvedTopLevelEntity(topLevelEntityName, topLevelEntityType);
    PolarisResolvedPathWrapper principalRoleWrapper =
        resolutionManifest.getResolvedTopLevelEntity(
            principalRoleName, PolarisEntityType.PRINCIPAL_ROLE);

    authorizer.authorizeOrThrow(
        callContext,
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        topLevelEntityWrapper,
        principalRoleWrapper);
  }

  private void authorizeGrantOnPrincipalRoleToPrincipalOperationOrThrow(
      PolarisAuthorizableOperation op, String principalRoleName, String principalName) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, securityContext, null);
    resolutionManifest.addTopLevelName(
        principalRoleName, PolarisEntityType.PRINCIPAL_ROLE, false /* isOptional */);
    resolutionManifest.addTopLevelName(
        principalName, PolarisEntityType.PRINCIPAL, false /* isOptional */);
    ResolverStatus status = resolutionManifest.resolveAll();

    if (status.getStatus() == ResolverStatus.StatusEnum.ENTITY_COULD_NOT_BE_RESOLVED) {
      throw new NotFoundException(
          "Entity %s not found when trying to assign %s to %s",
          status.getFailedToResolvedEntityName(), principalRoleName, principalName);
    }

    PolarisResolvedPathWrapper principalRoleWrapper =
        resolutionManifest.getResolvedTopLevelEntity(
            principalRoleName, PolarisEntityType.PRINCIPAL_ROLE);
    PolarisResolvedPathWrapper principalWrapper =
        resolutionManifest.getResolvedTopLevelEntity(principalName, PolarisEntityType.PRINCIPAL);

    authorizer.authorizeOrThrow(
        callContext,
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        principalRoleWrapper,
        principalWrapper);
  }

  private void authorizeGrantOnCatalogRoleToPrincipalRoleOperationOrThrow(
      PolarisAuthorizableOperation op,
      String catalogName,
      String catalogRoleName,
      String principalRoleName) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, securityContext, catalogName);
    resolutionManifest.addPath(
        new ResolverPath(List.of(catalogRoleName), PolarisEntityType.CATALOG_ROLE),
        catalogRoleName);
    resolutionManifest.addTopLevelName(
        principalRoleName, PolarisEntityType.PRINCIPAL_ROLE, false /* isOptional */);
    ResolverStatus status = resolutionManifest.resolveAll();

    if (status.getStatus() == ResolverStatus.StatusEnum.ENTITY_COULD_NOT_BE_RESOLVED) {
      throw new NotFoundException(
          "Entity %s not found when trying to assign %s.%s to %s",
          status.getFailedToResolvedEntityName(), catalogName, catalogRoleName, principalRoleName);
    } else if (status.getStatus() == ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED) {
      throw new NotFoundException(
          "Entity %s not found when trying to assign %s.%s to %s",
          status.getFailedToResolvePath(), catalogName, catalogRoleName, principalRoleName);
    }

    PolarisResolvedPathWrapper principalRoleWrapper =
        resolutionManifest.getResolvedTopLevelEntity(
            principalRoleName, PolarisEntityType.PRINCIPAL_ROLE);
    PolarisResolvedPathWrapper catalogRoleWrapper =
        resolutionManifest.getResolvedPath(catalogRoleName, true);

    authorizer.authorizeOrThrow(
        callContext,
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        catalogRoleWrapper,
        principalRoleWrapper);
  }

  private void authorizeGrantOnCatalogOperationOrThrow(
      PolarisAuthorizableOperation op, String catalogName, String catalogRoleName) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, securityContext, catalogName);
    resolutionManifest.addTopLevelName(
        catalogName, PolarisEntityType.CATALOG, false /* isOptional */);
    resolutionManifest.addPath(
        new ResolverPath(List.of(catalogRoleName), PolarisEntityType.CATALOG_ROLE),
        catalogRoleName);
    ResolverStatus status = resolutionManifest.resolveAll();

    if (status.getStatus() == ResolverStatus.StatusEnum.ENTITY_COULD_NOT_BE_RESOLVED) {
      throw new NotFoundException("Catalog not found: %s", catalogName);
    } else if (status.getStatus() == ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED) {
      throw new NotFoundException("CatalogRole not found: %s.%s", catalogName, catalogRoleName);
    }

    PolarisResolvedPathWrapper catalogWrapper =
        resolutionManifest.getResolvedTopLevelEntity(catalogName, PolarisEntityType.CATALOG);
    PolarisResolvedPathWrapper catalogRoleWrapper =
        resolutionManifest.getResolvedPath(catalogRoleName, true);
    authorizer.authorizeOrThrow(
        callContext,
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        catalogWrapper,
        catalogRoleWrapper);
  }

  private void authorizeGrantOnNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op,
      String catalogName,
      Namespace namespace,
      String catalogRoleName) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, securityContext, catalogName);
    resolutionManifest.addPath(
        new ResolverPath(Arrays.asList(namespace.levels()), PolarisEntityType.NAMESPACE),
        namespace);
    resolutionManifest.addPath(
        new ResolverPath(List.of(catalogRoleName), PolarisEntityType.CATALOG_ROLE),
        catalogRoleName);
    ResolverStatus status = resolutionManifest.resolveAll();

    if (status.getStatus() == ResolverStatus.StatusEnum.ENTITY_COULD_NOT_BE_RESOLVED) {
      throw new NotFoundException("Catalog not found: %s", catalogName);
    } else if (status.getStatus() == ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED) {
      if (status.getFailedToResolvePath().getLastEntityType() == PolarisEntityType.NAMESPACE) {
        throw new NoSuchNamespaceException(
            "Namespace does not exist: %s", status.getFailedToResolvePath().getEntityNames());
      } else {
        throw new NotFoundException("CatalogRole not found: %s.%s", catalogName, catalogRoleName);
      }
    }

    PolarisResolvedPathWrapper namespaceWrapper =
        resolutionManifest.getResolvedPath(namespace, true);
    PolarisResolvedPathWrapper catalogRoleWrapper =
        resolutionManifest.getResolvedPath(catalogRoleName, true);

    authorizer.authorizeOrThrow(
        callContext,
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        namespaceWrapper,
        catalogRoleWrapper);
  }

  private void authorizeGrantOnTableLikeOperationOrThrow(
      PolarisAuthorizableOperation op,
      String catalogName,
      List<PolarisEntitySubType> subTypes,
      TableIdentifier identifier,
      String catalogRoleName) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, securityContext, catalogName);
    resolutionManifest.addPath(
        new ResolverPath(
            PolarisCatalogHelpers.tableIdentifierToList(identifier), PolarisEntityType.TABLE_LIKE),
        identifier);
    resolutionManifest.addPath(
        new ResolverPath(List.of(catalogRoleName), PolarisEntityType.CATALOG_ROLE),
        catalogRoleName);
    ResolverStatus status = resolutionManifest.resolveAll();

    if (status.getStatus() == ResolverStatus.StatusEnum.ENTITY_COULD_NOT_BE_RESOLVED) {
      throw new NotFoundException("Catalog not found: %s", catalogName);
    } else if (status.getStatus() == ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED) {
      if (status.getFailedToResolvePath().getLastEntityType() == PolarisEntityType.TABLE_LIKE) {
        CatalogHandler.throwNotFoundExceptionForTableLikeEntity(identifier, subTypes);
      } else {
        throw new NotFoundException("CatalogRole not found: %s.%s", catalogName, catalogRoleName);
      }
    }

    PolarisResolvedPathWrapper tableLikeWrapper =
        resolutionManifest.getResolvedPath(
            identifier, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ANY_SUBTYPE, true);
    if (!subTypes.contains(tableLikeWrapper.getRawLeafEntity().getSubType())) {
      CatalogHandler.throwNotFoundExceptionForTableLikeEntity(identifier, subTypes);
    }

    PolarisResolvedPathWrapper catalogRoleWrapper =
        resolutionManifest.getResolvedPath(catalogRoleName, true);

    authorizer.authorizeOrThrow(
        callContext,
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        tableLikeWrapper,
        catalogRoleWrapper);
  }

  private void authorizeGrantOnPolicyOperationOrThrow(
      PolarisAuthorizableOperation op,
      String catalogName,
      PolicyIdentifier identifier,
      String catalogRoleName) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, securityContext, catalogName);
    resolutionManifest.addPath(
        new ResolverPath(
            PolarisCatalogHelpers.identifierToList(identifier.getNamespace(), identifier.getName()),
            PolarisEntityType.POLICY),
        identifier);
    resolutionManifest.addPath(
        new ResolverPath(List.of(catalogRoleName), PolarisEntityType.CATALOG_ROLE),
        catalogRoleName);
    ResolverStatus status = resolutionManifest.resolveAll();
    if (status.getStatus() == ResolverStatus.StatusEnum.ENTITY_COULD_NOT_BE_RESOLVED) {
      throw new NotFoundException("Catalog not found: %s", catalogName);
    } else if (status.getStatus() == ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED) {
      if (status.getFailedToResolvePath().getLastEntityType() == PolarisEntityType.POLICY) {
        throw new NoSuchPolicyException(String.format("Policy does not exist: %s", identifier));
      } else {
        throw new NotFoundException("CatalogRole not found: %s.%s", catalogName, catalogRoleName);
      }
    }

    PolarisResolvedPathWrapper policyWrapper = resolutionManifest.getResolvedPath(identifier, true);
    PolarisResolvedPathWrapper catalogRoleWrapper =
        resolutionManifest.getResolvedPath(catalogRoleName, true);

    authorizer.authorizeOrThrow(
        callContext,
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        policyWrapper,
        catalogRoleWrapper);
  }

  /** Get all locations where data for a `CatalogEntity` may be stored */
  private Set<String> getCatalogLocations(CatalogEntity catalogEntity) {
    HashSet<String> catalogLocations = new HashSet<>();
    catalogLocations.add(terminateWithSlash(catalogEntity.getBaseLocation()));
    if (catalogEntity.getStorageConfigurationInfo() != null) {
      catalogLocations.addAll(
          catalogEntity.getStorageConfigurationInfo().getAllowedLocations().stream()
              .map(this::terminateWithSlash)
              .toList());
    }
    return catalogLocations;
  }

  /** Ensure a path is terminated with a `/` */
  private String terminateWithSlash(String path) {
    if (path == null) {
      return null;
    } else if (path.endsWith("/")) {
      return path;
    }
    return path + "/";
  }

  /**
   * True if the `CatalogEntity` has a default base location or allowed location that overlaps with
   * that of any existing catalog. If `ALLOW_OVERLAPPING_CATALOG_URLS` is set to true, this check
   * will be skipped.
   */
  private boolean catalogOverlapsWithExistingCatalog(CatalogEntity catalogEntity) {
    boolean allowOverlappingCatalogUrls =
        getCurrentPolarisContext()
            .getConfigurationStore()
            .getConfiguration(
                callContext.getRealmContext(), FeatureConfiguration.ALLOW_OVERLAPPING_CATALOG_URLS);

    if (allowOverlappingCatalogUrls) {
      return false;
    }

    Set<String> newCatalogLocations = getCatalogLocations(catalogEntity);
    return listCatalogsUnsafe().stream()
        .map(CatalogEntity::new)
        .anyMatch(
            existingCatalog -> {
              if (existingCatalog.getName().equals(catalogEntity.getName())) {
                return false;
              }
              return getCatalogLocations(existingCatalog).stream()
                  .map(StorageLocation::of)
                  .anyMatch(
                      existingLocation -> {
                        return newCatalogLocations.stream()
                            .anyMatch(
                                newLocationString -> {
                                  StorageLocation newLocation =
                                      StorageLocation.of(newLocationString);
                                  return newLocation.isChildOf(existingLocation)
                                      || existingLocation.isChildOf(newLocation);
                                });
                      });
            });
  }

  /**
   * Secrets embedded *or* simply referenced through the API model will require separate processing
   * for normalizing into resolved/verified/offloaded UserSecretReference objects which are then
   * placed appropriately into persistence objects.
   *
   * <p>If secrets are already direct URIs/URNs to an external secret store, we may need to validate
   * the URI/URN and/or transform into a polaris-internal URN format along with type-information or
   * other secrets-manager metadata in the referencePayload.
   *
   * <p>If secrets reference first-class Polaris-stored secrets, we must resolve the associated
   * polaris persistence entities defining access to those secrets and perform authorization.
   *
   * <p>If secrets are provided inline as part of the request, we must explicitly offload the
   * secrets into a Polaris service-level secrets manager and return the associated internal
   * references to the stored secret.
   */
  private Map<String, UserSecretReference> extractSecretReferences(
      CreateCatalogRequest catalogRequest, PolarisEntity forEntity) {
    Map<String, UserSecretReference> secretReferences = new HashMap<>();
    Catalog catalog = catalogRequest.getCatalog();
    UserSecretsManager secretsManager = getUserSecretsManager();
    if (catalog instanceof ExternalCatalog externalCatalog) {
      if (externalCatalog.getConnectionConfigInfo() != null) {
        ConnectionConfigInfo connectionConfig = externalCatalog.getConnectionConfigInfo();
        AuthenticationParameters authenticationParameters =
            connectionConfig.getAuthenticationParameters();

        switch (authenticationParameters.getAuthenticationType()) {
          case OAUTH:
            {
              OAuthClientCredentialsParameters oauthClientCredentialsModel =
                  (OAuthClientCredentialsParameters) authenticationParameters;
              String inlineClientSecret = oauthClientCredentialsModel.getClientSecret();
              UserSecretReference secretReference =
                  secretsManager.writeSecret(inlineClientSecret, forEntity);
              secretReferences.put(
                  AuthenticationParametersDpo.INLINE_CLIENT_SECRET_REFERENCE_KEY, secretReference);
              break;
            }
          case BEARER:
            {
              BearerAuthenticationParameters bearerAuthenticationParametersModel =
                  (BearerAuthenticationParameters) authenticationParameters;
              String inlineBearerToken = bearerAuthenticationParametersModel.getBearerToken();
              UserSecretReference secretReference =
                  secretsManager.writeSecret(inlineBearerToken, forEntity);
              secretReferences.put(
                  AuthenticationParametersDpo.INLINE_BEARER_TOKEN_REFERENCE_KEY, secretReference);
              break;
            }
          default:
            throw new IllegalStateException(
                "Unsupported authentication type: "
                    + authenticationParameters.getAuthenticationType());
        }
      }
    }
    return secretReferences;
  }

  /**
   * @see #extractSecretReferences
   */
  private boolean requiresSecretReferenceExtraction(
      @NotNull ConnectionConfigInfo connectionConfigInfo) {
    return connectionConfigInfo.getAuthenticationParameters().getAuthenticationType()
        != AuthenticationParameters.AuthenticationTypeEnum.IMPLICIT;
  }

  public PolarisEntity createCatalog(CreateCatalogRequest catalogRequest) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_CATALOG;
    authorizeBasicRootOperationOrThrow(op);

    CatalogEntity entity = CatalogEntity.fromCatalog(callContext, catalogRequest.getCatalog());

    checkArgument(entity.getId() == -1, "Entity to be created must have no ID assigned");

    if (catalogOverlapsWithExistingCatalog((CatalogEntity) entity)) {
      throw new ValidationException(
          "Cannot create Catalog %s. One or more of its locations overlaps with an existing catalog",
          entity.getName());
    }

    // After basic validations, now populate id and creation timestamp.
    entity =
        new CatalogEntity.Builder(entity)
            .setId(metaStoreManager.generateNewEntityId(getCurrentPolarisContext()).getId())
            .setCreateTimestamp(System.currentTimeMillis())
            .setProperties(reservedProperties.removeReservedProperties(entity.getPropertiesAsMap()))
            .build();

    Catalog catalog = catalogRequest.getCatalog();
    if (catalog instanceof ExternalCatalog externalCatalog) {
      ConnectionConfigInfo connectionConfigInfo = externalCatalog.getConnectionConfigInfo();

      if (connectionConfigInfo != null) {
        LOGGER
            .atDebug()
            .addKeyValue("catalogName", entity.getName())
            .log("Creating a federated catalog");
        FeatureConfiguration.enforceFeatureEnabledOrThrow(
            callContext, FeatureConfiguration.ENABLE_CATALOG_FEDERATION);
        Map<String, UserSecretReference> processedSecretReferences = Map.of();
        List<String> supportedAuthenticationTypes =
            callContext
                .getPolarisCallContext()
                .getConfigurationStore()
                .getConfiguration(
                    callContext.getRealmContext(),
                    FeatureConfiguration.SUPPORTED_EXTERNAL_CATALOG_AUTHENTICATION_TYPES)
                .stream()
                .map(s -> s.toUpperCase(Locale.ROOT))
                .toList();
        if (requiresSecretReferenceExtraction(connectionConfigInfo)) {
          // For fields that contain references to secrets, we'll separately process the secrets
          // from the original request first, and then populate those fields with the extracted
          // secret references as part of the construction of the internal persistence entity.
          checkState(
              supportedAuthenticationTypes.contains(
                  connectionConfigInfo
                      .getAuthenticationParameters()
                      .getAuthenticationType()
                      .name()),
              "Authentication type %s is not supported.",
              connectionConfigInfo.getAuthenticationParameters().getAuthenticationType());
          processedSecretReferences = extractSecretReferences(catalogRequest, entity);
        } else {
          // Support no-auth catalog federation only when the feature is enabled.
          checkState(
              supportedAuthenticationTypes.contains(
                  AuthenticationParameters.AuthenticationTypeEnum.IMPLICIT.name()),
              "Implicit authentication based catalog federation is not supported.");
        }
        entity =
            new CatalogEntity.Builder(entity)
                .setConnectionConfigInfoDpoWithSecrets(
                    connectionConfigInfo, processedSecretReferences)
                .build();
      }
    }

    CreateCatalogResult catalogResult =
        metaStoreManager.createCatalog(getCurrentPolarisContext(), entity, List.of());
    if (catalogResult.alreadyExists()) {
      // TODO: Proactive garbage-collection of any inline secrets that were written to the
      // secrets manager, here and on any other unexpected exception as well.
      throw new AlreadyExistsException(
          "Cannot create Catalog %s. Catalog already exists or resolution failed",
          entity.getName());
    }
    return PolarisEntity.of(catalogResult.getCatalog());
  }

  public void deleteCatalog(String name) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DELETE_CATALOG;
    authorizeBasicTopLevelEntityOperationOrThrow(op, name, PolarisEntityType.CATALOG);

    PolarisEntity entity =
        findCatalogByName(name)
            .orElseThrow(() -> new NotFoundException("Catalog %s not found", name));
    // TODO: Handle return value in case of concurrent modification
    PolarisCallContext polarisCallContext = callContext.getPolarisCallContext();
    boolean cleanup =
        polarisCallContext
            .getConfigurationStore()
            .getConfiguration(
                callContext.getRealmContext(), FeatureConfiguration.CLEANUP_ON_CATALOG_DROP);
    DropEntityResult dropEntityResult =
        metaStoreManager.dropEntityIfExists(
            getCurrentPolarisContext(), null, entity, Map.of(), cleanup);

    // at least some handling of error
    if (!dropEntityResult.isSuccess()) {
      if (dropEntityResult.failedBecauseNotEmpty()) {
        throw new BadRequestException(
            "Catalog '%s' cannot be dropped, it is not empty", entity.getName());
      } else {
        throw new BadRequestException(
            "Catalog '%s' cannot be dropped, concurrent modification detected. Please try "
                + "again",
            entity.getName());
      }
    }
  }

  public @Nonnull CatalogEntity getCatalog(String name) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.GET_CATALOG;
    authorizeBasicTopLevelEntityOperationOrThrow(op, name, PolarisEntityType.CATALOG);

    return findCatalogByName(name)
        .orElseThrow(() -> new NotFoundException("Catalog %s not found", name));
  }

  /**
   * Helper to validate business logic of what is allowed to be updated or throw a
   * BadRequestException.
   */
  private void validateUpdateCatalogDiffOrThrow(
      CatalogEntity currentEntity, CatalogEntity newEntity) {
    // TODO: Expand the set of validations if there are other fields for other cloud providers
    // that we can't successfully apply changes to.
    PolarisStorageConfigurationInfo currentStorageConfig =
        currentEntity.getStorageConfigurationInfo();
    PolarisStorageConfigurationInfo newStorageConfig = newEntity.getStorageConfigurationInfo();

    if (currentStorageConfig == null || newStorageConfig == null) {
      return;
    }

    if (!currentStorageConfig.getClass().equals(newStorageConfig.getClass())) {
      throw new BadRequestException(
          "Cannot modify storage type of storage config from %s to %s",
          currentStorageConfig, newStorageConfig);
    }

    if (currentStorageConfig instanceof AwsStorageConfigurationInfo currentAwsConfig
        && newStorageConfig instanceof AwsStorageConfigurationInfo newAwsConfig) {

      if (!currentAwsConfig.getAwsAccountId().equals(newAwsConfig.getAwsAccountId())) {
        throw new BadRequestException(
            "Cannot modify Role ARN in storage config from %s to %s",
            currentStorageConfig, newStorageConfig);
      }

      if ((currentAwsConfig.getExternalId() != null
              && !currentAwsConfig.getExternalId().equals(newAwsConfig.getExternalId()))
          || (newAwsConfig.getExternalId() != null
              && !newAwsConfig.getExternalId().equals(currentAwsConfig.getExternalId()))) {
        throw new BadRequestException(
            "Cannot modify ExternalId in storage config from %s to %s",
            currentStorageConfig, newStorageConfig);
      }
    } else if (currentStorageConfig instanceof AzureStorageConfigurationInfo currentAzureConfig
        && newStorageConfig instanceof AzureStorageConfigurationInfo newAzureConfig) {

      if (!currentAzureConfig.getTenantId().equals(newAzureConfig.getTenantId())
          || !newAzureConfig.getTenantId().equals(currentAzureConfig.getTenantId())) {
        throw new BadRequestException(
            "Cannot modify TenantId in storage config from %s to %s",
            currentStorageConfig, newStorageConfig);
      }
    }
  }

  public @Nonnull CatalogEntity updateCatalog(String name, UpdateCatalogRequest updateRequest) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.UPDATE_CATALOG;
    authorizeBasicTopLevelEntityOperationOrThrow(op, name, PolarisEntityType.CATALOG);

    CatalogEntity currentCatalogEntity =
        findCatalogByName(name)
            .orElseThrow(() -> new NotFoundException("Catalog %s not found", name));

    if (currentCatalogEntity.getEntityVersion() != updateRequest.getCurrentEntityVersion()) {
      throw new CommitFailedException(
          "Failed to update Catalog; currentEntityVersion '%s', expected '%s'",
          currentCatalogEntity.getEntityVersion(), updateRequest.getCurrentEntityVersion());
    }

    CatalogEntity.Builder updateBuilder = new CatalogEntity.Builder(currentCatalogEntity);
    String defaultBaseLocation = currentCatalogEntity.getBaseLocation();
    if (updateRequest.getProperties() != null) {
      Map<String, String> updateProperties =
          reservedProperties.removeReservedPropertiesFromUpdate(
              currentCatalogEntity.getPropertiesAsMap(), updateRequest.getProperties());
      updateBuilder.setProperties(updateProperties);
      String newDefaultBaseLocation =
          updateRequest.getProperties().get(CatalogEntity.DEFAULT_BASE_LOCATION_KEY);
      // Since defaultBaseLocation is a required field during construction of a catalog, and the
      // syntax of the Catalog API model splits default-base-location out from other keys in
      // additionalProperties, it's easy for client libraries to focus on adding/merging
      // additionalProperties while neglecting to "echo" the default-base-location from the
      // fetched catalog, it's most user-friendly to treat a null or empty default-base-location
      // as meaning no intended change to the default-base-location.
      if (StringUtils.isNotEmpty(newDefaultBaseLocation)) {
        // New base location is already in the updated properties; we'll also potentially
        // plumb it into the logic for setting an updated StorageConfigurationInfo.
        defaultBaseLocation = newDefaultBaseLocation;
      } else {
        // No default-base-location present at all in the properties of the update request,
        // so we must restore it explicitly in the updateBuilder.
        updateBuilder.setDefaultBaseLocation(defaultBaseLocation);
      }
    }
    if (updateRequest.getStorageConfigInfo() != null) {
      updateBuilder.setStorageConfigurationInfo(
          callContext, updateRequest.getStorageConfigInfo(), defaultBaseLocation);
    }
    CatalogEntity updatedEntity = updateBuilder.build();

    validateUpdateCatalogDiffOrThrow(currentCatalogEntity, updatedEntity);

    if (catalogOverlapsWithExistingCatalog(updatedEntity)) {
      throw new ValidationException(
          "Cannot update Catalog %s. One or more of its new locations overlaps with an existing catalog",
          updatedEntity.getName());
    }

    CatalogEntity returnedEntity =
        Optional.ofNullable(
                CatalogEntity.of(
                    PolarisEntity.of(
                        metaStoreManager.updateEntityPropertiesIfNotChanged(
                            getCurrentPolarisContext(), null, updatedEntity))))
            .orElseThrow(
                () ->
                    new CommitFailedException(
                        "Concurrent modification on Catalog '%s'; retry later", name));
    return returnedEntity;
  }

  /**
   * List all catalogs after checking for permission. Nulls due to non-atomic list-then-get are
   * filtered out.
   */
  public List<PolarisEntity> listCatalogs() {
    authorizeBasicRootOperationOrThrow(PolarisAuthorizableOperation.LIST_CATALOGS);
    return listCatalogsUnsafe();
  }

  /**
   * List all catalogs without checking for permission. Nulls due to non-atomic list-then-get are
   * filtered out.
   */
  private List<PolarisEntity> listCatalogsUnsafe() {
    // loadEntity may return null due to multiple non-atomic
    // API calls to the persistence layer. Specifically, this can happen when a PolarisEntity is
    // returned by listCatalogs, but cannot be loaded afterward because it was purged by another
    // process before it could be loaded.
    return metaStoreManager
        .listEntities(
            getCurrentPolarisContext(),
            null,
            PolarisEntityType.CATALOG,
            PolarisEntitySubType.ANY_SUBTYPE,
            PageToken.readEverything())
        .getEntities()
        .stream()
        .map(
            nameAndId ->
                PolarisEntity.of(
                    metaStoreManager.loadEntity(
                        getCurrentPolarisContext(), 0, nameAndId.getId(), nameAndId.getType())))
        .filter(Objects::nonNull)
        .toList();
  }

  public PrincipalWithCredentials createPrincipal(PolarisEntity entity) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_PRINCIPAL;
    authorizeBasicRootOperationOrThrow(op);

    // the API should prevent this from happening
    if (FederatedEntities.isFederated(entity)) {
      throw new ValidationException("Cannot create a federated principal");
    }
    checkArgument(entity.getId() == -1, "Entity to be created must have no ID assigned");

    CreatePrincipalResult principalResult =
        metaStoreManager.createPrincipal(
            getCurrentPolarisContext(),
            new PolarisEntity.Builder(entity)
                .setId(metaStoreManager.generateNewEntityId(getCurrentPolarisContext()).getId())
                .setCreateTimestamp(System.currentTimeMillis())
                .build());
    if (principalResult.alreadyExists()) {
      throw new AlreadyExistsException(
          "Cannot create Principal %s. Principal already exists or resolution failed",
          entity.getName());
    }
    return new PrincipalWithCredentials(
        new PrincipalEntity(principalResult.getPrincipal()).asPrincipal(),
        new PrincipalWithCredentialsCredentials(
            principalResult.getPrincipalSecrets().getPrincipalClientId(),
            principalResult.getPrincipalSecrets().getMainSecret()));
  }

  public void deletePrincipal(String name) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DELETE_PRINCIPAL;
    authorizeBasicTopLevelEntityOperationOrThrow(op, name, PolarisEntityType.PRINCIPAL);

    PolarisEntity entity =
        findPrincipalByName(name)
            .orElseThrow(() -> new NotFoundException("Principal %s not found", name));
    // TODO: Handle return value in case of concurrent modification
    DropEntityResult dropEntityResult =
        metaStoreManager.dropEntityIfExists(
            getCurrentPolarisContext(), null, entity, Map.of(), false);

    // at least some handling of error
    if (!dropEntityResult.isSuccess()) {
      if (dropEntityResult.isEntityUnDroppable()) {
        throw new BadRequestException("Root principal cannot be dropped");
      } else {
        throw new BadRequestException(
            "Root principal cannot be dropped, concurrent modification "
                + "detected. Please try again");
      }
    }
  }

  public @Nonnull PrincipalEntity getPrincipal(String name) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.GET_PRINCIPAL;
    authorizeBasicTopLevelEntityOperationOrThrow(op, name, PolarisEntityType.PRINCIPAL);

    return findPrincipalByName(name)
        .orElseThrow(() -> new NotFoundException("Principal %s not found", name));
  }

  public @Nonnull PrincipalEntity updatePrincipal(
      String name, UpdatePrincipalRequest updateRequest) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.UPDATE_PRINCIPAL;
    authorizeBasicTopLevelEntityOperationOrThrow(op, name, PolarisEntityType.PRINCIPAL);

    PrincipalEntity currentPrincipalEntity =
        findPrincipalByName(name)
            .orElseThrow(() -> new NotFoundException("Principal %s not found", name));

    if (FederatedEntities.isFederated(currentPrincipalEntity)) {
      throw new ValidationException(
          "Cannot update a federated principal: %s", currentPrincipalEntity.getName());
    }
    if (currentPrincipalEntity.getEntityVersion() != updateRequest.getCurrentEntityVersion()) {
      throw new CommitFailedException(
          "Failed to update Principal; currentEntityVersion '%s', expected '%s'",
          currentPrincipalEntity.getEntityVersion(), updateRequest.getCurrentEntityVersion());
    }

    PrincipalEntity.Builder updateBuilder = new PrincipalEntity.Builder(currentPrincipalEntity);
    if (updateRequest.getProperties() != null) {
      Map<String, String> updateProperties =
          reservedProperties.removeReservedPropertiesFromUpdate(
              currentPrincipalEntity.getPropertiesAsMap(), updateRequest.getProperties());
      updateBuilder.setProperties(updateProperties);
    }
    PrincipalEntity updatedEntity = updateBuilder.build();
    PrincipalEntity returnedEntity =
        Optional.ofNullable(
                PrincipalEntity.of(
                    PolarisEntity.of(
                        metaStoreManager.updateEntityPropertiesIfNotChanged(
                            getCurrentPolarisContext(), null, updatedEntity))))
            .orElseThrow(
                () ->
                    new CommitFailedException(
                        "Concurrent modification on Principal '%s'; retry later", name));
    return returnedEntity;
  }

  private @Nonnull PrincipalWithCredentials rotateOrResetCredentialsHelper(
      String principalName, boolean shouldReset) {
    PrincipalEntity currentPrincipalEntity =
        findPrincipalByName(principalName)
            .orElseThrow(() -> new NotFoundException("Principal %s not found", principalName));

    if (FederatedEntities.isFederated(currentPrincipalEntity)) {
      throw new ValidationException(
          "Cannot rotate/reset credentials for a federated principal: %s", principalName);
    }
    PolarisPrincipalSecrets currentSecrets =
        metaStoreManager
            .loadPrincipalSecrets(getCurrentPolarisContext(), currentPrincipalEntity.getClientId())
            .getPrincipalSecrets();
    if (currentSecrets == null) {
      throw new IllegalArgumentException(
          String.format("Failed to load current secrets for principal '%s'", principalName));
    }
    PolarisPrincipalSecrets newSecrets =
        metaStoreManager
            .rotatePrincipalSecrets(
                getCurrentPolarisContext(),
                currentPrincipalEntity.getClientId(),
                currentPrincipalEntity.getId(),
                shouldReset,
                currentSecrets.getMainSecretHash())
            .getPrincipalSecrets();
    if (newSecrets == null) {
      throw new IllegalStateException(
          String.format(
              "Failed to %s secrets for principal '%s'",
              shouldReset ? "reset" : "rotate", principalName));
    }
    PolarisEntity newPrincipal =
        PolarisEntity.of(
            metaStoreManager.loadEntity(
                getCurrentPolarisContext(),
                0L,
                currentPrincipalEntity.getId(),
                currentPrincipalEntity.getType()));
    return new PrincipalWithCredentials(
        PrincipalEntity.of(newPrincipal).asPrincipal(),
        new PrincipalWithCredentialsCredentials(
            newSecrets.getPrincipalClientId(), newSecrets.getMainSecret()));
  }

  public @Nonnull PrincipalWithCredentials rotateCredentials(String principalName) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.ROTATE_CREDENTIALS;
    authorizeBasicTopLevelEntityOperationOrThrow(op, principalName, PolarisEntityType.PRINCIPAL);

    return rotateOrResetCredentialsHelper(principalName, false);
  }

  public @Nonnull PrincipalWithCredentials resetCredentials(String principalName) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.RESET_CREDENTIALS;
    authorizeBasicTopLevelEntityOperationOrThrow(op, principalName, PolarisEntityType.PRINCIPAL);

    return rotateOrResetCredentialsHelper(principalName, true);
  }

  public List<PolarisEntity> listPrincipals() {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_PRINCIPALS;
    authorizeBasicRootOperationOrThrow(op);

    return metaStoreManager
        .listEntities(
            getCurrentPolarisContext(),
            null,
            PolarisEntityType.PRINCIPAL,
            PolarisEntitySubType.NULL_SUBTYPE,
            PageToken.readEverything())
        .getEntities()
        .stream()
        .map(
            nameAndId ->
                PolarisEntity.of(
                    metaStoreManager.loadEntity(
                        getCurrentPolarisContext(), 0, nameAndId.getId(), nameAndId.getType())))
        .toList();
  }

  public PolarisEntity createPrincipalRole(PolarisEntity entity) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_PRINCIPAL_ROLE;
    authorizeBasicRootOperationOrThrow(op);

    checkArgument(entity.getId() == -1, "Entity to be created must have no ID assigned");

    PolarisEntity returnedEntity =
        PolarisEntity.of(
            metaStoreManager.createEntityIfNotExists(
                getCurrentPolarisContext(),
                null,
                new PolarisEntity.Builder(entity)
                    .setId(metaStoreManager.generateNewEntityId(getCurrentPolarisContext()).getId())
                    .setCreateTimestamp(System.currentTimeMillis())
                    .build()));
    if (returnedEntity == null) {
      throw new AlreadyExistsException(
          "Cannot create PrincipalRole %s. PrincipalRole already exists or resolution failed",
          entity.getName());
    }
    return returnedEntity;
  }

  public void deletePrincipalRole(String name) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DELETE_PRINCIPAL_ROLE;
    authorizeBasicTopLevelEntityOperationOrThrow(op, name, PolarisEntityType.PRINCIPAL_ROLE);

    PolarisEntity entity =
        findPrincipalRoleByName(name)
            .orElseThrow(() -> new NotFoundException("PrincipalRole %s not found", name));
    // TODO: Handle return value in case of concurrent modification
    DropEntityResult dropEntityResult =
        metaStoreManager.dropEntityIfExists(
            getCurrentPolarisContext(), null, entity, Map.of(), true); // cleanup grants

    // at least some handling of error
    if (!dropEntityResult.isSuccess()) {
      if (dropEntityResult.isEntityUnDroppable()) {
        throw new BadRequestException("Polaris service admin principal role cannot be dropped");
      } else {
        throw new BadRequestException(
            "Polaris service admin principal role cannot be dropped, "
                + "concurrent modification detected. Please try again");
      }
    }
  }

  public @Nonnull PrincipalRoleEntity getPrincipalRole(String name) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.GET_PRINCIPAL_ROLE;
    authorizeBasicTopLevelEntityOperationOrThrow(op, name, PolarisEntityType.PRINCIPAL_ROLE);

    return findPrincipalRoleByName(name)
        .orElseThrow(() -> new NotFoundException("PrincipalRole %s not found", name));
  }

  public @Nonnull PrincipalRoleEntity updatePrincipalRole(
      String name, UpdatePrincipalRoleRequest updateRequest) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.UPDATE_PRINCIPAL_ROLE;
    authorizeBasicTopLevelEntityOperationOrThrow(op, name, PolarisEntityType.PRINCIPAL_ROLE);

    PrincipalRoleEntity currentPrincipalRoleEntity =
        findPrincipalRoleByName(name)
            .orElseThrow(() -> new NotFoundException("PrincipalRole %s not found", name));

    if (currentPrincipalRoleEntity.getEntityVersion() != updateRequest.getCurrentEntityVersion()) {
      throw new CommitFailedException(
          "Failed to update PrincipalRole; currentEntityVersion '%s', expected '%s'",
          currentPrincipalRoleEntity.getEntityVersion(), updateRequest.getCurrentEntityVersion());
    }

    PrincipalRoleEntity.Builder updateBuilder =
        new PrincipalRoleEntity.Builder(currentPrincipalRoleEntity);
    if (updateRequest.getProperties() != null) {
      Map<String, String> updateProperties =
          reservedProperties.removeReservedPropertiesFromUpdate(
              currentPrincipalRoleEntity.getPropertiesAsMap(), updateRequest.getProperties());
      updateBuilder.setProperties(updateProperties);
    }
    PrincipalRoleEntity updatedEntity = updateBuilder.build();
    PrincipalRoleEntity returnedEntity =
        Optional.ofNullable(
                PrincipalRoleEntity.of(
                    PolarisEntity.of(
                        metaStoreManager.updateEntityPropertiesIfNotChanged(
                            getCurrentPolarisContext(), null, updatedEntity))))
            .orElseThrow(
                () ->
                    new CommitFailedException(
                        "Concurrent modification on PrincipalRole '%s'; retry later", name));
    return returnedEntity;
  }

  public List<PolarisEntity> listPrincipalRoles() {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_PRINCIPAL_ROLES;
    authorizeBasicRootOperationOrThrow(op);

    return metaStoreManager
        .listEntities(
            getCurrentPolarisContext(),
            null,
            PolarisEntityType.PRINCIPAL_ROLE,
            PolarisEntitySubType.NULL_SUBTYPE,
            PageToken.readEverything())
        .getEntities()
        .stream()
        .map(
            nameAndId ->
                PolarisEntity.of(
                    metaStoreManager.loadEntity(
                        getCurrentPolarisContext(), 0, nameAndId.getId(), nameAndId.getType())))
        .toList();
  }

  public PolarisEntity createCatalogRole(String catalogName, PolarisEntity entity) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_CATALOG_ROLE;
    authorizeBasicTopLevelEntityOperationOrThrow(op, catalogName, PolarisEntityType.CATALOG);

    checkArgument(entity.getId() == -1, "Entity to be created must have no ID assigned");

    PolarisEntity catalogEntity =
        findCatalogByName(catalogName)
            .orElseThrow(() -> new NotFoundException("Parent catalog %s not found", catalogName));

    PolarisEntity returnedEntity =
        PolarisEntity.of(
            metaStoreManager.createEntityIfNotExists(
                getCurrentPolarisContext(),
                PolarisEntity.toCoreList(List.of(catalogEntity)),
                new PolarisEntity.Builder(entity)
                    .setId(metaStoreManager.generateNewEntityId(getCurrentPolarisContext()).getId())
                    .setCatalogId(catalogEntity.getId())
                    .setParentId(catalogEntity.getId())
                    .setCreateTimestamp(System.currentTimeMillis())
                    .build()));
    if (returnedEntity == null) {
      throw new AlreadyExistsException(
          "Cannot create CatalogRole %s in %s. CatalogRole already exists or resolution failed",
          entity.getName(), catalogName);
    }
    return returnedEntity;
  }

  public void deleteCatalogRole(String catalogName, String name) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DELETE_CATALOG_ROLE;
    authorizeBasicCatalogRoleOperationOrThrow(op, catalogName, name);

    PolarisResolvedPathWrapper resolvedCatalogRoleEntity = resolutionManifest.getResolvedPath(name);
    if (resolvedCatalogRoleEntity == null) {
      throw new NotFoundException("CatalogRole %s not found in catalog %s", name, catalogName);
    }
    // TODO: Handle return value in case of concurrent modification
    DropEntityResult dropEntityResult =
        metaStoreManager.dropEntityIfExists(
            getCurrentPolarisContext(),
            PolarisEntity.toCoreList(resolvedCatalogRoleEntity.getRawParentPath()),
            resolvedCatalogRoleEntity.getRawLeafEntity(),
            Map.of(),
            true); // cleanup grants

    // at least some handling of error
    if (!dropEntityResult.isSuccess()) {
      if (dropEntityResult.isEntityUnDroppable()) {
        throw new BadRequestException("Catalog admin role cannot be dropped");
      } else {
        throw new BadRequestException(
            "Catalog admin role cannot be dropped, concurrent "
                + "modification detected. Please try again");
      }
    }
  }

  public @Nonnull CatalogRoleEntity getCatalogRole(String catalogName, String name) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.GET_CATALOG_ROLE;
    authorizeBasicCatalogRoleOperationOrThrow(op, catalogName, name);

    return findCatalogRoleByName(catalogName, name)
        .orElseThrow(() -> new NotFoundException("CatalogRole %s not found", name));
  }

  public @Nonnull CatalogRoleEntity updateCatalogRole(
      String catalogName, String name, UpdateCatalogRoleRequest updateRequest) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.UPDATE_CATALOG_ROLE;
    authorizeBasicCatalogRoleOperationOrThrow(op, catalogName, name);

    CatalogEntity catalogEntity =
        findCatalogByName(catalogName)
            .orElseThrow(() -> new NotFoundException("Catalog %s not found", catalogName));
    CatalogRoleEntity currentCatalogRoleEntity =
        findCatalogRoleByName(catalogName, name)
            .orElseThrow(() -> new NotFoundException("CatalogRole %s not found", name));

    if (currentCatalogRoleEntity.getEntityVersion() != updateRequest.getCurrentEntityVersion()) {
      throw new CommitFailedException(
          "Failed to update CatalogRole; currentEntityVersion '%s', expected '%s'",
          currentCatalogRoleEntity.getEntityVersion(), updateRequest.getCurrentEntityVersion());
    }

    CatalogRoleEntity.Builder updateBuilder =
        new CatalogRoleEntity.Builder(currentCatalogRoleEntity);
    if (updateRequest.getProperties() != null) {
      Map<String, String> updateProperties =
          reservedProperties.removeReservedPropertiesFromUpdate(
              currentCatalogRoleEntity.getPropertiesAsMap(), updateRequest.getProperties());
      updateBuilder.setProperties(updateProperties);
    }
    CatalogRoleEntity updatedEntity = updateBuilder.build();
    CatalogRoleEntity returnedEntity =
        Optional.ofNullable(
                CatalogRoleEntity.of(
                    PolarisEntity.of(
                        metaStoreManager.updateEntityPropertiesIfNotChanged(
                            getCurrentPolarisContext(),
                            PolarisEntity.toCoreList(List.of(catalogEntity)),
                            updatedEntity))))
            .orElseThrow(
                () ->
                    new CommitFailedException(
                        "Concurrent modification on CatalogRole '%s'; retry later", name));
    return returnedEntity;
  }

  public List<PolarisEntity> listCatalogRoles(String catalogName) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_CATALOG_ROLES;
    authorizeBasicTopLevelEntityOperationOrThrow(op, catalogName, PolarisEntityType.CATALOG);

    PolarisEntity catalogEntity =
        findCatalogByName(catalogName)
            .orElseThrow(() -> new NotFoundException("Parent catalog %s not found", catalogName));
    return metaStoreManager
        .listEntities(
            getCurrentPolarisContext(),
            PolarisEntity.toCoreList(List.of(catalogEntity)),
            PolarisEntityType.CATALOG_ROLE,
            PolarisEntitySubType.NULL_SUBTYPE,
            PageToken.readEverything())
        .getEntities()
        .stream()
        .map(
            nameAndId ->
                PolarisEntity.of(
                    metaStoreManager.loadEntity(
                        getCurrentPolarisContext(),
                        catalogEntity.getId(),
                        nameAndId.getId(),
                        nameAndId.getType())))
        .toList();
  }

  public boolean assignPrincipalRole(String principalName, String principalRoleName) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.ASSIGN_PRINCIPAL_ROLE;
    authorizeGrantOnPrincipalRoleToPrincipalOperationOrThrow(op, principalRoleName, principalName);

    PolarisEntity principalEntity =
        findPrincipalByName(principalName)
            .orElseThrow(() -> new NotFoundException("Principal %s not found", principalName));
    if (FederatedEntities.isFederated(principalEntity)) {
      throw new ValidationException("Cannot assign a role to a federated principal");
    }
    PolarisEntity principalRoleEntity =
        findPrincipalRoleByName(principalRoleName)
            .orElseThrow(
                () -> new NotFoundException("PrincipalRole %s not found", principalRoleName));
    if (FederatedEntities.isFederated(principalRoleEntity)) {
      throw new ValidationException("Cannot assign a federated role to a principal");
    }
    return metaStoreManager
        .grantUsageOnRoleToGrantee(
            getCurrentPolarisContext(), null, principalRoleEntity, principalEntity)
        .isSuccess();
  }

  public boolean revokePrincipalRole(String principalName, String principalRoleName) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.REVOKE_PRINCIPAL_ROLE;
    authorizeGrantOnPrincipalRoleToPrincipalOperationOrThrow(op, principalRoleName, principalName);

    PolarisEntity principalEntity =
        findPrincipalByName(principalName)
            .orElseThrow(() -> new NotFoundException("Principal %s not found", principalName));
    if (FederatedEntities.isFederated(principalEntity)) {
      throw new ValidationException("Cannot revoke a role from a federated principal");
    }
    PolarisEntity principalRoleEntity =
        findPrincipalRoleByName(principalRoleName)
            .orElseThrow(
                () -> new NotFoundException("PrincipalRole %s not found", principalRoleName));
    if (FederatedEntities.isFederated(principalRoleEntity)) {
      throw new ValidationException("Cannot revoke a federated role from a principal");
    }
    return metaStoreManager
        .revokeUsageOnRoleFromGrantee(
            getCurrentPolarisContext(), null, principalRoleEntity, principalEntity)
        .isSuccess();
  }

  public List<PolarisEntity> listPrincipalRolesAssigned(String principalName) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_PRINCIPAL_ROLES_ASSIGNED;

    authorizeBasicTopLevelEntityOperationOrThrow(op, principalName, PolarisEntityType.PRINCIPAL);

    PolarisEntity principalEntity =
        findPrincipalByName(principalName)
            .orElseThrow(() -> new NotFoundException("Principal %s not found", principalName));
    LoadGrantsResult grantList =
        metaStoreManager.loadGrantsToGrantee(getCurrentPolarisContext(), principalEntity);
    return buildEntitiesFromGrantResults(grantList, false, PolarisEntityType.PRINCIPAL_ROLE, null);
  }

  public boolean assignCatalogRoleToPrincipalRole(
      String principalRoleName, String catalogName, String catalogRoleName) {
    PolarisAuthorizableOperation op =
        PolarisAuthorizableOperation.ASSIGN_CATALOG_ROLE_TO_PRINCIPAL_ROLE;
    authorizeGrantOnCatalogRoleToPrincipalRoleOperationOrThrow(
        op, catalogName, catalogRoleName, principalRoleName);

    PolarisEntity principalRoleEntity =
        findPrincipalRoleByName(principalRoleName)
            .orElseThrow(
                () -> new NotFoundException("PrincipalRole %s not found", principalRoleName));
    PolarisEntity catalogEntity =
        findCatalogByName(catalogName)
            .orElseThrow(() -> new NotFoundException("Parent catalog %s not found", catalogName));
    PolarisEntity catalogRoleEntity =
        findCatalogRoleByName(catalogName, catalogRoleName)
            .orElseThrow(() -> new NotFoundException("CatalogRole %s not found", catalogRoleName));

    return metaStoreManager
        .grantUsageOnRoleToGrantee(
            getCurrentPolarisContext(), catalogEntity, catalogRoleEntity, principalRoleEntity)
        .isSuccess();
  }

  public boolean revokeCatalogRoleFromPrincipalRole(
      String principalRoleName, String catalogName, String catalogRoleName) {
    PolarisAuthorizableOperation op =
        PolarisAuthorizableOperation.REVOKE_CATALOG_ROLE_FROM_PRINCIPAL_ROLE;
    authorizeGrantOnCatalogRoleToPrincipalRoleOperationOrThrow(
        op, catalogName, catalogRoleName, principalRoleName);

    PolarisEntity principalRoleEntity =
        findPrincipalRoleByName(principalRoleName)
            .orElseThrow(
                () -> new NotFoundException("PrincipalRole %s not found", principalRoleName));
    PolarisEntity catalogEntity =
        findCatalogByName(catalogName)
            .orElseThrow(() -> new NotFoundException("Parent catalog %s not found", catalogName));
    PolarisEntity catalogRoleEntity =
        findCatalogRoleByName(catalogName, catalogRoleName)
            .orElseThrow(() -> new NotFoundException("CatalogRole %s not found", catalogRoleName));
    return metaStoreManager
        .revokeUsageOnRoleFromGrantee(
            getCurrentPolarisContext(), catalogEntity, catalogRoleEntity, principalRoleEntity)
        .isSuccess();
  }

  public List<PolarisEntity> listAssigneePrincipalsForPrincipalRole(String principalRoleName) {
    PolarisAuthorizableOperation op =
        PolarisAuthorizableOperation.LIST_ASSIGNEE_PRINCIPALS_FOR_PRINCIPAL_ROLE;

    authorizeBasicTopLevelEntityOperationOrThrow(
        op, principalRoleName, PolarisEntityType.PRINCIPAL_ROLE);

    PolarisEntity principalRoleEntity =
        findPrincipalRoleByName(principalRoleName)
            .orElseThrow(
                () -> new NotFoundException("PrincipalRole %s not found", principalRoleName));
    LoadGrantsResult grantList =
        metaStoreManager.loadGrantsOnSecurable(getCurrentPolarisContext(), principalRoleEntity);
    return buildEntitiesFromGrantResults(grantList, true, PolarisEntityType.PRINCIPAL, null);
  }

  /**
   * Build the list of entities matching the set of grant records returned by a grant lookup
   * request.
   *
   * @param grantList result of a load grants on a securable or to a grantee
   * @param grantees if true, return the list of grantee entities, else the list of securable
   *     entities
   * @param grantFilter filter on the grant records, use null for all
   * @return list of grantees or securables matching the filter
   */
  private List<PolarisEntity> buildEntitiesFromGrantResults(
      @Nonnull LoadGrantsResult grantList,
      boolean grantees,
      PolarisEntityType entityType,
      @Nullable Function<PolarisGrantRecord, Boolean> grantFilter) {
    Map<Long, PolarisBaseEntity> granteeMap = grantList.getEntitiesAsMap();
    List<PolarisEntity> toReturn = new ArrayList<>(grantList.getGrantRecords().size());
    for (PolarisGrantRecord grantRecord : grantList.getGrantRecords()) {
      if (grantFilter == null || grantFilter.apply(grantRecord)) {
        long catalogId =
            grantees ? grantRecord.getGranteeCatalogId() : grantRecord.getSecurableCatalogId();
        long entityId = grantees ? grantRecord.getGranteeId() : grantRecord.getSecurableId();
        // get the entity associated with the grantee
        PolarisBaseEntity entity =
            this.getOrLoadEntity(granteeMap, catalogId, entityId, entityType);
        if (entity != null) {
          toReturn.add(PolarisEntity.of(entity));
        }
      }
    }
    return toReturn;
  }

  public List<PolarisEntity> listCatalogRolesForPrincipalRole(
      String principalRoleName, String catalogName) {
    PolarisAuthorizableOperation op =
        PolarisAuthorizableOperation.LIST_CATALOG_ROLES_FOR_PRINCIPAL_ROLE;
    authorizeBasicTopLevelEntityOperationOrThrow(
        op, principalRoleName, PolarisEntityType.PRINCIPAL_ROLE, catalogName);

    PolarisEntity catalogEntity =
        findCatalogByName(catalogName)
            .orElseThrow(() -> new NotFoundException("Parent catalog %s not found", catalogName));
    PolarisEntity principalRoleEntity =
        findPrincipalRoleByName(principalRoleName)
            .orElseThrow(
                () -> new NotFoundException("PrincipalRole %s not found", principalRoleName));
    LoadGrantsResult grantList =
        metaStoreManager.loadGrantsToGrantee(getCurrentPolarisContext(), principalRoleEntity);
    return buildEntitiesFromGrantResults(
        grantList,
        false,
        PolarisEntityType.CATALOG_ROLE,
        grantRec -> grantRec.getSecurableCatalogId() == catalogEntity.getId());
  }

  /** Adds a grant on the root container of this realm to {@code principalRoleName}. */
  public boolean grantPrivilegeOnRootContainerToPrincipalRole(
      String principalRoleName, PolarisPrivilege privilege) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.ADD_ROOT_GRANT_TO_PRINCIPAL_ROLE;
    authorizeGrantOnRootContainerToPrincipalRoleOperationOrThrow(op, principalRoleName);

    PolarisEntity rootContainerEntity =
        resolutionManifest.getResolvedRootContainerEntityAsPath().getRawLeafEntity();
    PolarisEntity principalRoleEntity =
        findPrincipalRoleByName(principalRoleName)
            .orElseThrow(
                () -> new NotFoundException("PrincipalRole %s not found", principalRoleName));

    return metaStoreManager
        .grantPrivilegeOnSecurableToRole(
            getCurrentPolarisContext(), principalRoleEntity, null, rootContainerEntity, privilege)
        .isSuccess();
  }

  /** Revokes a grant on the root container of this realm from {@code principalRoleName}. */
  public boolean revokePrivilegeOnRootContainerFromPrincipalRole(
      String principalRoleName, PolarisPrivilege privilege) {
    PolarisAuthorizableOperation op =
        PolarisAuthorizableOperation.REVOKE_ROOT_GRANT_FROM_PRINCIPAL_ROLE;
    authorizeGrantOnRootContainerToPrincipalRoleOperationOrThrow(op, principalRoleName);

    PolarisEntity rootContainerEntity =
        resolutionManifest.getResolvedRootContainerEntityAsPath().getRawLeafEntity();
    PolarisEntity principalRoleEntity =
        findPrincipalRoleByName(principalRoleName)
            .orElseThrow(
                () -> new NotFoundException("PrincipalRole %s not found", principalRoleName));

    return metaStoreManager
        .revokePrivilegeOnSecurableFromRole(
            getCurrentPolarisContext(), principalRoleEntity, null, rootContainerEntity, privilege)
        .isSuccess();
  }

  /**
   * Adds a catalog-level grant on {@code catalogName} to {@code catalogRoleName} which resides
   * within the same catalog on which it is being granted the privilege.
   */
  public boolean grantPrivilegeOnCatalogToRole(
      String catalogName, String catalogRoleName, PolarisPrivilege privilege) {
    PolarisAuthorizableOperation op =
        PolarisAuthorizableOperation.ADD_CATALOG_GRANT_TO_CATALOG_ROLE;

    authorizeGrantOnCatalogOperationOrThrow(op, catalogName, catalogRoleName);

    PolarisEntity catalogEntity =
        findCatalogByName(catalogName)
            .orElseThrow(() -> new NotFoundException("Parent catalog %s not found", catalogName));
    PolarisEntity catalogRoleEntity =
        findCatalogRoleByName(catalogName, catalogRoleName)
            .orElseThrow(() -> new NotFoundException("CatalogRole %s not found", catalogRoleName));

    return metaStoreManager
        .grantPrivilegeOnSecurableToRole(
            getCurrentPolarisContext(),
            catalogRoleEntity,
            PolarisEntity.toCoreList(List.of(catalogEntity)),
            catalogEntity,
            privilege)
        .isSuccess();
  }

  /** Removes a catalog-level grant on {@code catalogName} from {@code catalogRoleName}. */
  public boolean revokePrivilegeOnCatalogFromRole(
      String catalogName, String catalogRoleName, PolarisPrivilege privilege) {
    PolarisAuthorizableOperation op =
        PolarisAuthorizableOperation.REVOKE_CATALOG_GRANT_FROM_CATALOG_ROLE;
    authorizeGrantOnCatalogOperationOrThrow(op, catalogName, catalogRoleName);

    PolarisEntity catalogEntity =
        findCatalogByName(catalogName)
            .orElseThrow(() -> new NotFoundException("Parent catalog %s not found", catalogName));
    PolarisEntity catalogRoleEntity =
        findCatalogRoleByName(catalogName, catalogRoleName)
            .orElseThrow(() -> new NotFoundException("CatalogRole %s not found", catalogRoleName));

    return metaStoreManager
        .revokePrivilegeOnSecurableFromRole(
            getCurrentPolarisContext(),
            catalogRoleEntity,
            PolarisEntity.toCoreList(List.of(catalogEntity)),
            catalogEntity,
            privilege)
        .isSuccess();
  }

  /** Adds a namespace-level grant on {@code namespace} to {@code catalogRoleName}. */
  public boolean grantPrivilegeOnNamespaceToRole(
      String catalogName, String catalogRoleName, Namespace namespace, PolarisPrivilege privilege) {
    PolarisAuthorizableOperation op =
        PolarisAuthorizableOperation.ADD_NAMESPACE_GRANT_TO_CATALOG_ROLE;
    authorizeGrantOnNamespaceOperationOrThrow(op, catalogName, namespace, catalogRoleName);

    PolarisEntity catalogRoleEntity =
        findCatalogRoleByName(catalogName, catalogRoleName)
            .orElseThrow(() -> new NotFoundException("CatalogRole %s not found", catalogRoleName));

    PolarisResolvedPathWrapper resolvedPathWrapper = resolutionManifest.getResolvedPath(namespace);
    if (resolvedPathWrapper == null) {
      throw new NotFoundException("Namespace %s not found", namespace);
    }
    List<PolarisEntity> catalogPath = resolvedPathWrapper.getRawParentPath();
    PolarisEntity namespaceEntity = resolvedPathWrapper.getRawLeafEntity();

    return metaStoreManager
        .grantPrivilegeOnSecurableToRole(
            getCurrentPolarisContext(),
            catalogRoleEntity,
            PolarisEntity.toCoreList(catalogPath),
            namespaceEntity,
            privilege)
        .isSuccess();
  }

  /** Removes a namespace-level grant on {@code namespace} from {@code catalogRoleName}. */
  public boolean revokePrivilegeOnNamespaceFromRole(
      String catalogName, String catalogRoleName, Namespace namespace, PolarisPrivilege privilege) {
    PolarisAuthorizableOperation op =
        PolarisAuthorizableOperation.REVOKE_NAMESPACE_GRANT_FROM_CATALOG_ROLE;
    authorizeGrantOnNamespaceOperationOrThrow(op, catalogName, namespace, catalogRoleName);

    PolarisEntity catalogRoleEntity =
        findCatalogRoleByName(catalogName, catalogRoleName)
            .orElseThrow(() -> new NotFoundException("CatalogRole %s not found", catalogRoleName));

    PolarisResolvedPathWrapper resolvedPathWrapper = resolutionManifest.getResolvedPath(namespace);
    if (resolvedPathWrapper == null) {
      throw new NotFoundException("Namespace %s not found", namespace);
    }
    List<PolarisEntity> catalogPath = resolvedPathWrapper.getRawParentPath();
    PolarisEntity namespaceEntity = resolvedPathWrapper.getRawLeafEntity();

    return metaStoreManager
        .revokePrivilegeOnSecurableFromRole(
            getCurrentPolarisContext(),
            catalogRoleEntity,
            PolarisEntity.toCoreList(catalogPath),
            namespaceEntity,
            privilege)
        .isSuccess();
  }

  public boolean grantPrivilegeOnTableToRole(
      String catalogName,
      String catalogRoleName,
      TableIdentifier identifier,
      PolarisPrivilege privilege) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.ADD_TABLE_GRANT_TO_CATALOG_ROLE;

    authorizeGrantOnTableLikeOperationOrThrow(
        op,
        catalogName,
        List.of(PolarisEntitySubType.GENERIC_TABLE, PolarisEntitySubType.ICEBERG_TABLE),
        identifier,
        catalogRoleName);

    return grantPrivilegeOnTableLikeToRole(
        catalogName,
        catalogRoleName,
        identifier,
        List.of(PolarisEntitySubType.GENERIC_TABLE, PolarisEntitySubType.ICEBERG_TABLE),
        privilege);
  }

  public boolean revokePrivilegeOnTableFromRole(
      String catalogName,
      String catalogRoleName,
      TableIdentifier identifier,
      PolarisPrivilege privilege) {
    PolarisAuthorizableOperation op =
        PolarisAuthorizableOperation.REVOKE_TABLE_GRANT_FROM_CATALOG_ROLE;

    authorizeGrantOnTableLikeOperationOrThrow(
        op,
        catalogName,
        List.of(PolarisEntitySubType.GENERIC_TABLE, PolarisEntitySubType.ICEBERG_TABLE),
        identifier,
        catalogRoleName);

    return revokePrivilegeOnTableLikeFromRole(
        catalogName,
        catalogRoleName,
        identifier,
        List.of(PolarisEntitySubType.GENERIC_TABLE, PolarisEntitySubType.ICEBERG_TABLE),
        privilege);
  }

  public boolean grantPrivilegeOnViewToRole(
      String catalogName,
      String catalogRoleName,
      TableIdentifier identifier,
      PolarisPrivilege privilege) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.ADD_VIEW_GRANT_TO_CATALOG_ROLE;

    authorizeGrantOnTableLikeOperationOrThrow(
        op, catalogName, List.of(PolarisEntitySubType.ICEBERG_VIEW), identifier, catalogRoleName);

    return grantPrivilegeOnTableLikeToRole(
        catalogName,
        catalogRoleName,
        identifier,
        List.of(PolarisEntitySubType.ICEBERG_VIEW),
        privilege);
  }

  public boolean revokePrivilegeOnViewFromRole(
      String catalogName,
      String catalogRoleName,
      TableIdentifier identifier,
      PolarisPrivilege privilege) {
    PolarisAuthorizableOperation op =
        PolarisAuthorizableOperation.REVOKE_VIEW_GRANT_FROM_CATALOG_ROLE;

    authorizeGrantOnTableLikeOperationOrThrow(
        op, catalogName, List.of(PolarisEntitySubType.ICEBERG_VIEW), identifier, catalogRoleName);

    return revokePrivilegeOnTableLikeFromRole(
        catalogName,
        catalogRoleName,
        identifier,
        List.of(PolarisEntitySubType.ICEBERG_VIEW),
        privilege);
  }

  public boolean grantPrivilegeOnPolicyToRole(
      String catalogName,
      String catalogRoleName,
      PolicyIdentifier identifier,
      PolarisPrivilege privilege) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.ADD_POLICY_GRANT_TO_CATALOG_ROLE;

    authorizeGrantOnPolicyOperationOrThrow(op, catalogName, identifier, catalogRoleName);

    return grantPrivilegeOnPolicyEntityToRole(catalogName, catalogRoleName, identifier, privilege);
  }

  public boolean revokePrivilegeOnPolicyFromRole(
      String catalogName,
      String catalogRoleName,
      PolicyIdentifier identifier,
      PolarisPrivilege privilege) {
    PolarisAuthorizableOperation op =
        PolarisAuthorizableOperation.REVOKE_POLICY_GRANT_FROM_CATALOG_ROLE;

    authorizeGrantOnPolicyOperationOrThrow(op, catalogName, identifier, catalogRoleName);

    return revokePrivilegeOnPolicyEntityFromRole(
        catalogName, catalogRoleName, identifier, privilege);
  }

  public List<PolarisEntity> listAssigneePrincipalRolesForCatalogRole(
      String catalogName, String catalogRoleName) {
    PolarisAuthorizableOperation op =
        PolarisAuthorizableOperation.LIST_ASSIGNEE_PRINCIPAL_ROLES_FOR_CATALOG_ROLE;
    authorizeBasicCatalogRoleOperationOrThrow(op, catalogName, catalogRoleName);

    if (findCatalogByName(catalogName).isEmpty()) {
      throw new NotFoundException("Parent catalog %s not found", catalogName);
    }
    PolarisEntity catalogRoleEntity =
        findCatalogRoleByName(catalogName, catalogRoleName)
            .orElseThrow(() -> new NotFoundException("CatalogRole %s not found", catalogRoleName));
    LoadGrantsResult grantList =
        metaStoreManager.loadGrantsOnSecurable(getCurrentPolarisContext(), catalogRoleEntity);
    return buildEntitiesFromGrantResults(grantList, true, PolarisEntityType.PRINCIPAL_ROLE, null);
  }

  /**
   * Lists all grants on Catalog-level resources (Catalog/Namespace/Table/View) granted to the
   * specified catalogRole.
   */
  public List<GrantResource> listGrantsForCatalogRole(String catalogName, String catalogRoleName) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_GRANTS_FOR_CATALOG_ROLE;
    authorizeBasicCatalogRoleOperationOrThrow(op, catalogName, catalogRoleName);

    PolarisEntity catalogRoleEntity =
        findCatalogRoleByName(catalogName, catalogRoleName)
            .orElseThrow(() -> new NotFoundException("CatalogRole %s not found", catalogRoleName));
    LoadGrantsResult grantList =
        metaStoreManager.loadGrantsToGrantee(getCurrentPolarisContext(), catalogRoleEntity);
    List<CatalogGrant> catalogGrants = new ArrayList<>();
    List<NamespaceGrant> namespaceGrants = new ArrayList<>();
    List<TableGrant> tableGrants = new ArrayList<>();
    List<ViewGrant> viewGrants = new ArrayList<>();
    List<PolicyGrant> policyGrants = new ArrayList<>();
    Map<Long, PolarisBaseEntity> entityMap = grantList.getEntitiesAsMap();
    for (PolarisGrantRecord record : grantList.getGrantRecords()) {
      PolarisPrivilege privilege = PolarisPrivilege.fromCode(record.getPrivilegeCode());
      PolarisBaseEntity baseEntity = this.getOrLoadEntityForGrant(entityMap, record);
      if (baseEntity != null) {
        switch (baseEntity.getType()) {
          case CATALOG:
            {
              CatalogGrant grant =
                  new CatalogGrant(
                      CatalogPrivilege.valueOf(privilege.toString()),
                      GrantResource.TypeEnum.CATALOG);
              catalogGrants.add(grant);
              break;
            }
          case NAMESPACE:
            {
              NamespaceGrant grant =
                  new NamespaceGrant(
                      List.of(NamespaceEntity.of(baseEntity).asNamespace().levels()),
                      NamespacePrivilege.valueOf(privilege.toString()),
                      GrantResource.TypeEnum.NAMESPACE);
              namespaceGrants.add(grant);
              break;
            }
          case TABLE_LIKE:
            {
              if (baseEntity.getSubType() == PolarisEntitySubType.ICEBERG_TABLE
                  || baseEntity.getSubType() == PolarisEntitySubType.GENERIC_TABLE) {
                TableIdentifier identifier =
                    IcebergTableLikeEntity.of(baseEntity).getTableIdentifier();
                TableGrant grant =
                    new TableGrant(
                        List.of(identifier.namespace().levels()),
                        identifier.name(),
                        TablePrivilege.valueOf(privilege.toString()),
                        GrantResource.TypeEnum.TABLE);
                tableGrants.add(grant);
              } else if (baseEntity.getSubType() == PolarisEntitySubType.ICEBERG_VIEW) {
                TableIdentifier identifier =
                    IcebergTableLikeEntity.of(baseEntity).getTableIdentifier();
                ViewGrant grant =
                    new ViewGrant(
                        List.of(identifier.namespace().levels()),
                        identifier.name(),
                        ViewPrivilege.valueOf(privilege.toString()),
                        GrantResource.TypeEnum.VIEW);
                viewGrants.add(grant);
              } else {
                throw new IllegalStateException(
                    "Unrecognized entity subtype " + baseEntity.getSubType());
              }
              break;
            }
          case POLICY:
            {
              PolicyEntity policyEntity = PolicyEntity.of(baseEntity);
              PolicyGrant grant =
                  new PolicyGrant(
                      Arrays.asList(policyEntity.getParentNamespace().levels()),
                      policyEntity.getName(),
                      PolicyPrivilege.valueOf(privilege.toString()),
                      GrantResource.TypeEnum.POLICY);
              policyGrants.add(grant);
              break;
            }
          default:
            throw new IllegalArgumentException(
                String.format(
                    "Unexpected entity type '%s' listing grants for catalogRole '%s' in catalog '%s'",
                    baseEntity.getType(), catalogRoleName, catalogName));
        }
      }
    }
    // Assemble these at the end so that they're grouped by type.
    List<GrantResource> allGrants = new ArrayList<>();
    allGrants.addAll(catalogGrants);
    allGrants.addAll(namespaceGrants);
    allGrants.addAll(tableGrants);
    allGrants.addAll(viewGrants);
    allGrants.addAll(policyGrants);
    return allGrants;
  }

  /**
   * Get the specified entity from the input map or load it from backend if the input map is null.
   * Normally the input map is not expected to be null, except for backward compatibility issue.
   *
   * @param entitiesMap map of entities
   * @param catalogId the id of the catalog of the entity we are looking for
   * @param id id of the entity we are looking for
   * @param entityType
   * @return null if the entity does not exist
   */
  private @Nullable PolarisBaseEntity getOrLoadEntity(
      @Nullable Map<Long, PolarisBaseEntity> entitiesMap,
      long catalogId,
      long id,
      PolarisEntityType entityType) {
    return (entitiesMap == null)
        ? metaStoreManager
            .loadEntity(getCurrentPolarisContext(), catalogId, id, entityType)
            .getEntity()
        : entitiesMap.get(id);
  }

  private @Nullable PolarisBaseEntity getOrLoadEntityForGrant(
      @Nullable Map<Long, PolarisBaseEntity> entitiesMap, PolarisGrantRecord record) {
    if (entitiesMap != null) {
      return entitiesMap.get(record.getSecurableId());
    }

    for (PolarisEntityType type : PolarisEntityType.values()) {
      EntityResult entityResult =
          metaStoreManager.loadEntity(
              getCurrentPolarisContext(),
              record.getSecurableCatalogId(),
              record.getSecurableId(),
              type);
      if (entityResult.isSuccess()) {
        return entityResult.getEntity();
      }
    }

    return null;
  }

  /** Adds a table-level or view-level grant on {@code identifier} to {@code catalogRoleName}. */
  private boolean grantPrivilegeOnTableLikeToRole(
      String catalogName,
      String catalogRoleName,
      TableIdentifier identifier,
      List<PolarisEntitySubType> subTypes,
      PolarisPrivilege privilege) {
    if (findCatalogByName(catalogName).isEmpty()) {
      throw new NotFoundException("Parent catalog %s not found", catalogName);
    }
    PolarisEntity catalogRoleEntity =
        findCatalogRoleByName(catalogName, catalogRoleName)
            .orElseThrow(() -> new NotFoundException("CatalogRole %s not found", catalogRoleName));

    PolarisResolvedPathWrapper resolvedPathWrapper =
        resolutionManifest.getResolvedPath(
            identifier, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ANY_SUBTYPE);
    if (resolvedPathWrapper == null
        || !subTypes.contains(resolvedPathWrapper.getRawLeafEntity().getSubType())) {
      CatalogHandler.throwNotFoundExceptionForTableLikeEntity(identifier, subTypes);
    }
    List<PolarisEntity> catalogPath = resolvedPathWrapper.getRawParentPath();
    PolarisEntity tableLikeEntity = resolvedPathWrapper.getRawLeafEntity();

    return metaStoreManager
        .grantPrivilegeOnSecurableToRole(
            getCurrentPolarisContext(),
            catalogRoleEntity,
            PolarisEntity.toCoreList(catalogPath),
            tableLikeEntity,
            privilege)
        .isSuccess();
  }

  /**
   * Removes a table-level or view-level grant on {@code identifier} from {@code catalogRoleName}.
   */
  private boolean revokePrivilegeOnTableLikeFromRole(
      String catalogName,
      String catalogRoleName,
      TableIdentifier identifier,
      List<PolarisEntitySubType> subTypes,
      PolarisPrivilege privilege) {
    if (findCatalogByName(catalogName).isEmpty()) {
      throw new NotFoundException("Parent catalog %s not found", catalogName);
    }
    PolarisEntity catalogRoleEntity =
        findCatalogRoleByName(catalogName, catalogRoleName)
            .orElseThrow(() -> new NotFoundException("CatalogRole %s not found", catalogRoleName));

    PolarisResolvedPathWrapper resolvedPathWrapper =
        resolutionManifest.getResolvedPath(
            identifier, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ANY_SUBTYPE);
    if (resolvedPathWrapper == null
        || !subTypes.contains(resolvedPathWrapper.getRawLeafEntity().getSubType())) {
      CatalogHandler.throwNotFoundExceptionForTableLikeEntity(identifier, subTypes);
    }
    List<PolarisEntity> catalogPath = resolvedPathWrapper.getRawParentPath();
    PolarisEntity tableLikeEntity = resolvedPathWrapper.getRawLeafEntity();

    return metaStoreManager
        .revokePrivilegeOnSecurableFromRole(
            getCurrentPolarisContext(),
            catalogRoleEntity,
            PolarisEntity.toCoreList(catalogPath),
            tableLikeEntity,
            privilege)
        .isSuccess();
  }

  private boolean grantPrivilegeOnPolicyEntityToRole(
      String catalogName,
      String catalogRoleName,
      PolicyIdentifier identifier,
      PolarisPrivilege privilege) {
    if (findCatalogByName(catalogName).isEmpty()) {
      throw new NotFoundException("Parent catalog %s not found", catalogName);
    }
    PolarisEntity catalogRoleEntity =
        findCatalogRoleByName(catalogName, catalogRoleName)
            .orElseThrow(() -> new NotFoundException("CatalogRole %s not found", catalogRoleName));

    PolarisResolvedPathWrapper resolvedPathWrapper = resolutionManifest.getResolvedPath(identifier);
    if (resolvedPathWrapper == null) {
      throw new NoSuchPolicyException(String.format("Policy not exists: %s", identifier));
    }

    List<PolarisEntity> catalogPath = resolvedPathWrapper.getRawParentPath();
    PolarisEntity policyEntity = resolvedPathWrapper.getRawLeafEntity();

    return metaStoreManager
        .grantPrivilegeOnSecurableToRole(
            getCurrentPolarisContext(),
            catalogRoleEntity,
            PolarisEntity.toCoreList(catalogPath),
            policyEntity,
            privilege)
        .isSuccess();
  }

  private boolean revokePrivilegeOnPolicyEntityFromRole(
      String catalogName,
      String catalogRoleName,
      PolicyIdentifier identifier,
      PolarisPrivilege privilege) {
    if (findCatalogByName(catalogName).isEmpty()) {
      throw new NotFoundException("Parent catalog %s not found", catalogName);
    }
    PolarisEntity catalogRoleEntity =
        findCatalogRoleByName(catalogName, catalogRoleName)
            .orElseThrow(() -> new NotFoundException("CatalogRole %s not found", catalogRoleName));

    PolarisResolvedPathWrapper resolvedPathWrapper = resolutionManifest.getResolvedPath(identifier);
    if (resolvedPathWrapper == null) {
      throw new NoSuchPolicyException(String.format("Policy not exists: %s", identifier));
    }

    List<PolarisEntity> catalogPath = resolvedPathWrapper.getRawParentPath();
    PolarisEntity policyEntity = resolvedPathWrapper.getRawLeafEntity();

    return metaStoreManager
        .revokePrivilegeOnSecurableFromRole(
            getCurrentPolarisContext(),
            catalogRoleEntity,
            PolarisEntity.toCoreList(catalogPath),
            policyEntity,
            privilege)
        .isSuccess();
  }
}
