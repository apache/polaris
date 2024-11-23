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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisConfiguration;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.NamespaceGrant;
import org.apache.polaris.core.admin.model.NamespacePrivilege;
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
import org.apache.polaris.core.auth.PolarisGrantManager.LoadGrantsResult;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
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
import org.apache.polaris.core.entity.TableLikeEntity;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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
  private final AuthenticatedPolarisPrincipal authenticatedPrincipal;
  private final PolarisAuthorizer authorizer;
  private final PolarisMetaStoreManager metaStoreManager;

  // Initialized in the authorize methods.
  private PolarisResolutionManifest resolutionManifest = null;

  public PolarisAdminService(
      CallContext callContext,
      PolarisEntityManager entityManager,
      PolarisMetaStoreManager metaStoreManager,
      AuthenticatedPolarisPrincipal authenticatedPrincipal,
      PolarisAuthorizer authorizer) {
    this.callContext = callContext;
    this.entityManager = entityManager;
    this.metaStoreManager = metaStoreManager;
    this.authenticatedPrincipal = authenticatedPrincipal;
    this.authorizer = authorizer;
  }

  private PolarisCallContext getCurrentPolarisContext() {
    return callContext.getPolarisCallContext();
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
            callContext, authenticatedPrincipal, null /* referenceCatalogName */);
    resolutionManifest.resolveAll();
    PolarisResolvedPathWrapper rootContainerWrapper =
        resolutionManifest.getResolvedRootContainerEntityAsPath();
    authorizer.authorizeOrThrow(
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
        entityManager.prepareResolutionManifest(
            callContext, authenticatedPrincipal, referenceCatalogName);
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
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        topLevelEntityWrapper,
        null /* secondary */);
  }

  private void authorizeBasicCatalogRoleOperationOrThrow(
      PolarisAuthorizableOperation op, String catalogName, String catalogRoleName) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, authenticatedPrincipal, catalogName);
    resolutionManifest.addPath(
        new ResolverPath(List.of(catalogRoleName), PolarisEntityType.CATALOG_ROLE),
        catalogRoleName);
    resolutionManifest.resolveAll();
    PolarisResolvedPathWrapper target = resolutionManifest.getResolvedPath(catalogRoleName, true);
    if (target == null) {
      throw new NotFoundException("CatalogRole does not exist: %s", catalogRoleName);
    }
    authorizer.authorizeOrThrow(
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        target,
        null /* secondary */);
  }

  private void authorizeGrantOnRootContainerToPrincipalRoleOperationOrThrow(
      PolarisAuthorizableOperation op, String principalRoleName) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, authenticatedPrincipal, null);
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
        entityManager.prepareResolutionManifest(callContext, authenticatedPrincipal, null);
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
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        topLevelEntityWrapper,
        principalRoleWrapper);
  }

  private void authorizeGrantOnPrincipalRoleToPrincipalOperationOrThrow(
      PolarisAuthorizableOperation op, String principalRoleName, String principalName) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, authenticatedPrincipal, null);
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
        entityManager.prepareResolutionManifest(callContext, authenticatedPrincipal, catalogName);
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
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        catalogRoleWrapper,
        principalRoleWrapper);
  }

  private void authorizeGrantOnCatalogOperationOrThrow(
      PolarisAuthorizableOperation op, String catalogName, String catalogRoleName) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, authenticatedPrincipal, catalogName);
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
        entityManager.prepareResolutionManifest(callContext, authenticatedPrincipal, catalogName);
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
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        namespaceWrapper,
        catalogRoleWrapper);
  }

  private void authorizeGrantOnTableLikeOperationOrThrow(
      PolarisAuthorizableOperation op,
      String catalogName,
      PolarisEntitySubType subType,
      TableIdentifier identifier,
      String catalogRoleName) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, authenticatedPrincipal, catalogName);
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
        if (subType == PolarisEntitySubType.TABLE) {
          throw new NoSuchTableException("Table does not exist: %s", identifier);
        } else {
          throw new NoSuchViewException("View does not exist: %s", identifier);
        }
      } else {
        throw new NotFoundException("CatalogRole not found: %s.%s", catalogName, catalogRoleName);
      }
    }

    PolarisResolvedPathWrapper tableLikeWrapper =
        resolutionManifest.getResolvedPath(identifier, subType, true);
    PolarisResolvedPathWrapper catalogRoleWrapper =
        resolutionManifest.getResolvedPath(catalogRoleName, true);

    authorizer.authorizeOrThrow(
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        tableLikeWrapper,
        catalogRoleWrapper);
  }

  /** Get all locations where data for a `CatalogEntity` may be stored */
  private Set<String> getCatalogLocations(CatalogEntity catalogEntity) {
    HashSet<String> catalogLocations = new HashSet<>();
    catalogLocations.add(terminateWithSlash(catalogEntity.getDefaultBaseLocation()));
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
                getCurrentPolarisContext(), PolarisConfiguration.ALLOW_OVERLAPPING_CATALOG_URLS);

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

  public PolarisEntity createCatalog(PolarisEntity entity) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_CATALOG;
    authorizeBasicRootOperationOrThrow(op);

    if (catalogOverlapsWithExistingCatalog((CatalogEntity) entity)) {
      throw new ValidationException(
          "Cannot create Catalog %s. One or more of its locations overlaps with an existing catalog",
          entity.getName());
    }

    long id =
        entity.getId() <= 0
            ? metaStoreManager.generateNewEntityId(getCurrentPolarisContext()).getId()
            : entity.getId();
    PolarisEntity polarisEntity =
        new PolarisEntity.Builder(entity)
            .setId(id)
            .setCreateTimestamp(System.currentTimeMillis())
            .build();
    PolarisMetaStoreManager.CreateCatalogResult catalogResult =
        metaStoreManager.createCatalog(getCurrentPolarisContext(), polarisEntity, List.of());
    if (catalogResult.alreadyExists()) {
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
            .getConfiguration(polarisCallContext, PolarisConfiguration.CLEANUP_ON_CATALOG_DROP);
    PolarisMetaStoreManager.DropEntityResult dropEntityResult =
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

  public @NotNull CatalogEntity getCatalog(String name) {
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

      if (!currentAwsConfig.getRoleARN().equals(newAwsConfig.getRoleARN())
          || !newAwsConfig.getRoleARN().equals(currentAwsConfig.getRoleARN())) {
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

  public @NotNull CatalogEntity updateCatalog(String name, UpdateCatalogRequest updateRequest) {
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
    String defaultBaseLocation = currentCatalogEntity.getDefaultBaseLocation();
    if (updateRequest.getProperties() != null) {
      updateBuilder.setProperties(updateRequest.getProperties());
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
          updateRequest.getStorageConfigInfo(), defaultBaseLocation);
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

  public List<PolarisEntity> listCatalogs() {
    authorizeBasicRootOperationOrThrow(PolarisAuthorizableOperation.LIST_CATALOGS);
    return listCatalogsUnsafe();
  }

  /** List all catalogs without checking for permission */
  private List<PolarisEntity> listCatalogsUnsafe() {
    return metaStoreManager
        .listEntities(
            getCurrentPolarisContext(),
            null,
            PolarisEntityType.CATALOG,
            PolarisEntitySubType.ANY_SUBTYPE)
        .getEntities()
        .stream()
        .map(
            nameAndId ->
                PolarisEntity.of(
                    metaStoreManager.loadEntity(getCurrentPolarisContext(), 0, nameAndId.getId())))
        .toList();
  }

  public PrincipalWithCredentials createPrincipal(PolarisEntity entity) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_PRINCIPAL;
    authorizeBasicRootOperationOrThrow(op);

    long id =
        entity.getId() <= 0
            ? metaStoreManager.generateNewEntityId(getCurrentPolarisContext()).getId()
            : entity.getId();
    PolarisMetaStoreManager.CreatePrincipalResult principalResult =
        metaStoreManager.createPrincipal(
            getCurrentPolarisContext(),
            new PolarisEntity.Builder(entity)
                .setId(id)
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
    PolarisMetaStoreManager.DropEntityResult dropEntityResult =
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

  public @NotNull PrincipalEntity getPrincipal(String name) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.GET_PRINCIPAL;
    authorizeBasicTopLevelEntityOperationOrThrow(op, name, PolarisEntityType.PRINCIPAL);

    return findPrincipalByName(name)
        .orElseThrow(() -> new NotFoundException("Principal %s not found", name));
  }

  public @NotNull PrincipalEntity updatePrincipal(
      String name, UpdatePrincipalRequest updateRequest) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.UPDATE_PRINCIPAL;
    authorizeBasicTopLevelEntityOperationOrThrow(op, name, PolarisEntityType.PRINCIPAL);

    PrincipalEntity currentPrincipalEntity =
        findPrincipalByName(name)
            .orElseThrow(() -> new NotFoundException("Principal %s not found", name));

    if (currentPrincipalEntity.getEntityVersion() != updateRequest.getCurrentEntityVersion()) {
      throw new CommitFailedException(
          "Failed to update Principal; currentEntityVersion '%s', expected '%s'",
          currentPrincipalEntity.getEntityVersion(), updateRequest.getCurrentEntityVersion());
    }

    PrincipalEntity.Builder updateBuilder = new PrincipalEntity.Builder(currentPrincipalEntity);
    if (updateRequest.getProperties() != null) {
      updateBuilder.setProperties(updateRequest.getProperties());
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

  private @NotNull PrincipalWithCredentials rotateOrResetCredentialsHelper(
      String principalName, boolean shouldReset) {
    PrincipalEntity currentPrincipalEntity =
        findPrincipalByName(principalName)
            .orElseThrow(() -> new NotFoundException("Principal %s not found", principalName));

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
                getCurrentPolarisContext(), 0L, currentPrincipalEntity.getId()));
    return new PrincipalWithCredentials(
        PrincipalEntity.of(newPrincipal).asPrincipal(),
        new PrincipalWithCredentialsCredentials(
            newSecrets.getPrincipalClientId(), newSecrets.getMainSecret()));
  }

  public @NotNull PrincipalWithCredentials rotateCredentials(String principalName) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.ROTATE_CREDENTIALS;
    authorizeBasicTopLevelEntityOperationOrThrow(op, principalName, PolarisEntityType.PRINCIPAL);

    return rotateOrResetCredentialsHelper(principalName, false);
  }

  public @NotNull PrincipalWithCredentials resetCredentials(String principalName) {
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
            PolarisEntitySubType.NULL_SUBTYPE)
        .getEntities()
        .stream()
        .map(
            nameAndId ->
                PolarisEntity.of(
                    metaStoreManager.loadEntity(getCurrentPolarisContext(), 0, nameAndId.getId())))
        .toList();
  }

  public PolarisEntity createPrincipalRole(PolarisEntity entity) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_PRINCIPAL_ROLE;
    authorizeBasicRootOperationOrThrow(op);

    long id =
        entity.getId() <= 0
            ? metaStoreManager.generateNewEntityId(getCurrentPolarisContext()).getId()
            : entity.getId();
    PolarisEntity returnedEntity =
        PolarisEntity.of(
            metaStoreManager.createEntityIfNotExists(
                getCurrentPolarisContext(),
                null,
                new PolarisEntity.Builder(entity)
                    .setId(id)
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
    PolarisMetaStoreManager.DropEntityResult dropEntityResult =
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

  public @NotNull PrincipalRoleEntity getPrincipalRole(String name) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.GET_PRINCIPAL_ROLE;
    authorizeBasicTopLevelEntityOperationOrThrow(op, name, PolarisEntityType.PRINCIPAL_ROLE);

    return findPrincipalRoleByName(name)
        .orElseThrow(() -> new NotFoundException("PrincipalRole %s not found", name));
  }

  public @NotNull PrincipalRoleEntity updatePrincipalRole(
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
      updateBuilder.setProperties(updateRequest.getProperties());
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
            PolarisEntitySubType.NULL_SUBTYPE)
        .getEntities()
        .stream()
        .map(
            nameAndId ->
                PolarisEntity.of(
                    metaStoreManager.loadEntity(getCurrentPolarisContext(), 0, nameAndId.getId())))
        .toList();
  }

  public PolarisEntity createCatalogRole(String catalogName, PolarisEntity entity) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_CATALOG_ROLE;
    authorizeBasicTopLevelEntityOperationOrThrow(op, catalogName, PolarisEntityType.CATALOG);

    PolarisEntity catalogEntity =
        findCatalogByName(catalogName)
            .orElseThrow(() -> new NotFoundException("Parent catalog %s not found", catalogName));

    long id =
        entity.getId() <= 0
            ? metaStoreManager.generateNewEntityId(getCurrentPolarisContext()).getId()
            : entity.getId();
    PolarisEntity returnedEntity =
        PolarisEntity.of(
            metaStoreManager.createEntityIfNotExists(
                getCurrentPolarisContext(),
                PolarisEntity.toCoreList(List.of(catalogEntity)),
                new PolarisEntity.Builder(entity)
                    .setId(id)
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
    PolarisMetaStoreManager.DropEntityResult dropEntityResult =
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

  public @NotNull CatalogRoleEntity getCatalogRole(String catalogName, String name) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.GET_CATALOG_ROLE;
    authorizeBasicCatalogRoleOperationOrThrow(op, catalogName, name);

    return findCatalogRoleByName(catalogName, name)
        .orElseThrow(() -> new NotFoundException("CatalogRole %s not found", name));
  }

  public @NotNull CatalogRoleEntity updateCatalogRole(
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
      updateBuilder.setProperties(updateRequest.getProperties());
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
            PolarisEntitySubType.NULL_SUBTYPE)
        .getEntities()
        .stream()
        .map(
            nameAndId ->
                PolarisEntity.of(
                    metaStoreManager.loadEntity(
                        getCurrentPolarisContext(), catalogEntity.getId(), nameAndId.getId())))
        .toList();
  }

  public boolean assignPrincipalRole(String principalName, String principalRoleName) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.ASSIGN_PRINCIPAL_ROLE;
    authorizeGrantOnPrincipalRoleToPrincipalOperationOrThrow(op, principalRoleName, principalName);

    PolarisEntity principalEntity =
        findPrincipalByName(principalName)
            .orElseThrow(() -> new NotFoundException("Principal %s not found", principalName));
    PolarisEntity principalRoleEntity =
        findPrincipalRoleByName(principalRoleName)
            .orElseThrow(
                () -> new NotFoundException("PrincipalRole %s not found", principalRoleName));

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
    PolarisEntity principalRoleEntity =
        findPrincipalRoleByName(principalRoleName)
            .orElseThrow(
                () -> new NotFoundException("PrincipalRole %s not found", principalRoleName));
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
        metaStoreManager.loadGrantsToGrantee(
            getCurrentPolarisContext(), principalEntity.getCatalogId(), principalEntity.getId());
    return buildEntitiesFromGrantResults(grantList, false, null);
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
        metaStoreManager.loadGrantsOnSecurable(
            getCurrentPolarisContext(),
            principalRoleEntity.getCatalogId(),
            principalRoleEntity.getId());
    return buildEntitiesFromGrantResults(grantList, true, null);
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
      @NotNull LoadGrantsResult grantList,
      boolean grantees,
      @Nullable Function<PolarisGrantRecord, Boolean> grantFilter) {
    Map<Long, PolarisBaseEntity> granteeMap = grantList.getEntitiesAsMap();
    List<PolarisEntity> toReturn = new ArrayList<>(grantList.getGrantRecords().size());
    for (PolarisGrantRecord grantRecord : grantList.getGrantRecords()) {
      if (grantFilter == null || grantFilter.apply(grantRecord)) {
        long catalogId =
            grantees ? grantRecord.getGranteeCatalogId() : grantRecord.getSecurableCatalogId();
        long entityId = grantees ? grantRecord.getGranteeId() : grantRecord.getSecurableId();
        // get the entity associated with the grantee
        PolarisBaseEntity entity = this.getOrLoadEntity(granteeMap, catalogId, entityId);
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
        metaStoreManager.loadGrantsToGrantee(
            getCurrentPolarisContext(),
            principalRoleEntity.getCatalogId(),
            principalRoleEntity.getId());
    return buildEntitiesFromGrantResults(
        grantList, false, grantRec -> grantRec.getSecurableCatalogId() == catalogEntity.getId());
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
        op, catalogName, PolarisEntitySubType.TABLE, identifier, catalogRoleName);

    return grantPrivilegeOnTableLikeToRole(
        catalogName, catalogRoleName, identifier, PolarisEntitySubType.TABLE, privilege);
  }

  public boolean revokePrivilegeOnTableFromRole(
      String catalogName,
      String catalogRoleName,
      TableIdentifier identifier,
      PolarisPrivilege privilege) {
    PolarisAuthorizableOperation op =
        PolarisAuthorizableOperation.REVOKE_TABLE_GRANT_FROM_CATALOG_ROLE;

    authorizeGrantOnTableLikeOperationOrThrow(
        op, catalogName, PolarisEntitySubType.TABLE, identifier, catalogRoleName);

    return revokePrivilegeOnTableLikeFromRole(
        catalogName, catalogRoleName, identifier, PolarisEntitySubType.TABLE, privilege);
  }

  public boolean grantPrivilegeOnViewToRole(
      String catalogName,
      String catalogRoleName,
      TableIdentifier identifier,
      PolarisPrivilege privilege) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.ADD_VIEW_GRANT_TO_CATALOG_ROLE;

    authorizeGrantOnTableLikeOperationOrThrow(
        op, catalogName, PolarisEntitySubType.VIEW, identifier, catalogRoleName);

    return grantPrivilegeOnTableLikeToRole(
        catalogName, catalogRoleName, identifier, PolarisEntitySubType.VIEW, privilege);
  }

  public boolean revokePrivilegeOnViewFromRole(
      String catalogName,
      String catalogRoleName,
      TableIdentifier identifier,
      PolarisPrivilege privilege) {
    PolarisAuthorizableOperation op =
        PolarisAuthorizableOperation.REVOKE_VIEW_GRANT_FROM_CATALOG_ROLE;

    authorizeGrantOnTableLikeOperationOrThrow(
        op, catalogName, PolarisEntitySubType.VIEW, identifier, catalogRoleName);

    return revokePrivilegeOnTableLikeFromRole(
        catalogName, catalogRoleName, identifier, PolarisEntitySubType.VIEW, privilege);
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
        metaStoreManager.loadGrantsOnSecurable(
            getCurrentPolarisContext(),
            catalogRoleEntity.getCatalogId(),
            catalogRoleEntity.getId());
    return buildEntitiesFromGrantResults(grantList, true, null);
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
        metaStoreManager.loadGrantsToGrantee(
            getCurrentPolarisContext(),
            catalogRoleEntity.getCatalogId(),
            catalogRoleEntity.getId());
    List<CatalogGrant> catalogGrants = new ArrayList<>();
    List<NamespaceGrant> namespaceGrants = new ArrayList<>();
    List<TableGrant> tableGrants = new ArrayList<>();
    List<ViewGrant> viewGrants = new ArrayList<>();
    Map<Long, PolarisBaseEntity> entityMap = grantList.getEntitiesAsMap();
    for (PolarisGrantRecord record : grantList.getGrantRecords()) {
      PolarisPrivilege privilege = PolarisPrivilege.fromCode(record.getPrivilegeCode());
      PolarisBaseEntity baseEntity =
          this.getOrLoadEntity(entityMap, record.getSecurableCatalogId(), record.getSecurableId());
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
              if (baseEntity.getSubType() == PolarisEntitySubType.TABLE) {
                TableIdentifier identifier = TableLikeEntity.of(baseEntity).getTableIdentifier();
                TableGrant grant =
                    new TableGrant(
                        List.of(identifier.namespace().levels()),
                        identifier.name(),
                        TablePrivilege.valueOf(privilege.toString()),
                        GrantResource.TypeEnum.TABLE);
                tableGrants.add(grant);
              } else {
                TableIdentifier identifier = TableLikeEntity.of(baseEntity).getTableIdentifier();
                ViewGrant grant =
                    new ViewGrant(
                        List.of(identifier.namespace().levels()),
                        identifier.name(),
                        ViewPrivilege.valueOf(privilege.toString()),
                        GrantResource.TypeEnum.VIEW);
                viewGrants.add(grant);
              }
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
    return allGrants;
  }

  /**
   * Get the specified entity from the input map or load it from backend if the input map is null.
   * Normally the input map is not expected to be null, except for backward compatibility issue.
   *
   * @param entitiesMap map of entities
   * @param catalogId the id of the catalog of the entity we are looking for
   * @param id id of the entity we are looking for
   * @return null if the entity does not exist
   */
  private @Nullable PolarisBaseEntity getOrLoadEntity(
      @Nullable Map<Long, PolarisBaseEntity> entitiesMap, long catalogId, long id) {
    return (entitiesMap == null)
        ? metaStoreManager.loadEntity(getCurrentPolarisContext(), catalogId, id).getEntity()
        : entitiesMap.get(id);
  }

  /** Adds a table-level or view-level grant on {@code identifier} to {@code catalogRoleName}. */
  private boolean grantPrivilegeOnTableLikeToRole(
      String catalogName,
      String catalogRoleName,
      TableIdentifier identifier,
      PolarisEntitySubType subType,
      PolarisPrivilege privilege) {
    if (findCatalogByName(catalogName).isEmpty()) {
      throw new NotFoundException("Parent catalog %s not found", catalogName);
    }
    PolarisEntity catalogRoleEntity =
        findCatalogRoleByName(catalogName, catalogRoleName)
            .orElseThrow(() -> new NotFoundException("CatalogRole %s not found", catalogRoleName));

    PolarisResolvedPathWrapper resolvedPathWrapper =
        resolutionManifest.getResolvedPath(identifier, subType);
    if (resolvedPathWrapper == null) {
      if (subType == PolarisEntitySubType.VIEW) {
        throw new NotFoundException("View %s not found", identifier);
      } else {
        throw new NotFoundException("Table %s not found", identifier);
      }
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
      PolarisEntitySubType subType,
      PolarisPrivilege privilege) {
    if (findCatalogByName(catalogName).isEmpty()) {
      throw new NotFoundException("Parent catalog %s not found", catalogName);
    }
    PolarisEntity catalogRoleEntity =
        findCatalogRoleByName(catalogName, catalogRoleName)
            .orElseThrow(() -> new NotFoundException("CatalogRole %s not found", catalogRoleName));

    PolarisResolvedPathWrapper resolvedPathWrapper =
        resolutionManifest.getResolvedPath(identifier, subType);
    if (resolvedPathWrapper == null) {
      if (subType == PolarisEntitySubType.VIEW) {
        throw new NotFoundException("View %s not found", identifier);
      } else {
        throw new NotFoundException("Table %s not found", identifier);
      }
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
}
