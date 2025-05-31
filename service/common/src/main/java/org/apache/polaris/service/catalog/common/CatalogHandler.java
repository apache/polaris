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
package org.apache.polaris.service.catalog.common;

import static org.apache.polaris.core.entity.PolarisEntitySubType.ICEBERG_TABLE;

import jakarta.ws.rs.core.SecurityContext;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.service.catalog.conversion.xtable.RemoteXTableConvertor;
import org.apache.polaris.service.types.PolicyIdentifier;

/**
 * An ABC for catalog wrappers which provides authorize methods that should be called before a
 * request is actually forwarded to a catalog. Child types must implement `initializeCatalog` which
 * will be called after a successful authorization.
 */
public abstract class CatalogHandler {

  // Initialized in the authorize methods.
  protected PolarisResolutionManifest resolutionManifest = null;

  protected final PolarisEntityManager entityManager;
  protected final String catalogName;
  protected final PolarisAuthorizer authorizer;

  protected final CallContext callContext;
  protected final AuthenticatedPolarisPrincipal authenticatedPrincipal;
  protected final SecurityContext securityContext;

  public CatalogHandler(
      CallContext callContext,
      PolarisEntityManager entityManager,
      SecurityContext securityContext,
      String catalogName,
      PolarisAuthorizer authorizer) {
    this.callContext = callContext;
    this.entityManager = entityManager;
    this.catalogName = catalogName;
    PolarisDiagnostics diagServices = callContext.getPolarisCallContext().getDiagServices();
    diagServices.checkNotNull(securityContext, "null_security_context");
    diagServices.checkNotNull(securityContext.getUserPrincipal(), "null_user_principal");
    diagServices.check(
        securityContext.getUserPrincipal() instanceof AuthenticatedPolarisPrincipal,
        "invalid_principal_type",
        "Principal must be an AuthenticatedPolarisPrincipal");
    this.securityContext = securityContext;
    this.authenticatedPrincipal =
        (AuthenticatedPolarisPrincipal) securityContext.getUserPrincipal();
    this.authorizer = authorizer;
  }

  /** Initialize the catalog once authorized. Called after all `authorize...` methods. */
  protected abstract void initializeCatalog();

  protected void authorizeBasicNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op, Namespace namespace) {
    authorizeBasicNamespaceOperationOrThrow(op, namespace, null, null, null);
  }

  protected void authorizeBasicNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op,
      Namespace namespace,
      List<Namespace> extraPassthroughNamespaces,
      List<TableIdentifier> extraPassthroughTableLikes,
      List<PolicyIdentifier> extraPassThroughPolicies) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, securityContext, catalogName);
    resolutionManifest.addPath(
        new ResolverPath(Arrays.asList(namespace.levels()), PolarisEntityType.NAMESPACE),
        namespace);

    if (extraPassthroughNamespaces != null) {
      for (Namespace ns : extraPassthroughNamespaces) {
        resolutionManifest.addPassthroughPath(
            new ResolverPath(
                Arrays.asList(ns.levels()), PolarisEntityType.NAMESPACE, true /* optional */),
            ns);
      }
    }
    if (extraPassthroughTableLikes != null) {
      for (TableIdentifier id : extraPassthroughTableLikes) {
        resolutionManifest.addPassthroughPath(
            new ResolverPath(
                PolarisCatalogHelpers.tableIdentifierToList(id),
                PolarisEntityType.TABLE_LIKE,
                true /* optional */),
            id);
      }
    }

    if (extraPassThroughPolicies != null) {
      for (PolicyIdentifier id : extraPassThroughPolicies) {
        resolutionManifest.addPassthroughPath(
            new ResolverPath(
                PolarisCatalogHelpers.identifierToList(id.getNamespace(), id.getName()),
                PolarisEntityType.POLICY,
                true /* optional */),
            id);
      }
    }

    resolutionManifest.resolveAll();
    PolarisResolvedPathWrapper target = resolutionManifest.getResolvedPath(namespace, true);
    if (target == null) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
    authorizer.authorizeOrThrow(
        callContext,
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        target,
        null /* secondary */);

    initializeCatalog();
  }

  protected void authorizeCreateNamespaceUnderNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op, Namespace namespace) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, securityContext, catalogName);

    Namespace parentNamespace = PolarisCatalogHelpers.getParentNamespace(namespace);
    resolutionManifest.addPath(
        new ResolverPath(Arrays.asList(parentNamespace.levels()), PolarisEntityType.NAMESPACE),
        parentNamespace);

    // When creating an entity under a namespace, the authz target is the parentNamespace, but we
    // must also add the actual path that will be created as an "optional" passthrough resolution
    // path to indicate that the underlying catalog is "allowed" to check the creation path for
    // a conflicting entity.
    resolutionManifest.addPassthroughPath(
        new ResolverPath(
            Arrays.asList(namespace.levels()), PolarisEntityType.NAMESPACE, true /* optional */),
        namespace);
    resolutionManifest.resolveAll();
    PolarisResolvedPathWrapper target = resolutionManifest.getResolvedPath(parentNamespace, true);
    if (target == null) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", parentNamespace);
    }
    authorizer.authorizeOrThrow(
        callContext,
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        target,
        null /* secondary */);

    initializeCatalog();
  }

  protected void authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op, TableIdentifier identifier) {
    Namespace namespace = identifier.namespace();

    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, securityContext, catalogName);
    resolutionManifest.addPath(
        new ResolverPath(Arrays.asList(namespace.levels()), PolarisEntityType.NAMESPACE),
        namespace);

    // When creating an entity under a namespace, the authz target is the namespace, but we must
    // also
    // add the actual path that will be created as an "optional" passthrough resolution path to
    // indicate that the underlying catalog is "allowed" to check the creation path for a
    // conflicting
    // entity.
    resolutionManifest.addPassthroughPath(
        new ResolverPath(
            PolarisCatalogHelpers.tableIdentifierToList(identifier),
            PolarisEntityType.TABLE_LIKE,
            true /* optional */),
        identifier);
    resolutionManifest.resolveAll();
    PolarisResolvedPathWrapper target = resolutionManifest.getResolvedPath(namespace, true);
    if (target == null) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
    authorizer.authorizeOrThrow(
        callContext,
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        target,
        null /* secondary */);

    initializeCatalog();
  }

  protected void authorizeBasicTableLikeOperationOrThrow(
      PolarisAuthorizableOperation op, PolarisEntitySubType subType, TableIdentifier identifier) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, securityContext, catalogName);

    // The underlying Catalog is also allowed to fetch "fresh" versions of the target entity.
    resolutionManifest.addPassthroughPath(
        new ResolverPath(
            PolarisCatalogHelpers.tableIdentifierToList(identifier),
            PolarisEntityType.TABLE_LIKE,
            true /* optional */),
        identifier);
    resolutionManifest.resolveAll();
    PolarisResolvedPathWrapper target =
        resolutionManifest.getResolvedPath(identifier, PolarisEntityType.TABLE_LIKE, subType, true);
    if (target == null) {
      throwNotFoundExceptionForTableLikeEntity(identifier, List.of(subType));
    }
    authorizer.authorizeOrThrow(
        callContext,
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        target,
        null /* secondary */);

    initializeCatalog();
  }

  protected void authorizeCollectionOfTableLikeOperationOrThrow(
      PolarisAuthorizableOperation op,
      final PolarisEntitySubType subType,
      List<TableIdentifier> ids) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, securityContext, catalogName);
    ids.forEach(
        identifier ->
            resolutionManifest.addPassthroughPath(
                new ResolverPath(
                    PolarisCatalogHelpers.tableIdentifierToList(identifier),
                    PolarisEntityType.TABLE_LIKE),
                identifier));

    ResolverStatus status = resolutionManifest.resolveAll();

    // If one of the paths failed to resolve, throw exception based on the one that
    // we first failed to resolve.
    if (status.getStatus() == ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED) {
      TableIdentifier identifier =
          PolarisCatalogHelpers.listToTableIdentifier(
              status.getFailedToResolvePath().getEntityNames());
      throwNotFoundExceptionForTableLikeEntity(identifier, List.of(subType));
    }

    List<PolarisResolvedPathWrapper> targets =
        ids.stream()
            .map(
                identifier ->
                    Optional.ofNullable(
                            resolutionManifest.getResolvedPath(
                                identifier, PolarisEntityType.TABLE_LIKE, subType, true))
                        .orElseThrow(
                            () ->
                                subType == ICEBERG_TABLE
                                    ? new NoSuchTableException(
                                        "Table does not exist: %s", identifier)
                                    : new NoSuchViewException(
                                        "View does not exist: %s", identifier)))
            .toList();
    authorizer.authorizeOrThrow(
        callContext,
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        targets,
        null /* secondaries */);

    initializeCatalog();
  }

  protected void authorizeRenameTableLikeOperationOrThrow(
      PolarisAuthorizableOperation op,
      PolarisEntitySubType subType,
      TableIdentifier src,
      TableIdentifier dst) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, securityContext, catalogName);
    // Add src, dstParent, and dst(optional)
    resolutionManifest.addPath(
        new ResolverPath(
            PolarisCatalogHelpers.tableIdentifierToList(src), PolarisEntityType.TABLE_LIKE),
        src);
    resolutionManifest.addPath(
        new ResolverPath(Arrays.asList(dst.namespace().levels()), PolarisEntityType.NAMESPACE),
        dst.namespace());
    resolutionManifest.addPath(
        new ResolverPath(
            PolarisCatalogHelpers.tableIdentifierToList(dst),
            PolarisEntityType.TABLE_LIKE,
            true /* optional */),
        dst);
    ResolverStatus status = resolutionManifest.resolveAll();
    if (status.getStatus() == ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED
        && status.getFailedToResolvePath().getLastEntityType() == PolarisEntityType.NAMESPACE) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", dst.namespace());
    } else if (resolutionManifest.getResolvedPath(src, PolarisEntityType.TABLE_LIKE, subType)
        == null) {
      throwNotFoundExceptionForTableLikeEntity(dst, List.of(subType));
    }

    // Normally, since we added the dst as an optional path, we'd expect it to only get resolved
    // up to its parent namespace, and for there to be no TABLE_LIKE already in the dst in which
    // case the leafSubType will be NULL_SUBTYPE.
    // If there is a conflicting TABLE or VIEW, this leafSubType will indicate that conflicting
    // type.
    // TODO: Possibly modify the exception thrown depending on whether the caller has privileges
    // on the parent namespace.
    PolarisEntitySubType dstLeafSubType = resolutionManifest.getLeafSubType(dst);

    switch (dstLeafSubType) {
      case ICEBERG_TABLE:
        throw new AlreadyExistsException("Cannot rename %s to %s. Table already exists", src, dst);

      case PolarisEntitySubType.ICEBERG_VIEW:
        throw new AlreadyExistsException("Cannot rename %s to %s. View already exists", src, dst);

      case PolarisEntitySubType.GENERIC_TABLE:
        throw new AlreadyExistsException(
            "Cannot rename %s to %s. Generic table already exists", src, dst);

      default:
        break;
    }

    PolarisResolvedPathWrapper target =
        resolutionManifest.getResolvedPath(src, PolarisEntityType.TABLE_LIKE, subType, true);
    PolarisResolvedPathWrapper secondary =
        resolutionManifest.getResolvedPath(dst.namespace(), true);
    authorizer.authorizeOrThrow(
        callContext,
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        target,
        secondary);

    initializeCatalog();
  }

  protected void initializeConversionServiceIfEnabled() {
    boolean conversionServiceEnabled =
        callContext
            .getPolarisCallContext()
            .getConfigurationStore()
            .getConfiguration(
                callContext.getPolarisCallContext(),
                FeatureConfiguration.ENABLE_XTABLE_REST_SERVICE);
    if (conversionServiceEnabled) {
      String hostUrl =
          callContext
              .getPolarisCallContext()
              .getConfigurationStore()
              .getConfiguration(
                  callContext.getPolarisCallContext(),
                  FeatureConfiguration.XTABLE_REST_SERVICE_HOST_URL);
      RemoteXTableConvertor.initialize(hostUrl);
    }
  }

  /**
   * Helper function for when a TABLE_LIKE entity is not found so we want to throw the appropriate
   * exception. Used in Iceberg APIs, so the Iceberg messages cannot be changed.
   *
   * @param subTypes The subtypes of the entity that the exception should report doesn't exist
   */
  public static void throwNotFoundExceptionForTableLikeEntity(
      TableIdentifier identifier, List<PolarisEntitySubType> subTypes) {

    // In this case, we assume it's a table
    if (subTypes.size() > 1) {
      throw new NoSuchTableException("Table does not exist: %s", identifier);
    } else {
      PolarisEntitySubType subType = subTypes.getFirst();
      switch (subType) {
        case ICEBERG_TABLE:
          throw new NoSuchTableException("Table does not exist: %s", identifier);
        case ICEBERG_VIEW:
          throw new NoSuchViewException("View does not exist: %s", identifier);
        case GENERIC_TABLE:
          throw new NoSuchTableException("Generic table does not exist: %s", identifier);
        default:
          // Assume it's a table
          throw new NoSuchTableException("Table does not exist: %s", identifier);
      }
    }
  }
}
