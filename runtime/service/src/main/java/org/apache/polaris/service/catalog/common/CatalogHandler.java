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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.polaris.service.catalog.common.ExceptionUtils.alreadyExistsExceptionWithSameNameForTableLikeEntity;
import static org.apache.polaris.service.catalog.common.ExceptionUtils.entityNameForSubType;
import static org.apache.polaris.service.catalog.common.ExceptionUtils.noSuchNamespaceException;
import static org.apache.polaris.service.catalog.common.ExceptionUtils.notFoundExceptionForTableLikeEntity;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.polaris.core.auth.AuthorizationIntent;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.RenameAuthorizationIntent;
import org.apache.polaris.core.auth.SingleTargetAuthorizationIntent;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.service.types.PolicyIdentifier;
import org.immutables.value.Value;

/**
 * An ABC for catalog wrappers which provides authorize methods that should be called before a
 * request is actually forwarded to a catalog. Child types must implement `initializeCatalog` which
 * will be called after a successful authorization.
 */
public abstract class CatalogHandler {

  public abstract String catalogName();

  public abstract PolarisPrincipal polarisPrincipal();

  public abstract CallContext callContext();

  @Value.Derived
  public RealmConfig realmConfig() {
    return callContext().getRealmConfig();
  }

  @Value.Derived
  public RealmContext realmContext() {
    return callContext().getRealmContext();
  }

  public abstract PolarisMetaStoreManager metaStoreManager();

  public abstract ResolutionManifestFactory resolutionManifestFactory();

  public abstract PolarisAuthorizer authorizer();

  public abstract AuthorizationState authorizationState();

  protected PolarisResolutionManifest newResolutionManifest() {
    return resolutionManifestFactory().createResolutionManifest(polarisPrincipal(), catalogName());
  }

  // Initialized in the authorize methods.
  @SuppressWarnings("immutables:incompat")
  protected PolarisResolutionManifest resolutionManifest = null;

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
    resolutionManifest = newResolutionManifest();
    resolutionManifest.addPath(
        new ResolverPath(Arrays.asList(namespace.levels()), PolarisEntityType.NAMESPACE));

    if (extraPassthroughNamespaces != null) {
      for (Namespace ns : extraPassthroughNamespaces) {
        resolutionManifest.addPassthroughPath(
            new ResolverPath(
                Arrays.asList(ns.levels()), PolarisEntityType.NAMESPACE, true /* optional */));
      }
    }
    if (extraPassthroughTableLikes != null) {
      for (TableIdentifier id : extraPassthroughTableLikes) {
        resolutionManifest.addPassthroughPath(
            new ResolverPath(
                PolarisCatalogHelpers.tableIdentifierToList(id),
                PolarisEntityType.TABLE_LIKE,
                true /* optional */));
      }
    }

    if (extraPassThroughPolicies != null) {
      for (PolicyIdentifier id : extraPassThroughPolicies) {
        resolutionManifest.addPassthroughPath(
            new ResolverPath(
                PolarisCatalogHelpers.identifierToList(id.namespace(), id.name()),
                PolarisEntityType.POLICY,
                true /* optional */));
      }
    }

    authorizationState().setResolutionManifest(resolutionManifest);
    authorizer()
        .resolveAuthorizationInputs(
            authorizationState(),
            new AuthorizationRequest(
                polarisPrincipal(),
                List.of(
                    new SingleTargetAuthorizationIntent(
                        op,
                        namespace.isEmpty()
                            ? PolarisSecurableMapper.catalog(catalogName())
                            : PolarisSecurableMapper.namespace(catalogName(), namespace)))));
    PolarisResolvedPathWrapper target =
        resolutionManifest.getResolvedPath(ResolvedPathKey.ofNamespace(namespace), true);
    if (target == null) {
      throw noSuchNamespaceException(namespace);
    }
    authorizer()
        .authorizeOrThrow(
            polarisPrincipal(),
            resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
            op,
            target,
            null /* secondary */);

    initializeCatalog();
  }

  protected void authorizeCreateNamespaceUnderNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op, Namespace namespace) {
    resolutionManifest = newResolutionManifest();

    Namespace parentNamespace = PolarisCatalogHelpers.getParentNamespace(namespace);
    resolutionManifest.addPath(
        new ResolverPath(Arrays.asList(parentNamespace.levels()), PolarisEntityType.NAMESPACE));

    // When creating an entity under a namespace, the authz target is the parentNamespace, but we
    // must also add the actual path that will be created as an "optional" passthrough resolution
    // path to indicate that the underlying catalog is "allowed" to check the creation path for
    // a conflicting entity.
    resolutionManifest.addPassthroughPath(
        new ResolverPath(
            Arrays.asList(namespace.levels()), PolarisEntityType.NAMESPACE, true /* optional */));
    authorizationState().setResolutionManifest(resolutionManifest);
    authorizer()
        .resolveAuthorizationInputs(
            authorizationState(),
            new AuthorizationRequest(
                polarisPrincipal(),
                List.of(
                    new SingleTargetAuthorizationIntent(
                        op,
                        parentNamespace.isEmpty()
                            ? PolarisSecurableMapper.catalog(catalogName())
                            : PolarisSecurableMapper.namespace(catalogName(), parentNamespace)))));
    PolarisResolvedPathWrapper target =
        resolutionManifest.getResolvedPath(ResolvedPathKey.ofNamespace(parentNamespace), true);
    if (target == null) {
      throw noSuchNamespaceException(parentNamespace);
    }
    authorizer()
        .authorizeOrThrow(
            polarisPrincipal(),
            resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
            op,
            target,
            null /* secondary */);

    initializeCatalog();
  }

  protected void authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op, TableIdentifier identifier) {
    Namespace namespace = identifier.namespace();

    resolutionManifest = newResolutionManifest();
    resolutionManifest.addPath(
        new ResolverPath(Arrays.asList(namespace.levels()), PolarisEntityType.NAMESPACE));

    // When creating an entity under a namespace, the authz target is the namespace, but we must
    // also add the actual path that will be created as an "optional" passthrough resolution path to
    // indicate that the underlying catalog is "allowed" to check for a conflicting entity at the
    // creation path.
    resolutionManifest.addPassthroughPath(
        new ResolverPath(
            PolarisCatalogHelpers.tableIdentifierToList(identifier),
            PolarisEntityType.TABLE_LIKE,
            true /* optional */));
    authorizationState().setResolutionManifest(resolutionManifest);
    authorizer()
        .resolveAuthorizationInputs(
            authorizationState(),
            new AuthorizationRequest(
                polarisPrincipal(),
                List.of(
                    new SingleTargetAuthorizationIntent(
                        op, PolarisSecurableMapper.namespace(catalogName(), namespace)))));
    authorizeResolvedCreateTableLikeUnderNamespaceOperationOrThrow(op, identifier);
  }

  protected void authorizeResolvedCreateTableLikeUnderNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op, TableIdentifier identifier) {
    if (resolutionManifest == null) {
      throw new IllegalStateException(
          "Resolved authorization requires resolveAuthorizationInputs to run first");
    }

    Namespace namespace = identifier.namespace();
    PolarisResolvedPathWrapper target =
        resolutionManifest.getResolvedPath(ResolvedPathKey.ofNamespace(namespace), true);
    if (target == null) {
      throw noSuchNamespaceException(namespace);
    }

    authorizer()
        .authorizeOrThrow(
            polarisPrincipal(),
            resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
            op,
            target,
            null /* secondary */);

    initializeCatalog();
  }

  /**
   * Authorizes a register-table-with-overwrite operation. If the table already exists, {@code
   * overwriteOp} is authorized against the table entity. If the table does not exist, {@code
   * fallbackOp} is authorized against the parent namespace.
   */
  protected void authorizeRegisterTableOverwriteOrThrow(
      PolarisAuthorizableOperation overwriteOp,
      PolarisAuthorizableOperation fallbackOp,
      TableIdentifier identifier) {
    Namespace namespace = identifier.namespace();
    resolutionManifest = newResolutionManifest();
    resolutionManifest.addPath(
        new ResolverPath(Arrays.asList(namespace.levels()), PolarisEntityType.NAMESPACE));
    resolutionManifest.addPassthroughPath(
        new ResolverPath(
            PolarisCatalogHelpers.tableIdentifierToList(identifier),
            PolarisEntityType.TABLE_LIKE,
            true /* optional */));
    authorizationState().setResolutionManifest(resolutionManifest);
    authorizer()
        .resolveAuthorizationInputs(
            authorizationState(),
            new AuthorizationRequest(
                polarisPrincipal(),
                List.of(
                    new SingleTargetAuthorizationIntent(
                        overwriteOp,
                        PolarisSecurableMapper.tableLike(catalogName(), identifier)))));
    authorizeResolvedRegisterTableOverwriteOrThrow(overwriteOp, fallbackOp, identifier);
  }

  protected void authorizeResolvedRegisterTableOverwriteOrThrow(
      PolarisAuthorizableOperation overwriteOp,
      PolarisAuthorizableOperation fallbackOp,
      TableIdentifier identifier) {
    if (resolutionManifest == null) {
      throw new IllegalStateException(
          "Resolved authorization requires resolveAuthorizationInputs to run first");
    }

    Namespace namespace = identifier.namespace();

    // Early check so that a caller that has table-level REGISTER_TABLE_OVERWRITE but not
    // namespace-level REGISTER_TABLE doesn't get a permission error instead of
    // "View with same name already exists"
    PolarisEntitySubType leafSubType =
        resolutionManifest.getLeafSubType(ResolvedPathKey.ofTableLike(identifier));
    if (leafSubType == PolarisEntitySubType.ICEBERG_VIEW) {
      throw alreadyExistsExceptionWithSameNameForTableLikeEntity(
          identifier, PolarisEntitySubType.ICEBERG_VIEW);
    }

    PolarisResolvedPathWrapper tableTarget =
        resolutionManifest.getResolvedPath(
            ResolvedPathKey.ofTableLike(identifier), PolarisEntitySubType.ICEBERG_TABLE, true);

    if (tableTarget != null) {
      authorizer()
          .authorizeOrThrow(
              polarisPrincipal(),
              resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
              overwriteOp,
              tableTarget,
              null /* secondary */);
    } else {
      PolarisResolvedPathWrapper namespaceTarget =
          resolutionManifest.getResolvedPath(ResolvedPathKey.ofNamespace(namespace), true);
      if (namespaceTarget == null) {
        throw noSuchNamespaceException(namespace);
      }
      authorizer()
          .authorizeOrThrow(
              polarisPrincipal(),
              resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
              fallbackOp, // normal register-table operation
              namespaceTarget,
              null /* secondary */);
    }

    initializeCatalog();
  }

  /**
   * Ensures resolution manifest is initialized for a table identifier. This allows checking
   * catalog-level feature flags or other resolved entities before authorization. If already
   * initialized, this is a no-op.
   */
  protected void ensureResolutionManifestForTable(TableIdentifier identifier) {
    if (resolutionManifest == null) {
      resolutionManifest = newResolutionManifest();

      // The underlying Catalog is also allowed to fetch "fresh" versions of the target entity.
      resolutionManifest.addPassthroughPath(
          new ResolverPath(
              PolarisCatalogHelpers.tableIdentifierToList(identifier),
              PolarisEntityType.TABLE_LIKE,
              true /* optional */));
    }
  }

  protected void resolveBasicTableLikeTargetOrThrow(
      PolarisAuthorizableOperation op, TableIdentifier identifier) {
    ensureResolutionManifestForTable(identifier);

    authorizationState().setResolutionManifest(resolutionManifest);
    authorizer()
        .resolveAuthorizationInputs(
            authorizationState(),
            new AuthorizationRequest(
                polarisPrincipal(),
                List.of(
                    new SingleTargetAuthorizationIntent(
                        op, PolarisSecurableMapper.tableLike(catalogName(), identifier)))));
  }

  protected void authorizeResolvedBasicTableLikeOperationOrThrow(
      PolarisAuthorizableOperation op, PolarisEntitySubType subType, TableIdentifier identifier) {
    PolarisResolvedPathWrapper target =
        resolutionManifest.getResolvedPath(ResolvedPathKey.ofTableLike(identifier), subType, true);
    if (target == null) {
      throw notFoundExceptionForTableLikeEntity(identifier, subType);
    }

    authorizer()
        .authorizeOrThrow(
            polarisPrincipal(),
            resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
            op,
            target,
            null /* secondary */);

    initializeCatalog();
  }

  protected void resolveAndAuthorizeBasicTableLikeOperationOrThrow(
      PolarisAuthorizableOperation op, PolarisEntitySubType subType, TableIdentifier identifier) {
    resolveBasicTableLikeTargetOrThrow(op, identifier);
    authorizeResolvedBasicTableLikeOperationOrThrow(op, subType, identifier);
  }

  protected void authorizeBasicTableLikeOperationsOrThrow(
      EnumSet<PolarisAuthorizableOperation> ops,
      PolarisEntitySubType subType,
      TableIdentifier identifier) {
    ensureResolutionManifestForTable(identifier);
    checkArgument(!ops.isEmpty(), "No operations provided for table-like authorization");
    resolutionManifest.getPrimaryResolverStatusOrThrow();

    PolarisResolvedPathWrapper target =
        resolutionManifest.getResolvedPath(ResolvedPathKey.ofTableLike(identifier), subType, true);
    if (target == null) {
      throw notFoundExceptionForTableLikeEntity(identifier, subType);
    }

    for (PolarisAuthorizableOperation op : ops) {
      authorizer()
          .authorizeOrThrow(
              polarisPrincipal(),
              resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
              op,
              target,
              null /* secondary */);
    }

    initializeCatalog();
  }

  protected void authorizeCollectionOfTableLikeOperationOrThrow(
      PolarisAuthorizableOperation op,
      final PolarisEntitySubType subType,
      List<TableIdentifier> ids) {
    resolutionManifest = newResolutionManifest();
    ids.forEach(
        identifier ->
            resolutionManifest.addPassthroughPath(
                new ResolverPath(
                    PolarisCatalogHelpers.tableIdentifierToList(identifier),
                    PolarisEntityType.TABLE_LIKE)));

    authorizationState().setResolutionManifest(resolutionManifest);
    authorizer()
        .resolveAuthorizationInputs(
            authorizationState(),
            new AuthorizationRequest(
                polarisPrincipal(),
                ids.stream()
                    .map(identifier -> PolarisSecurableMapper.tableLike(catalogName(), identifier))
                    .<AuthorizationIntent>map(
                        target -> new SingleTargetAuthorizationIntent(op, target))
                    .toList()));
    ResolverStatus status = resolutionManifest.getPrimaryResolverStatusOrThrow();

    // If one of the paths failed to resolve, throw exception based on the one that
    // we first failed to resolve.
    if (status.getStatus() == ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED) {
      TableIdentifier identifier =
          PolarisCatalogHelpers.listToTableIdentifier(
              status.getFailedToResolvePath().entityNames());
      throw notFoundExceptionForTableLikeEntity(identifier, subType);
    }

    List<PolarisResolvedPathWrapper> targets =
        ids.stream()
            .map(
                identifier ->
                    Optional.ofNullable(
                            resolutionManifest.getResolvedPath(
                                ResolvedPathKey.ofTableLike(identifier), subType, true))
                        .orElseThrow(
                            () -> notFoundExceptionForTableLikeEntity(identifier, subType)))
            .toList();
    authorizer()
        .authorizeOrThrow(
            polarisPrincipal(),
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
    resolutionManifest = newResolutionManifest();
    // Add src, dstParent, and dst(optional)
    resolutionManifest.addPath(
        new ResolverPath(
            PolarisCatalogHelpers.tableIdentifierToList(src), PolarisEntityType.TABLE_LIKE));
    resolutionManifest.addPath(
        new ResolverPath(Arrays.asList(dst.namespace().levels()), PolarisEntityType.NAMESPACE));
    resolutionManifest.addPath(
        new ResolverPath(
            PolarisCatalogHelpers.tableIdentifierToList(dst),
            PolarisEntityType.TABLE_LIKE,
            true /* optional */));
    authorizationState().setResolutionManifest(resolutionManifest);
    authorizer()
        .resolveAuthorizationInputs(
            authorizationState(),
            new AuthorizationRequest(
                polarisPrincipal(),
                List.of(
                    new RenameAuthorizationIntent(
                        op,
                        PolarisSecurableMapper.tableLike(catalogName(), src),
                        PolarisSecurableMapper.tableLike(catalogName(), dst)))));
    ResolverStatus status = resolutionManifest.getPrimaryResolverStatusOrThrow();
    if (status.getStatus() == ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED
        && status.getFailedToResolvePath().lastEntityType() == PolarisEntityType.NAMESPACE) {
      throw noSuchNamespaceException(dst.namespace());
    } else if (resolutionManifest.getResolvedPath(ResolvedPathKey.ofTableLike(src), subType)
        == null) {
      throw notFoundExceptionForTableLikeEntity(dst, subType);
    }

    // Normally, since we added the dst as an optional path, we'd expect it to only get resolved
    // up to its parent namespace, and for there to be no TABLE_LIKE already in the dst in which
    // case the leafSubType will be NULL_SUBTYPE.
    // If there is a conflicting TABLE or VIEW, this leafSubType will indicate that conflicting
    // type.
    // TODO: Possibly modify the exception thrown depending on whether the caller has privileges
    // on the parent namespace.
    PolarisEntitySubType dstLeafSubType =
        resolutionManifest.getLeafSubType(ResolvedPathKey.ofTableLike(dst));

    switch (dstLeafSubType) {
      case ICEBERG_TABLE:
      case PolarisEntitySubType.ICEBERG_VIEW:
      case PolarisEntitySubType.GENERIC_TABLE:
        throw new AlreadyExistsException(
            "Cannot rename %s to %s. %s already exists",
            src, dst, entityNameForSubType(dstLeafSubType));

      default:
        break;
    }

    PolarisResolvedPathWrapper target =
        resolutionManifest.getResolvedPath(ResolvedPathKey.ofTableLike(src), subType, true);
    PolarisResolvedPathWrapper secondary =
        resolutionManifest.getResolvedPath(ResolvedPathKey.ofNamespace(dst.namespace()), true);
    authorizer()
        .authorizeOrThrow(
            polarisPrincipal(),
            resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
            op,
            target,
            secondary);

    initializeCatalog();
  }
}
