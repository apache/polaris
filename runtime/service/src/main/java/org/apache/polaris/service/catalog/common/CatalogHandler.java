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

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.polaris.core.auth.AuthorizationCallContext;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisSecurable;
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

  protected PolarisResolutionManifest newResolutionManifest() {
    return resolutionManifestFactory().createResolutionManifest(polarisPrincipal(), catalogName());
  }

  private AuthorizationRequest newAuthorizationRequest(PolarisAuthorizableOperation op) {
    return new AuthorizationRequest(polarisPrincipal(), op, null, null);
  }

  private AuthorizationRequest newAuthorizationRequest(
      PolarisAuthorizableOperation op,
      List<PolarisSecurable> targets,
      List<PolarisSecurable> secondaries) {
    return new AuthorizationRequest(polarisPrincipal(), op, targets, secondaries);
  }

  protected PolarisSecurable newSecurable(PolarisEntityType type, List<String> nameParts) {
    return new PolarisSecurable(type, nameParts);
  }

  protected PolarisSecurable newNamespaceSecurable(Namespace namespace) {
    return newSecurable(PolarisEntityType.NAMESPACE, Arrays.asList(namespace.levels()));
  }

  protected PolarisSecurable newTableLikeSecurable(TableIdentifier identifier) {
    return newSecurable(
        PolarisEntityType.TABLE_LIKE, PolarisCatalogHelpers.tableIdentifierToList(identifier));
  }

  protected PolarisSecurable newPolicySecurable(PolicyIdentifier identifier) {
    return newSecurable(
        PolarisEntityType.POLICY,
        PolarisCatalogHelpers.identifierToList(identifier.getNamespace(), identifier.getName()));
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
    PolarisSecurable namespaceSecurable = newNamespaceSecurable(namespace);
    resolutionManifest.addPath(
        new ResolverPath(namespaceSecurable.getNameParts(), PolarisEntityType.NAMESPACE),
        namespaceSecurable);
    resolutionManifest.addPathAlias(namespaceSecurable, namespace);

    if (extraPassthroughNamespaces != null) {
      for (Namespace ns : extraPassthroughNamespaces) {
        PolarisSecurable nsSecurable = newNamespaceSecurable(ns);
        resolutionManifest.addPassthroughPath(
            new ResolverPath(
                Arrays.asList(ns.levels()), PolarisEntityType.NAMESPACE, true /* optional */),
            nsSecurable);
        resolutionManifest.addPassthroughAlias(nsSecurable, ns);
      }
    }
    if (extraPassthroughTableLikes != null) {
      for (TableIdentifier id : extraPassthroughTableLikes) {
        PolarisSecurable tableSecurable = newTableLikeSecurable(id);
        resolutionManifest.addPassthroughPath(
            new ResolverPath(
                tableSecurable.getNameParts(), PolarisEntityType.TABLE_LIKE, true /* optional */),
            tableSecurable);
        resolutionManifest.addPassthroughAlias(tableSecurable, id);
      }
    }

    if (extraPassThroughPolicies != null) {
      for (PolicyIdentifier id : extraPassThroughPolicies) {
        PolarisSecurable policySecurable = newPolicySecurable(id);
        resolutionManifest.addPassthroughPath(
            new ResolverPath(
                policySecurable.getNameParts(), PolarisEntityType.POLICY, true /* optional */),
            policySecurable);
        resolutionManifest.addPassthroughAlias(policySecurable, id);
      }
    }

    AuthorizationCallContext authzContext = new AuthorizationCallContext(resolutionManifest);
    authorizer().preAuthorize(authzContext, newAuthorizationRequest(op));
    PolarisResolvedPathWrapper target =
        resolutionManifest.getResolvedPath(namespaceSecurable, true);
    if (target == null) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
    authorizer()
        .authorize(authzContext, newAuthorizationRequest(op, List.of(namespaceSecurable), null));

    initializeCatalog();
  }

  protected void authorizeCreateNamespaceUnderNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op, Namespace namespace) {
    resolutionManifest = newResolutionManifest();

    Namespace parentNamespace = PolarisCatalogHelpers.getParentNamespace(namespace);
    PolarisSecurable parentNamespaceSecurable = newNamespaceSecurable(parentNamespace);
    resolutionManifest.addPath(
        new ResolverPath(parentNamespaceSecurable.getNameParts(), PolarisEntityType.NAMESPACE),
        parentNamespaceSecurable);
    resolutionManifest.addPathAlias(parentNamespaceSecurable, parentNamespace);

    // When creating an entity under a namespace, the authz target is the parentNamespace, but we
    // must also add the actual path that will be created as an "optional" passthrough resolution
    // path to indicate that the underlying catalog is "allowed" to check the creation path for
    // a conflicting entity.
    resolutionManifest.addPassthroughPath(
        new ResolverPath(
            Arrays.asList(namespace.levels()), PolarisEntityType.NAMESPACE, true /* optional */),
        newNamespaceSecurable(namespace));
    resolutionManifest.addPassthroughAlias(newNamespaceSecurable(namespace), namespace);
    AuthorizationCallContext authzContext = new AuthorizationCallContext(resolutionManifest);
    authorizer().preAuthorize(authzContext, newAuthorizationRequest(op));
    PolarisResolvedPathWrapper target =
        resolutionManifest.getResolvedPath(parentNamespaceSecurable, true);
    if (target == null) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", parentNamespace);
    }
    authorizer()
        .authorize(
            authzContext, newAuthorizationRequest(op, List.of(parentNamespaceSecurable), null));

    initializeCatalog();
  }

  protected void authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op, TableIdentifier identifier) {
    Namespace namespace = identifier.namespace();

    resolutionManifest = newResolutionManifest();
    PolarisSecurable namespaceSecurable = newNamespaceSecurable(namespace);
    resolutionManifest.addPath(
        new ResolverPath(namespaceSecurable.getNameParts(), PolarisEntityType.NAMESPACE),
        namespaceSecurable);
    resolutionManifest.addPathAlias(namespaceSecurable, namespace);

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
        newTableLikeSecurable(identifier));
    resolutionManifest.addPassthroughAlias(newTableLikeSecurable(identifier), identifier);
    AuthorizationCallContext authzContext = new AuthorizationCallContext(resolutionManifest);
    authorizer().preAuthorize(authzContext, newAuthorizationRequest(op));
    PolarisResolvedPathWrapper target =
        resolutionManifest.getResolvedPath(namespaceSecurable, true);
    if (target == null) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
    authorizer()
        .authorize(authzContext, newAuthorizationRequest(op, List.of(namespaceSecurable), null));

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

      PolarisSecurable namespaceSecurable = newNamespaceSecurable(identifier.namespace());
      resolutionManifest.addPassthroughPath(
          new ResolverPath(namespaceSecurable.getNameParts(), PolarisEntityType.NAMESPACE),
          namespaceSecurable);
      resolutionManifest.addPassthroughAlias(namespaceSecurable, identifier.namespace());

      // The underlying Catalog is also allowed to fetch "fresh" versions of the target entity.
      PolarisSecurable tableSecurable = newTableLikeSecurable(identifier);
      resolutionManifest.addPassthroughPath(
          new ResolverPath(
              tableSecurable.getNameParts(), PolarisEntityType.TABLE_LIKE, true /* optional */),
          tableSecurable);
      resolutionManifest.addPassthroughAlias(tableSecurable, identifier);
      AuthorizationCallContext authzContext = new AuthorizationCallContext(resolutionManifest);
      authorizer()
          .preAuthorize(
              authzContext, newAuthorizationRequest(PolarisAuthorizableOperation.LOAD_TABLE));
    }
  }

  protected void authorizeBasicTableLikeOperationOrThrow(
      PolarisAuthorizableOperation op, PolarisEntitySubType subType, TableIdentifier identifier) {
    authorizeBasicTableLikeOperationsOrThrow(EnumSet.of(op), subType, identifier);
  }

  protected void authorizeBasicTableLikeOperationsOrThrow(
      EnumSet<PolarisAuthorizableOperation> ops,
      PolarisEntitySubType subType,
      TableIdentifier identifier) {
    ensureResolutionManifestForTable(identifier);
    AuthorizationCallContext authzContext = new AuthorizationCallContext(resolutionManifest);
    PolarisAuthorizableOperation primaryOp = ops.iterator().next();
    authorizer().preAuthorize(authzContext, newAuthorizationRequest(primaryOp));
    PolarisSecurable targetSecurable = newTableLikeSecurable(identifier);
    PolarisResolvedPathWrapper target =
        resolutionManifest.getResolvedPath(
            targetSecurable, PolarisEntityType.TABLE_LIKE, subType, true);
    if (target == null) {
      throwNotFoundExceptionForTableLikeEntity(identifier, List.of(subType));
    }

    for (PolarisAuthorizableOperation op : ops) {
      authorizer()
          .authorize(authzContext, newAuthorizationRequest(op, List.of(targetSecurable), null));
    }

    initializeCatalog();
  }

  protected void authorizeCollectionOfTableLikeOperationOrThrow(
      PolarisAuthorizableOperation op,
      final PolarisEntitySubType subType,
      List<TableIdentifier> ids) {
    resolutionManifest = newResolutionManifest();
    ids.forEach(
        identifier -> {
          PolarisSecurable tableSecurable = newTableLikeSecurable(identifier);
          resolutionManifest.addPassthroughPath(
              new ResolverPath(
                  PolarisCatalogHelpers.tableIdentifierToList(identifier),
                  PolarisEntityType.TABLE_LIKE),
              tableSecurable);
          resolutionManifest.addPassthroughAlias(tableSecurable, identifier);
        });

    AuthorizationCallContext authzContext = new AuthorizationCallContext(resolutionManifest);
    authorizer().preAuthorize(authzContext, newAuthorizationRequest(op));
    ResolverStatus status = resolutionManifest.getResolverStatus();

    // If one of the paths failed to resolve, throw exception based on the one that
    // we first failed to resolve.
    if (status.getStatus() == ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED) {
      TableIdentifier identifier =
          PolarisCatalogHelpers.listToTableIdentifier(
              status.getFailedToResolvePath().getEntityNames());
      throwNotFoundExceptionForTableLikeEntity(identifier, List.of(subType));
    }

    List<PolarisSecurable> targets =
        ids.stream()
            .map(
                identifier -> {
                  PolarisSecurable securable = newTableLikeSecurable(identifier);
                  if (resolutionManifest.getResolvedPath(
                          securable, PolarisEntityType.TABLE_LIKE, subType, true)
                      == null) {
                    throw subType == ICEBERG_TABLE
                        ? new NoSuchTableException("Table does not exist: %s", identifier)
                        : new NoSuchViewException("View does not exist: %s", identifier);
                  }
                  return securable;
                })
            .toList();
    authorizer().authorize(authzContext, newAuthorizationRequest(op, targets, null));

    initializeCatalog();
  }

  protected void authorizeRenameTableLikeOperationOrThrow(
      PolarisAuthorizableOperation op,
      PolarisEntitySubType subType,
      TableIdentifier src,
      TableIdentifier dst) {
    resolutionManifest = newResolutionManifest();
    // Add src, dstParent, and dst(optional)
    PolarisSecurable srcSecurable = newTableLikeSecurable(src);
    resolutionManifest.addPath(
        new ResolverPath(
            PolarisCatalogHelpers.tableIdentifierToList(src), PolarisEntityType.TABLE_LIKE),
        srcSecurable);
    resolutionManifest.addPathAlias(srcSecurable, src);
    PolarisSecurable dstNamespaceSecurable = newNamespaceSecurable(dst.namespace());
    resolutionManifest.addPath(
        new ResolverPath(dstNamespaceSecurable.getNameParts(), PolarisEntityType.NAMESPACE),
        dstNamespaceSecurable);
    resolutionManifest.addPathAlias(dstNamespaceSecurable, dst.namespace());
    PolarisSecurable dstSecurable = newTableLikeSecurable(dst);
    resolutionManifest.addPath(
        new ResolverPath(
            PolarisCatalogHelpers.tableIdentifierToList(dst),
            PolarisEntityType.TABLE_LIKE,
            true /* optional */),
        dstSecurable);
    resolutionManifest.addPathAlias(dstSecurable, dst);
    AuthorizationCallContext authzContext = new AuthorizationCallContext(resolutionManifest);
    authorizer().preAuthorize(authzContext, newAuthorizationRequest(op));
    ResolverStatus status = resolutionManifest.getResolverStatus();
    if (status.getStatus() == ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED
        && status.getFailedToResolvePath().getLastEntityType() == PolarisEntityType.NAMESPACE) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", dst.namespace());
    } else if (resolutionManifest.getResolvedPath(
            srcSecurable, PolarisEntityType.TABLE_LIKE, subType)
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
    PolarisEntitySubType dstLeafSubType = resolutionManifest.getLeafSubType(dstSecurable);

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

    authorizer()
        .authorize(
            authzContext,
            newAuthorizationRequest(op, List.of(srcSecurable), List.of(dstNamespaceSecurable)));

    initializeCatalog();
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
