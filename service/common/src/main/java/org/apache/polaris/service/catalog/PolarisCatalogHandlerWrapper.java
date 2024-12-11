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
package org.apache.polaris.service.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.Closeable;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.polaris.core.PolarisConfiguration;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.TransactionWorkspaceMetaStoreManager;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.service.context.CallContextCatalogFactory;
import org.apache.polaris.service.types.NotificationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authorization-aware adapter between REST stubs and shared Iceberg SDK CatalogHandlers.
 *
 * <p>We must make authorization decisions based on entity resolution at this layer instead of the
 * underlying BasePolarisCatalog layer, because this REST-adjacent layer captures intent of
 * different REST calls that share underlying catalog calls (e.g. updateTable will call loadTable
 * under the hood), and some features of the REST API aren't expressed at all in the underlying
 * Catalog interfaces (e.g. credential-vending in createTable/loadTable).
 *
 * <p>We also want this layer to be independent of API-endpoint-specific idioms, such as dealing
 * with jakarta.ws.rs.core.Response objects, and other implementations that expose different HTTP
 * stubs or even tunnel the protocol over something like gRPC can still normalize on the Iceberg
 * model objects used in this layer to still benefit from the shared implementation of
 * authorization-aware catalog protocols.
 */
public class PolarisCatalogHandlerWrapper {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisCatalogHandlerWrapper.class);

  private final CallContext callContext;
  private final PolarisEntityManager entityManager;
  private final PolarisMetaStoreManager metaStoreManager;
  private final String catalogName;
  private final AuthenticatedPolarisPrincipal authenticatedPrincipal;
  private final PolarisAuthorizer authorizer;
  private final CallContextCatalogFactory catalogFactory;

  // Initialized in the authorize methods.
  private PolarisResolutionManifest resolutionManifest = null;

  // Catalog instance will be initialized after authorizing resolver successfully resolves
  // the catalog entity.
  private Catalog baseCatalog = null;
  private SupportsNamespaces namespaceCatalog = null;
  private ViewCatalog viewCatalog = null;

  public PolarisCatalogHandlerWrapper(
      CallContext callContext,
      PolarisEntityManager entityManager,
      PolarisMetaStoreManager metaStoreManager,
      AuthenticatedPolarisPrincipal authenticatedPrincipal,
      CallContextCatalogFactory catalogFactory,
      String catalogName,
      PolarisAuthorizer authorizer) {
    this.callContext = callContext;
    this.entityManager = entityManager;
    this.metaStoreManager = metaStoreManager;
    this.catalogName = catalogName;
    this.authenticatedPrincipal = authenticatedPrincipal;
    this.authorizer = authorizer;
    this.catalogFactory = catalogFactory;
  }

  /**
   * TODO: Make the helper in org.apache.iceberg.rest.CatalogHandlers public instead of needing to
   * copy/paste here.
   */
  public static boolean isCreate(UpdateTableRequest request) {
    boolean isCreate =
        request.requirements().stream()
            .anyMatch(UpdateRequirement.AssertTableDoesNotExist.class::isInstance);

    if (isCreate) {
      List<UpdateRequirement> invalidRequirements =
          request.requirements().stream()
              .filter(req -> !(req instanceof UpdateRequirement.AssertTableDoesNotExist))
              .collect(Collectors.toList());
      Preconditions.checkArgument(
          invalidRequirements.isEmpty(), "Invalid create requirements: %s", invalidRequirements);
    }

    return isCreate;
  }

  private void initializeCatalog() {
    this.baseCatalog =
        catalogFactory.createCallContextCatalog(
            callContext, authenticatedPrincipal, resolutionManifest);
    this.namespaceCatalog =
        (baseCatalog instanceof SupportsNamespaces) ? (SupportsNamespaces) baseCatalog : null;
    this.viewCatalog = (baseCatalog instanceof ViewCatalog) ? (ViewCatalog) baseCatalog : null;
  }

  private void authorizeBasicNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op, Namespace namespace) {
    authorizeBasicNamespaceOperationOrThrow(op, namespace, null, null);
  }

  private void authorizeBasicNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op,
      Namespace namespace,
      List<Namespace> extraPassthroughNamespaces,
      List<TableIdentifier> extraPassthroughTableLikes) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, authenticatedPrincipal, catalogName);
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
    resolutionManifest.resolveAll();
    PolarisResolvedPathWrapper target = resolutionManifest.getResolvedPath(namespace, true);
    if (target == null) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
    authorizer.authorizeOrThrow(
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        target,
        null /* secondary */);

    initializeCatalog();
  }

  private void authorizeCreateNamespaceUnderNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op, Namespace namespace) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, authenticatedPrincipal, catalogName);

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
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        target,
        null /* secondary */);

    initializeCatalog();
  }

  private void authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op, TableIdentifier identifier) {
    Namespace namespace = identifier.namespace();

    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, authenticatedPrincipal, catalogName);
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
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        target,
        null /* secondary */);

    initializeCatalog();
  }

  private void authorizeBasicTableLikeOperationOrThrow(
      PolarisAuthorizableOperation op, PolarisEntitySubType subType, TableIdentifier identifier) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, authenticatedPrincipal, catalogName);

    // The underlying Catalog is also allowed to fetch "fresh" versions of the target entity.
    resolutionManifest.addPassthroughPath(
        new ResolverPath(
            PolarisCatalogHelpers.tableIdentifierToList(identifier),
            PolarisEntityType.TABLE_LIKE,
            true /* optional */),
        identifier);
    resolutionManifest.resolveAll();
    PolarisResolvedPathWrapper target =
        resolutionManifest.getResolvedPath(identifier, subType, true);
    if (target == null) {
      if (subType == PolarisEntitySubType.TABLE) {
        throw new NoSuchTableException("Table does not exist: %s", identifier);
      } else {
        throw new NoSuchViewException("View does not exist: %s", identifier);
      }
    }
    authorizer.authorizeOrThrow(
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        target,
        null /* secondary */);

    initializeCatalog();
  }

  private void authorizeCollectionOfTableLikeOperationOrThrow(
      PolarisAuthorizableOperation op,
      final PolarisEntitySubType subType,
      List<TableIdentifier> ids) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, authenticatedPrincipal, catalogName);
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
      if (subType == PolarisEntitySubType.TABLE) {
        throw new NoSuchTableException("Table does not exist: %s", identifier);
      } else {
        throw new NoSuchViewException("View does not exist: %s", identifier);
      }
    }

    List<PolarisResolvedPathWrapper> targets =
        ids.stream()
            .map(
                identifier ->
                    Optional.ofNullable(
                            resolutionManifest.getResolvedPath(identifier, subType, true))
                        .orElseThrow(
                            () ->
                                subType == PolarisEntitySubType.TABLE
                                    ? new NoSuchTableException(
                                        "Table does not exist: %s", identifier)
                                    : new NoSuchViewException(
                                        "View does not exist: %s", identifier)))
            .toList();
    authorizer.authorizeOrThrow(
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        targets,
        null /* secondaries */);

    initializeCatalog();
  }

  private void authorizeRenameTableLikeOperationOrThrow(
      PolarisAuthorizableOperation op,
      PolarisEntitySubType subType,
      TableIdentifier src,
      TableIdentifier dst) {
    resolutionManifest =
        entityManager.prepareResolutionManifest(callContext, authenticatedPrincipal, catalogName);
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
    } else if (resolutionManifest.getResolvedPath(src, subType) == null) {
      if (subType == PolarisEntitySubType.TABLE) {
        throw new NoSuchTableException("Table does not exist: %s", src);
      } else {
        throw new NoSuchViewException("View does not exist: %s", src);
      }
    }

    // Normally, since we added the dst as an optional path, we'd expect it to only get resolved
    // up to its parent namespace, and for there to be no TABLE_LIKE already in the dst in which
    // case the leafSubType will be NULL_SUBTYPE.
    // If there is a conflicting TABLE or VIEW, this leafSubType will indicate that conflicting
    // type.
    // TODO: Possibly modify the exception thrown depending on whether the caller has privileges
    // on the parent namespace.
    PolarisEntitySubType dstLeafSubType = resolutionManifest.getLeafSubType(dst);
    if (dstLeafSubType == PolarisEntitySubType.TABLE) {
      throw new AlreadyExistsException("Cannot rename %s to %s. Table already exists", src, dst);
    } else if (dstLeafSubType == PolarisEntitySubType.VIEW) {
      throw new AlreadyExistsException("Cannot rename %s to %s. View already exists", src, dst);
    }

    PolarisResolvedPathWrapper target = resolutionManifest.getResolvedPath(src, subType, true);
    PolarisResolvedPathWrapper secondary =
        resolutionManifest.getResolvedPath(dst.namespace(), true);
    authorizer.authorizeOrThrow(
        authenticatedPrincipal,
        resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
        op,
        target,
        secondary);

    initializeCatalog();
  }

  public ListNamespacesResponse listNamespaces(Namespace parent) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_NAMESPACES;
    authorizeBasicNamespaceOperationOrThrow(op, parent);

    return doCatalogOperation(() -> CatalogHandlers.listNamespaces(namespaceCatalog, parent));
  }

  public CreateNamespaceResponse createNamespace(CreateNamespaceRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_NAMESPACE;

    Namespace namespace = request.namespace();
    if (namespace.isEmpty()) {
      throw new AlreadyExistsException(
          "Cannot create root namespace, as it already exists implicitly.");
    }
    authorizeCreateNamespaceUnderNamespaceOperationOrThrow(op, namespace);

    if (namespaceCatalog instanceof BasePolarisCatalog) {
      // Note: The CatalogHandlers' default implementation will non-atomically create the
      // namespace and then fetch its properties using loadNamespaceMetadata for the response.
      // However, the latest namespace metadata technically isn't the same authorized instance,
      // so we don't want all cals to loadNamespaceMetadata to automatically use the manifest
      // in "passthrough" mode.
      //
      // For CreateNamespace, we consider this a special case in that the creator is able to
      // retrieve the latest namespace metadata for the duration of the CreateNamespace
      // operation, even if the entityVersion and/or grantsVersion update in the interim.
      return doCatalogOperation(
          () -> {
            namespaceCatalog.createNamespace(namespace, request.properties());
            return CreateNamespaceResponse.builder()
                .withNamespace(namespace)
                .setProperties(
                    resolutionManifest
                        .getPassthroughResolvedPath(namespace)
                        .getRawLeafEntity()
                        .getPropertiesAsMap())
                .build();
          });
    } else {
      return doCatalogOperation(() -> CatalogHandlers.createNamespace(namespaceCatalog, request));
    }
  }

  private static boolean isExternal(CatalogEntity catalog) {
    return org.apache.polaris.core.admin.model.Catalog.TypeEnum.EXTERNAL.equals(
        catalog.getCatalogType());
  }

  private void doCatalogOperation(Runnable handler) {
    doCatalogOperation(
        () -> {
          handler.run();
          return null;
        });
  }

  /**
   * Execute a catalog function and ensure we close the BaseCatalog afterward. This will typically
   * ensure the underlying FileIO is closed
   */
  private <T> T doCatalogOperation(Supplier<T> handler) {
    try {
      return handler.get();
    } finally {
      if (baseCatalog instanceof Closeable closeable) {
        try {
          closeable.close();
        } catch (IOException e) {
          LOGGER.error("Error while closing BaseCatalog", e);
        }
      }
    }
  }

  public GetNamespaceResponse loadNamespaceMetadata(Namespace namespace) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LOAD_NAMESPACE_METADATA;
    authorizeBasicNamespaceOperationOrThrow(op, namespace);

    return doCatalogOperation(() -> CatalogHandlers.loadNamespace(namespaceCatalog, namespace));
  }

  public void namespaceExists(Namespace namespace) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.NAMESPACE_EXISTS;

    // TODO: This authz check doesn't accomplish true authz in terms of blocking the ability
    // for a caller to ascertain whether the namespace exists or not, but instead just behaves
    // according to convention -- if existence is going to be privileged, we must instead
    // add a base layer that throws NotFound exceptions instead of NotAuthorizedException
    // for *all* operations in which we determine that the basic privilege for determining
    // existence is also missing.
    authorizeBasicNamespaceOperationOrThrow(op, namespace);

    // TODO: Just skip CatalogHandlers for this one maybe
    doCatalogOperation(() -> CatalogHandlers.loadNamespace(namespaceCatalog, namespace));
  }

  public void dropNamespace(Namespace namespace) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_NAMESPACE;
    authorizeBasicNamespaceOperationOrThrow(op, namespace);

    doCatalogOperation(() -> CatalogHandlers.dropNamespace(namespaceCatalog, namespace));
  }

  public UpdateNamespacePropertiesResponse updateNamespaceProperties(
      Namespace namespace, UpdateNamespacePropertiesRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.UPDATE_NAMESPACE_PROPERTIES;
    authorizeBasicNamespaceOperationOrThrow(op, namespace);

    return doCatalogOperation(
        () -> CatalogHandlers.updateNamespaceProperties(namespaceCatalog, namespace, request));
  }

  public ListTablesResponse listTables(Namespace namespace) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_TABLES;
    authorizeBasicNamespaceOperationOrThrow(op, namespace);

    return doCatalogOperation(() -> CatalogHandlers.listTables(baseCatalog, namespace));
  }

  public LoadTableResponse createTableDirect(Namespace namespace, CreateTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_TABLE_DIRECT;
    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
        op, TableIdentifier.of(namespace, request.name()));

    CatalogEntity catalog =
        CatalogEntity.of(resolutionManifest.getResolvedReferenceCatalogEntity().getRawLeafEntity());
    if (isExternal(catalog)) {
      throw new BadRequestException("Cannot create table on external catalogs.");
    }
    return doCatalogOperation(() -> CatalogHandlers.createTable(baseCatalog, namespace, request));
  }

  public LoadTableResponse createTableDirectWithWriteDelegation(
      Namespace namespace, CreateTableRequest request) {
    PolarisAuthorizableOperation op =
        PolarisAuthorizableOperation.CREATE_TABLE_DIRECT_WITH_WRITE_DELEGATION;
    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
        op, TableIdentifier.of(namespace, request.name()));

    CatalogEntity catalog =
        CatalogEntity.of(resolutionManifest.getResolvedReferenceCatalogEntity().getRawLeafEntity());
    if (isExternal(catalog)) {
      throw new BadRequestException("Cannot create table on external catalogs.");
    }
    return doCatalogOperation(
        () -> {
          request.validate();

          TableIdentifier tableIdentifier = TableIdentifier.of(namespace, request.name());
          if (baseCatalog.tableExists(tableIdentifier)) {
            throw new AlreadyExistsException("Table already exists: %s", tableIdentifier);
          }

          Map<String, String> properties = Maps.newHashMap();
          properties.put("created-at", OffsetDateTime.now(ZoneOffset.UTC).toString());
          properties.putAll(request.properties());

          Table table =
              baseCatalog
                  .buildTable(tableIdentifier, request.schema())
                  .withLocation(request.location())
                  .withPartitionSpec(request.spec())
                  .withSortOrder(request.writeOrder())
                  .withProperties(properties)
                  .create();

          if (table instanceof BaseTable baseTable) {
            TableMetadata tableMetadata = baseTable.operations().current();
            LoadTableResponse.Builder responseBuilder =
                LoadTableResponse.builder().withTableMetadata(tableMetadata);
            if (baseCatalog instanceof SupportsCredentialDelegation credentialDelegation) {
              LOGGER
                  .atDebug()
                  .addKeyValue("tableIdentifier", tableIdentifier)
                  .addKeyValue("tableLocation", tableMetadata.location())
                  .log("Fetching client credentials for table");
              responseBuilder.addAllConfig(
                  credentialDelegation.getCredentialConfig(
                      tableIdentifier,
                      tableMetadata,
                      Set.of(
                          PolarisStorageActions.READ,
                          PolarisStorageActions.WRITE,
                          PolarisStorageActions.LIST)));
            }
            return responseBuilder.build();
          } else if (table instanceof BaseMetadataTable) {
            // metadata tables are loaded on the client side, return NoSuchTableException for now
            throw new NoSuchTableException("Table does not exist: %s", tableIdentifier.toString());
          }

          throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
        });
  }

  private TableMetadata stageTableCreateHelper(Namespace namespace, CreateTableRequest request) {
    request.validate();

    TableIdentifier ident = TableIdentifier.of(namespace, request.name());
    if (baseCatalog.tableExists(ident)) {
      throw new AlreadyExistsException("Table already exists: %s", ident);
    }

    Map<String, String> properties = Maps.newHashMap();
    properties.put("created-at", OffsetDateTime.now(ZoneOffset.UTC).toString());
    properties.putAll(request.properties());

    String location;
    if (request.location() != null) {
      // Even if the request provides a location, run it through the catalog's TableBuilder
      // to inherit any override behaviors if applicable.
      if (baseCatalog instanceof BasePolarisCatalog) {
        location =
            ((BasePolarisCatalog) baseCatalog).transformTableLikeLocation(request.location());
      } else {
        location = request.location();
      }
    } else {
      location =
          baseCatalog
              .buildTable(ident, request.schema())
              .withPartitionSpec(request.spec())
              .withSortOrder(request.writeOrder())
              .withProperties(properties)
              .createTransaction()
              .table()
              .location();
    }

    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            request.schema(),
            request.spec() != null ? request.spec() : PartitionSpec.unpartitioned(),
            request.writeOrder() != null ? request.writeOrder() : SortOrder.unsorted(),
            location,
            properties);
    return metadata;
  }

  public LoadTableResponse createTableStaged(Namespace namespace, CreateTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_TABLE_STAGED;
    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
        op, TableIdentifier.of(namespace, request.name()));

    CatalogEntity catalog =
        CatalogEntity.of(
            resolutionManifest.getResolvedReferenceCatalogEntity().getResolvedLeafEntity());
    if (isExternal(catalog)) {
      throw new BadRequestException("Cannot create table on external catalogs.");
    }
    return doCatalogOperation(
        () -> {
          TableMetadata metadata = stageTableCreateHelper(namespace, request);
          return LoadTableResponse.builder().withTableMetadata(metadata).build();
        });
  }

  public LoadTableResponse createTableStagedWithWriteDelegation(
      Namespace namespace, CreateTableRequest request) {
    PolarisAuthorizableOperation op =
        PolarisAuthorizableOperation.CREATE_TABLE_STAGED_WITH_WRITE_DELEGATION;
    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
        op, TableIdentifier.of(namespace, request.name()));

    CatalogEntity catalog =
        CatalogEntity.of(
            resolutionManifest.getResolvedReferenceCatalogEntity().getResolvedLeafEntity());
    if (isExternal(catalog)) {
      throw new BadRequestException("Cannot create table on external catalogs.");
    }
    return doCatalogOperation(
        () -> {
          TableIdentifier ident = TableIdentifier.of(namespace, request.name());
          TableMetadata metadata = stageTableCreateHelper(namespace, request);

          LoadTableResponse.Builder responseBuilder =
              LoadTableResponse.builder().withTableMetadata(metadata);

          if (baseCatalog instanceof SupportsCredentialDelegation credentialDelegation) {
            LOGGER
                .atDebug()
                .addKeyValue("tableIdentifier", ident)
                .addKeyValue("tableLocation", metadata.location())
                .log("Fetching client credentials for table");
            responseBuilder.addAllConfig(
                credentialDelegation.getCredentialConfig(
                    ident, metadata, Set.of(PolarisStorageActions.ALL)));
          }
          return responseBuilder.build();
        });
  }

  public LoadTableResponse registerTable(Namespace namespace, RegisterTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.REGISTER_TABLE;
    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
        op, TableIdentifier.of(namespace, request.name()));

    return doCatalogOperation(() -> CatalogHandlers.registerTable(baseCatalog, namespace, request));
  }

  public boolean sendNotification(TableIdentifier identifier, NotificationRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.SEND_NOTIFICATIONS;

    // For now, just require the full set of privileges on the base Catalog entity, which we can
    // also express just as the "root" Namespace for purposes of the BasePolarisCatalog being
    // able to fetch Namespace.empty() as path key.
    List<TableIdentifier> extraPassthroughTableLikes = List.of(identifier);
    List<Namespace> extraPassthroughNamespaces = new ArrayList<>();
    extraPassthroughNamespaces.add(Namespace.empty());
    for (int i = 1; i <= identifier.namespace().length(); i++) {
      Namespace nsLevel =
          Namespace.of(
              Arrays.stream(identifier.namespace().levels()).limit(i).toArray(String[]::new));
      extraPassthroughNamespaces.add(nsLevel);
    }
    authorizeBasicNamespaceOperationOrThrow(
        op, Namespace.empty(), extraPassthroughNamespaces, extraPassthroughTableLikes);

    CatalogEntity catalog =
        CatalogEntity.of(
            resolutionManifest.getResolvedReferenceCatalogEntity().getResolvedLeafEntity());
    if (catalog
        .getCatalogType()
        .equals(org.apache.polaris.core.admin.model.Catalog.TypeEnum.INTERNAL)) {
      LOGGER
          .atWarn()
          .addKeyValue("catalog", catalog)
          .addKeyValue("notification", request)
          .log("Attempted notification on internal catalog");
      throw new BadRequestException("Cannot update internal catalog via notifications");
    }
    return baseCatalog instanceof SupportsNotifications notificationCatalog
        && notificationCatalog.sendNotification(identifier, request);
  }

  public LoadTableResponse loadTable(TableIdentifier tableIdentifier, String snapshots) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LOAD_TABLE;
    authorizeBasicTableLikeOperationOrThrow(op, PolarisEntitySubType.TABLE, tableIdentifier);

    return doCatalogOperation(() -> CatalogHandlers.loadTable(baseCatalog, tableIdentifier));
  }

  public LoadTableResponse loadTableWithAccessDelegation(
      TableIdentifier tableIdentifier, String snapshots) {
    // Here we have a single method that falls through multiple candidate
    // PolarisAuthorizableOperations because instead of identifying the desired operation up-front
    // and
    // failing the authz check if grants aren't found, we find the first most-privileged authz match
    // and respond according to that.
    PolarisAuthorizableOperation read =
        PolarisAuthorizableOperation.LOAD_TABLE_WITH_READ_DELEGATION;
    PolarisAuthorizableOperation write =
        PolarisAuthorizableOperation.LOAD_TABLE_WITH_WRITE_DELEGATION;

    Set<PolarisStorageActions> actionsRequested =
        new HashSet<>(Set.of(PolarisStorageActions.READ, PolarisStorageActions.LIST));
    try {
      // TODO: Refactor to have a boolean-return version of the helpers so we can fallthrough
      // easily.
      authorizeBasicTableLikeOperationOrThrow(write, PolarisEntitySubType.TABLE, tableIdentifier);
      actionsRequested.add(PolarisStorageActions.WRITE);
    } catch (ForbiddenException e) {
      authorizeBasicTableLikeOperationOrThrow(read, PolarisEntitySubType.TABLE, tableIdentifier);
    }

    PolarisResolvedPathWrapper catalogPath = resolutionManifest.getResolvedReferenceCatalogEntity();
    callContext
        .getPolarisCallContext()
        .getDiagServices()
        .checkNotNull(catalogPath, "No catalog available for loadTable request");
    CatalogEntity catalogEntity = CatalogEntity.of(catalogPath.getRawLeafEntity());
    PolarisConfigurationStore configurationStore =
        callContext.getPolarisCallContext().getConfigurationStore();
    if (catalogEntity
            .getCatalogType()
            .equals(org.apache.polaris.core.admin.model.Catalog.TypeEnum.EXTERNAL)
        && !configurationStore.getConfiguration(
            callContext.getPolarisCallContext(),
            catalogEntity,
            PolarisConfiguration.ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING)) {
      throw new ForbiddenException(
          "Access Delegation is not enabled for this catalog. Please consult applicable "
              + "documentation for the catalog config property '%s' to enable this feature",
          PolarisConfiguration.ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING.catalogConfig());
    }

    // TODO: Find a way for the configuration or caller to better express whether to fail or omit
    // when data-access is specified but access delegation grants are not found.
    return doCatalogOperation(
        () -> {
          Table table = baseCatalog.loadTable(tableIdentifier);

          if (table instanceof BaseTable baseTable) {
            TableMetadata tableMetadata = baseTable.operations().current();
            LoadTableResponse.Builder responseBuilder =
                LoadTableResponse.builder().withTableMetadata(tableMetadata);
            if (baseCatalog instanceof SupportsCredentialDelegation credentialDelegation) {
              LOGGER
                  .atDebug()
                  .addKeyValue("tableIdentifier", tableIdentifier)
                  .addKeyValue("tableLocation", tableMetadata.location())
                  .log("Fetching client credentials for table");
              responseBuilder.addAllConfig(
                  credentialDelegation.getCredentialConfig(
                      tableIdentifier, tableMetadata, actionsRequested));
            }
            return responseBuilder.build();
          } else if (table instanceof BaseMetadataTable) {
            // metadata tables are loaded on the client side, return NoSuchTableException for now
            throw new NoSuchTableException("Table does not exist: %s", tableIdentifier.toString());
          }

          throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
        });
  }

  private UpdateTableRequest applyUpdateFilters(UpdateTableRequest request) {
    // Certain MetadataUpdates need to be explicitly transformed to achieve the same behavior
    // as using a local Catalog client via TableBuilder.
    TableIdentifier identifier = request.identifier();
    List<UpdateRequirement> requirements = request.requirements();
    List<MetadataUpdate> updates =
        request.updates().stream()
            .map(
                update -> {
                  if (baseCatalog instanceof BasePolarisCatalog
                      && update instanceof MetadataUpdate.SetLocation) {
                    String requestedLocation = ((MetadataUpdate.SetLocation) update).location();
                    String filteredLocation =
                        ((BasePolarisCatalog) baseCatalog)
                            .transformTableLikeLocation(requestedLocation);
                    return new MetadataUpdate.SetLocation(filteredLocation);
                  } else {
                    return update;
                  }
                })
            .toList();
    return UpdateTableRequest.create(identifier, requirements, updates);
  }

  public LoadTableResponse updateTable(
      TableIdentifier tableIdentifier, UpdateTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.UPDATE_TABLE;
    authorizeBasicTableLikeOperationOrThrow(op, PolarisEntitySubType.TABLE, tableIdentifier);

    CatalogEntity catalog =
        CatalogEntity.of(
            resolutionManifest.getResolvedReferenceCatalogEntity().getResolvedLeafEntity());
    if (isExternal(catalog)) {
      throw new BadRequestException("Cannot update table on external catalogs.");
    }
    return doCatalogOperation(
        () ->
            CatalogHandlers.updateTable(baseCatalog, tableIdentifier, applyUpdateFilters(request)));
  }

  public LoadTableResponse updateTableForStagedCreate(
      TableIdentifier tableIdentifier, UpdateTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.UPDATE_TABLE_FOR_STAGED_CREATE;
    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(op, tableIdentifier);

    CatalogEntity catalog =
        CatalogEntity.of(
            resolutionManifest.getResolvedReferenceCatalogEntity().getResolvedLeafEntity());
    if (isExternal(catalog)) {
      throw new BadRequestException("Cannot update table on external catalogs.");
    }
    return doCatalogOperation(
        () ->
            CatalogHandlers.updateTable(baseCatalog, tableIdentifier, applyUpdateFilters(request)));
  }

  public void dropTableWithoutPurge(TableIdentifier tableIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_TABLE_WITHOUT_PURGE;
    authorizeBasicTableLikeOperationOrThrow(op, PolarisEntitySubType.TABLE, tableIdentifier);

    doCatalogOperation(() -> CatalogHandlers.dropTable(baseCatalog, tableIdentifier));
  }

  public void dropTableWithPurge(TableIdentifier tableIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_TABLE_WITH_PURGE;
    authorizeBasicTableLikeOperationOrThrow(op, PolarisEntitySubType.TABLE, tableIdentifier);

    CatalogEntity catalog =
        CatalogEntity.of(
            resolutionManifest.getResolvedReferenceCatalogEntity().getResolvedLeafEntity());
    if (isExternal(catalog)) {
      throw new BadRequestException("Cannot drop table on external catalogs.");
    }
    doCatalogOperation(() -> CatalogHandlers.purgeTable(baseCatalog, tableIdentifier));
  }

  public void tableExists(TableIdentifier tableIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.TABLE_EXISTS;
    authorizeBasicTableLikeOperationOrThrow(op, PolarisEntitySubType.TABLE, tableIdentifier);

    // TODO: Just skip CatalogHandlers for this one maybe
    doCatalogOperation(() -> CatalogHandlers.loadTable(baseCatalog, tableIdentifier));
  }

  public void renameTable(RenameTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.RENAME_TABLE;
    authorizeRenameTableLikeOperationOrThrow(
        op, PolarisEntitySubType.TABLE, request.source(), request.destination());

    CatalogEntity catalog =
        CatalogEntity.of(
            resolutionManifest.getResolvedReferenceCatalogEntity().getResolvedLeafEntity());
    if (isExternal(catalog)) {
      throw new BadRequestException("Cannot rename table on external catalogs.");
    }
    doCatalogOperation(() -> CatalogHandlers.renameTable(baseCatalog, request));
  }

  public void commitTransaction(CommitTransactionRequest commitTransactionRequest) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.COMMIT_TRANSACTION;
    // TODO: The authz actually needs to detect hidden updateForStagedCreate UpdateTableRequests
    // and have some kind of per-item conditional privilege requirement if we want to make it
    // so that only the stageCreate updates need TABLE_CREATE whereas everything else only
    // needs TABLE_WRITE_PROPERTIES.
    authorizeCollectionOfTableLikeOperationOrThrow(
        op,
        PolarisEntitySubType.TABLE,
        commitTransactionRequest.tableChanges().stream()
            .map(UpdateTableRequest::identifier)
            .toList());
    CatalogEntity catalog =
        CatalogEntity.of(
            resolutionManifest.getResolvedReferenceCatalogEntity().getResolvedLeafEntity());
    if (isExternal(catalog)) {
      throw new BadRequestException("Cannot update table on external catalogs.");
    }

    if (!(baseCatalog instanceof BasePolarisCatalog)) {
      throw new BadRequestException(
          "Unsupported operation: commitTransaction with baseCatalog type: %s",
          baseCatalog.getClass().getName());
    }

    // Swap in TransactionWorkspaceMetaStoreManager for all mutations made by this baseCatalog to
    // only go into an in-memory collection that we can commit as a single atomic unit after all
    // validations.
    TransactionWorkspaceMetaStoreManager transactionMetaStoreManager =
        new TransactionWorkspaceMetaStoreManager(metaStoreManager);
    ((BasePolarisCatalog) baseCatalog).setMetaStoreManager(transactionMetaStoreManager);

    commitTransactionRequest.tableChanges().stream()
        .forEach(
            change -> {
              Table table = baseCatalog.loadTable(change.identifier());
              if (!(table instanceof BaseTable)) {
                throw new IllegalStateException(
                    "Cannot wrap catalog that does not produce BaseTable");
              }
              if (isCreate(change)) {
                throw new BadRequestException(
                    "Unsupported operation: commitTranaction with updateForStagedCreate: %s",
                    change);
              }

              TableOperations tableOps = ((BaseTable) table).operations();
              TableMetadata currentMetadata = tableOps.current();

              // Validate requirements; any CommitFailedExceptions will fail the overall request
              change.requirements().forEach(requirement -> requirement.validate(currentMetadata));

              // Apply changes
              TableMetadata.Builder metadataBuilder = TableMetadata.buildFrom(currentMetadata);
              change.updates().stream()
                  .forEach(
                      singleUpdate -> {
                        // Note: If location-overlap checking is refactored to be atomic, we could
                        // support validation within a single multi-table transaction as well, but
                        // will need to update the TransactionWorkspaceMetaStoreManager to better
                        // expose the concept of being able to read uncommitted updates.
                        if (singleUpdate instanceof MetadataUpdate.SetLocation) {
                          if (!currentMetadata
                                  .location()
                                  .equals(((MetadataUpdate.SetLocation) singleUpdate).location())
                              && !callContext
                                  .getPolarisCallContext()
                                  .getConfigurationStore()
                                  .getConfiguration(
                                      callContext.getPolarisCallContext(),
                                      PolarisConfiguration.ALLOW_NAMESPACE_LOCATION_OVERLAP)) {
                            throw new BadRequestException(
                                "Unsupported operation: commitTransaction containing SetLocation"
                                    + " for table '%s' and new location '%s'",
                                change.identifier(),
                                ((MetadataUpdate.SetLocation) singleUpdate).location());
                          }
                        }

                        // Apply updates to builder
                        singleUpdate.applyTo(metadataBuilder);
                      });

              // Commit into transaction workspace we swapped the baseCatalog to use
              TableMetadata updatedMetadata = metadataBuilder.build();
              if (!updatedMetadata.changes().isEmpty()) {
                tableOps.commit(currentMetadata, updatedMetadata);
              }
            });

    // Commit the collected updates in a single atomic operation
    List<PolarisMetaStoreManager.EntityWithPath> pendingUpdates =
        transactionMetaStoreManager.getPendingUpdates();
    PolarisMetaStoreManager.EntitiesResult result =
        metaStoreManager.updateEntitiesPropertiesIfNotChanged(
            callContext.getPolarisCallContext(), pendingUpdates);
    if (!result.isSuccess()) {
      // TODO: Retries and server-side cleanup on failure
      throw new CommitFailedException(
          "Transaction commit failed with status: %s, extraInfo: %s",
          result.getReturnStatus(), result.getExtraInformation());
    }
  }

  public ListTablesResponse listViews(Namespace namespace) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_VIEWS;
    authorizeBasicNamespaceOperationOrThrow(op, namespace);

    return doCatalogOperation(() -> CatalogHandlers.listViews(viewCatalog, namespace));
  }

  public LoadViewResponse createView(Namespace namespace, CreateViewRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_VIEW;
    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
        op, TableIdentifier.of(namespace, request.name()));

    CatalogEntity catalog =
        CatalogEntity.of(
            resolutionManifest.getResolvedReferenceCatalogEntity().getResolvedLeafEntity());
    if (isExternal(catalog)) {
      throw new BadRequestException("Cannot create view on external catalogs.");
    }
    return doCatalogOperation(() -> CatalogHandlers.createView(viewCatalog, namespace, request));
  }

  public LoadViewResponse loadView(TableIdentifier viewIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LOAD_VIEW;
    authorizeBasicTableLikeOperationOrThrow(op, PolarisEntitySubType.VIEW, viewIdentifier);

    return doCatalogOperation(() -> CatalogHandlers.loadView(viewCatalog, viewIdentifier));
  }

  public LoadViewResponse replaceView(TableIdentifier viewIdentifier, UpdateTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.REPLACE_VIEW;
    authorizeBasicTableLikeOperationOrThrow(op, PolarisEntitySubType.VIEW, viewIdentifier);

    CatalogEntity catalog =
        CatalogEntity.of(
            resolutionManifest.getResolvedReferenceCatalogEntity().getResolvedLeafEntity());
    if (isExternal(catalog)) {
      throw new BadRequestException("Cannot replace view on external catalogs.");
    }
    return doCatalogOperation(
        () -> CatalogHandlers.updateView(viewCatalog, viewIdentifier, applyUpdateFilters(request)));
  }

  public void dropView(TableIdentifier viewIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_VIEW;
    authorizeBasicTableLikeOperationOrThrow(op, PolarisEntitySubType.VIEW, viewIdentifier);

    doCatalogOperation(() -> CatalogHandlers.dropView(viewCatalog, viewIdentifier));
  }

  public void viewExists(TableIdentifier viewIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.VIEW_EXISTS;
    authorizeBasicTableLikeOperationOrThrow(op, PolarisEntitySubType.VIEW, viewIdentifier);

    // TODO: Just skip CatalogHandlers for this one maybe
    doCatalogOperation(() -> CatalogHandlers.loadView(viewCatalog, viewIdentifier));
  }

  public void renameView(RenameTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.RENAME_VIEW;
    authorizeRenameTableLikeOperationOrThrow(
        op, PolarisEntitySubType.VIEW, request.source(), request.destination());

    CatalogEntity catalog =
        CatalogEntity.of(
            resolutionManifest.getResolvedReferenceCatalogEntity().getResolvedLeafEntity());
    if (isExternal(catalog)) {
      throw new BadRequestException("Cannot rename view on external catalogs.");
    }
    doCatalogOperation(() -> CatalogHandlers.renameView(viewCatalog, request));
  }
}
