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
package org.apache.polaris.service.catalog.iceberg;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import jakarta.ws.rs.core.SecurityContext;
import java.io.Closeable;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
import org.apache.iceberg.exceptions.NoSuchTableException;
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
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.TransactionWorkspaceMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.service.catalog.SupportsNotifications;
import org.apache.polaris.service.catalog.common.CatalogHandler;
import org.apache.polaris.service.context.CallContextCatalogFactory;
import org.apache.polaris.service.http.IcebergHttpUtil;
import org.apache.polaris.service.http.IfNoneMatch;
import org.apache.polaris.service.types.NotificationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authorization-aware adapter between REST stubs and shared Iceberg SDK CatalogHandlers.
 *
 * <p>We must make authorization decisions based on entity resolution at this layer instead of the
 * underlying PolarisIcebergCatalog layer, because this REST-adjacent layer captures intent of
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
public class IcebergCatalogHandler extends CatalogHandler implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergCatalogHandler.class);

  private final PolarisMetaStoreManager metaStoreManager;
  private final CallContextCatalogFactory catalogFactory;

  // Catalog instance will be initialized after authorizing resolver successfully resolves
  // the catalog entity.
  protected Catalog baseCatalog = null;
  protected SupportsNamespaces namespaceCatalog = null;
  protected ViewCatalog viewCatalog = null;

  public IcebergCatalogHandler(
      CallContext callContext,
      PolarisEntityManager entityManager,
      PolarisMetaStoreManager metaStoreManager,
      SecurityContext securityContext,
      CallContextCatalogFactory catalogFactory,
      String catalogName,
      PolarisAuthorizer authorizer) {
    super(callContext, entityManager, securityContext, catalogName, authorizer);
    this.metaStoreManager = metaStoreManager;
    this.catalogFactory = catalogFactory;
  }

  @Override
  protected void initializeCatalog() {
    this.baseCatalog =
        catalogFactory.createCallContextCatalog(
            callContext, authenticatedPrincipal, securityContext, resolutionManifest);
    this.namespaceCatalog =
        (baseCatalog instanceof SupportsNamespaces) ? (SupportsNamespaces) baseCatalog : null;
    this.viewCatalog = (baseCatalog instanceof ViewCatalog) ? (ViewCatalog) baseCatalog : null;
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

  public ListNamespacesResponse listNamespaces(Namespace parent) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_NAMESPACES;
    authorizeBasicNamespaceOperationOrThrow(op, parent);

    return CatalogHandlers.listNamespaces(namespaceCatalog, parent);
  }

  public CreateNamespaceResponse createNamespace(CreateNamespaceRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_NAMESPACE;

    Namespace namespace = request.namespace();
    if (namespace.isEmpty()) {
      throw new AlreadyExistsException(
          "Cannot create root namespace, as it already exists implicitly.");
    }
    authorizeCreateNamespaceUnderNamespaceOperationOrThrow(op, namespace);

    if (namespaceCatalog instanceof IcebergCatalog) {
      // Note: The CatalogHandlers' default implementation will non-atomically create the
      // namespace and then fetch its properties using loadNamespaceMetadata for the response.
      // However, the latest namespace metadata technically isn't the same authorized instance,
      // so we don't want all cals to loadNamespaceMetadata to automatically use the manifest
      // in "passthrough" mode.
      //
      // For CreateNamespace, we consider this a special case in that the creator is able to
      // retrieve the latest namespace metadata for the duration of the CreateNamespace
      // operation, even if the entityVersion and/or grantsVersion update in the interim.
      namespaceCatalog.createNamespace(namespace, request.properties());
      return CreateNamespaceResponse.builder()
          .withNamespace(namespace)
          .setProperties(
              resolutionManifest
                  .getPassthroughResolvedPath(namespace)
                  .getRawLeafEntity()
                  .getPropertiesAsMap())
          .build();
    } else {
      return CatalogHandlers.createNamespace(namespaceCatalog, request);
    }
  }

  private static boolean isExternal(CatalogEntity catalog) {
    return org.apache.polaris.core.admin.model.Catalog.TypeEnum.EXTERNAL.equals(
        catalog.getCatalogType());
  }

  public GetNamespaceResponse loadNamespaceMetadata(Namespace namespace) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LOAD_NAMESPACE_METADATA;
    authorizeBasicNamespaceOperationOrThrow(op, namespace);

    return CatalogHandlers.loadNamespace(namespaceCatalog, namespace);
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
    CatalogHandlers.loadNamespace(namespaceCatalog, namespace);
  }

  public void dropNamespace(Namespace namespace) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_NAMESPACE;
    authorizeBasicNamespaceOperationOrThrow(op, namespace);

    CatalogHandlers.dropNamespace(namespaceCatalog, namespace);
  }

  public UpdateNamespacePropertiesResponse updateNamespaceProperties(
      Namespace namespace, UpdateNamespacePropertiesRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.UPDATE_NAMESPACE_PROPERTIES;
    authorizeBasicNamespaceOperationOrThrow(op, namespace);

    return CatalogHandlers.updateNamespaceProperties(namespaceCatalog, namespace, request);
  }

  public ListTablesResponse listTables(Namespace namespace) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_TABLES;
    authorizeBasicNamespaceOperationOrThrow(op, namespace);

    return CatalogHandlers.listTables(baseCatalog, namespace);
  }

  /**
   * Create a table.
   *
   * @param namespace the namespace to create the table in
   * @param request the table creation request
   * @return ETagged {@link LoadTableResponse} to uniquely identify the table metadata
   */
  public LoadTableResponse createTableDirect(Namespace namespace, CreateTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_TABLE_DIRECT;
    TableIdentifier identifier = TableIdentifier.of(namespace, request.name());
    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(op, identifier);

    CatalogEntity catalog =
        CatalogEntity.of(
            resolutionManifest
                .getResolvedReferenceCatalogEntity()
                .getResolvedLeafEntity()
                .getEntity());
    if (isExternal(catalog)) {
      throw new BadRequestException("Cannot create table on external catalogs.");
    }
    return CatalogHandlers.createTable(baseCatalog, namespace, request);
  }

  /**
   * Create a table.
   *
   * @param namespace the namespace to create the table in
   * @param request the table creation request
   * @return ETagged {@link LoadTableResponse} to uniquely identify the table metadata
   */
  public LoadTableResponse createTableDirectWithWriteDelegation(
      Namespace namespace, CreateTableRequest request) {
    PolarisAuthorizableOperation op =
        PolarisAuthorizableOperation.CREATE_TABLE_DIRECT_WITH_WRITE_DELEGATION;
    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
        op, TableIdentifier.of(namespace, request.name()));

    CatalogEntity catalog =
        CatalogEntity.of(
            resolutionManifest
                .getResolvedReferenceCatalogEntity()
                .getResolvedLeafEntity()
                .getEntity());
    if (isExternal(catalog)) {
      throw new BadRequestException("Cannot create table on external catalogs.");
    }
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
      if (baseCatalog instanceof IcebergCatalog) {
        location = ((IcebergCatalog) baseCatalog).transformTableLikeLocation(request.location());
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
            resolutionManifest
                .getResolvedReferenceCatalogEntity()
                .getResolvedLeafEntity()
                .getEntity());
    if (isExternal(catalog)) {
      throw new BadRequestException("Cannot create table on external catalogs.");
    }
    TableMetadata metadata = stageTableCreateHelper(namespace, request);
    return LoadTableResponse.builder().withTableMetadata(metadata).build();
  }

  public LoadTableResponse createTableStagedWithWriteDelegation(
      Namespace namespace, CreateTableRequest request) {
    PolarisAuthorizableOperation op =
        PolarisAuthorizableOperation.CREATE_TABLE_STAGED_WITH_WRITE_DELEGATION;
    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
        op, TableIdentifier.of(namespace, request.name()));

    CatalogEntity catalog =
        CatalogEntity.of(
            resolutionManifest
                .getResolvedReferenceCatalogEntity()
                .getResolvedLeafEntity()
                .getEntity());
    if (isExternal(catalog)) {
      throw new BadRequestException("Cannot create table on external catalogs.");
    }
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
  }

  /**
   * Register a table.
   *
   * @param namespace The namespace to register the table in
   * @param request the register table request
   * @return ETagged {@link LoadTableResponse} to uniquely identify the table metadata
   */
  public LoadTableResponse registerTable(Namespace namespace, RegisterTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.REGISTER_TABLE;
    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
        op, TableIdentifier.of(namespace, request.name()));

    return CatalogHandlers.registerTable(baseCatalog, namespace, request);
  }

  public boolean sendNotification(TableIdentifier identifier, NotificationRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.SEND_NOTIFICATIONS;

    // For now, just require the full set of privileges on the base Catalog entity, which we can
    // also express just as the "root" Namespace for purposes of the PolarisIcebergCatalog being
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
            resolutionManifest
                .getResolvedReferenceCatalogEntity()
                .getResolvedLeafEntity()
                .getEntity());
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

  /**
   * Fetch the metastore table entity for the given table identifier
   *
   * @param tableIdentifier The identifier of the table
   * @return the Polaris table entity for the table
   */
  private IcebergTableLikeEntity getTableEntity(TableIdentifier tableIdentifier) {
    PolarisResolvedPathWrapper target = resolutionManifest.getResolvedPath(tableIdentifier);

    return IcebergTableLikeEntity.of(target.getRawLeafEntity());
  }

  public LoadTableResponse loadTable(TableIdentifier tableIdentifier, String snapshots) {
    return loadTableIfStale(tableIdentifier, null, snapshots).get();
  }

  /**
   * Attempt to perform a loadTable operation only when the specified set of eTags do not match the
   * current state of the table metadata.
   *
   * @param tableIdentifier The identifier of the table to load
   * @param ifNoneMatch set of entity-tags to check the metadata against for staleness
   * @param snapshots
   * @return {@link Optional#empty()} if the ETag is current, an {@link Optional} containing the
   *     load table response, otherwise
   */
  public Optional<LoadTableResponse> loadTableIfStale(
      TableIdentifier tableIdentifier, IfNoneMatch ifNoneMatch, String snapshots) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LOAD_TABLE;
    authorizeBasicTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);

    if (ifNoneMatch != null) {
      // Perform freshness-aware table loading if caller specified ifNoneMatch.
      IcebergTableLikeEntity tableEntity = getTableEntity(tableIdentifier);
      if (tableEntity == null || tableEntity.getMetadataLocation() == null) {
        LOGGER
            .atWarn()
            .addKeyValue("tableIdentifier", tableIdentifier)
            .addKeyValue("tableEntity", tableEntity)
            .log("Failed to getMetadataLocation to generate ETag when loading table");
      } else {
        // TODO: Refactor null-checking into the helper method once we create a more canonical
        // interface for associate etags with entities.
        String tableEntityTag =
            IcebergHttpUtil.generateETagForMetadataFileLocation(tableEntity.getMetadataLocation());
        if (ifNoneMatch.anyMatch(tableEntityTag)) {
          return Optional.empty();
        }
      }
    }

    return Optional.of(CatalogHandlers.loadTable(baseCatalog, tableIdentifier));
  }

  public LoadTableResponse loadTableWithAccessDelegation(
      TableIdentifier tableIdentifier, String snapshots) {
    return loadTableWithAccessDelegationIfStale(tableIdentifier, null, snapshots).get();
  }

  /**
   * Attempt to perform a loadTable operation with access delegation only when the if none of the
   * provided eTags match the current state of the table metadata.
   *
   * @param tableIdentifier The identifier of the table to load
   * @param ifNoneMatch set of entity-tags to check the metadata against for staleness
   * @param snapshots
   * @return {@link Optional#empty()} if the ETag is current, an {@link Optional} containing the
   *     load table response, otherwise
   */
  public Optional<LoadTableResponse> loadTableWithAccessDelegationIfStale(
      TableIdentifier tableIdentifier, IfNoneMatch ifNoneMatch, String snapshots) {
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
      authorizeBasicTableLikeOperationOrThrow(
          write, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);
      actionsRequested.add(PolarisStorageActions.WRITE);
    } catch (ForbiddenException e) {
      authorizeBasicTableLikeOperationOrThrow(
          read, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);
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
            FeatureConfiguration.ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING)) {
      throw new ForbiddenException(
          "Access Delegation is not enabled for this catalog. Please consult applicable "
              + "documentation for the catalog config property '%s' to enable this feature",
          FeatureConfiguration.ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING.catalogConfig());
    }

    if (ifNoneMatch != null) {
      // Perform freshness-aware table loading if caller specified ifNoneMatch.
      IcebergTableLikeEntity tableEntity = getTableEntity(tableIdentifier);
      if (tableEntity == null || tableEntity.getMetadataLocation() == null) {
        LOGGER
            .atWarn()
            .addKeyValue("tableIdentifier", tableIdentifier)
            .addKeyValue("tableEntity", tableEntity)
            .log("Failed to getMetadataLocation to generate ETag when loading table");
      } else {
        // TODO: Refactor null-checking into the helper method once we create a more canonical
        // interface for associate etags with entities.
        String tableETag =
            IcebergHttpUtil.generateETagForMetadataFileLocation(tableEntity.getMetadataLocation());
        if (ifNoneMatch.anyMatch(tableETag)) {
          return Optional.empty();
        }
      }
    }

    // TODO: Find a way for the configuration or caller to better express whether to fail or omit
    // when data-access is specified but access delegation grants are not found.
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

      return Optional.of(responseBuilder.build());
    } else if (table instanceof BaseMetadataTable) {
      // metadata tables are loaded on the client side, return NoSuchTableException for now
      throw new NoSuchTableException("Table does not exist: %s", tableIdentifier.toString());
    }

    throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
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
                  if (baseCatalog instanceof IcebergCatalog
                      && update instanceof MetadataUpdate.SetLocation) {
                    String requestedLocation = ((MetadataUpdate.SetLocation) update).location();
                    String filteredLocation =
                        ((IcebergCatalog) baseCatalog)
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
    authorizeBasicTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);

    CatalogEntity catalog =
        CatalogEntity.of(
            resolutionManifest
                .getResolvedReferenceCatalogEntity()
                .getResolvedLeafEntity()
                .getEntity());
    if (isExternal(catalog)) {
      throw new BadRequestException("Cannot update table on external catalogs.");
    }
    return CatalogHandlers.updateTable(baseCatalog, tableIdentifier, applyUpdateFilters(request));
  }

  public LoadTableResponse updateTableForStagedCreate(
      TableIdentifier tableIdentifier, UpdateTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.UPDATE_TABLE_FOR_STAGED_CREATE;
    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(op, tableIdentifier);

    CatalogEntity catalog =
        CatalogEntity.of(
            resolutionManifest
                .getResolvedReferenceCatalogEntity()
                .getResolvedLeafEntity()
                .getEntity());
    if (isExternal(catalog)) {
      throw new BadRequestException("Cannot update table on external catalogs.");
    }
    return CatalogHandlers.updateTable(baseCatalog, tableIdentifier, applyUpdateFilters(request));
  }

  public void dropTableWithoutPurge(TableIdentifier tableIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_TABLE_WITHOUT_PURGE;
    authorizeBasicTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);

    CatalogHandlers.dropTable(baseCatalog, tableIdentifier);
  }

  public void dropTableWithPurge(TableIdentifier tableIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_TABLE_WITH_PURGE;
    authorizeBasicTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);

    CatalogEntity catalog =
        CatalogEntity.of(
            resolutionManifest
                .getResolvedReferenceCatalogEntity()
                .getResolvedLeafEntity()
                .getEntity());
    if (isExternal(catalog)) {
      throw new BadRequestException("Cannot drop table on external catalogs.");
    }
    CatalogHandlers.purgeTable(baseCatalog, tableIdentifier);
  }

  public void tableExists(TableIdentifier tableIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.TABLE_EXISTS;
    authorizeBasicTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);

    // TODO: Just skip CatalogHandlers for this one maybe
    CatalogHandlers.loadTable(baseCatalog, tableIdentifier);
  }

  public void renameTable(RenameTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.RENAME_TABLE;
    authorizeRenameTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_TABLE, request.source(), request.destination());

    CatalogEntity catalog =
        CatalogEntity.of(
            resolutionManifest
                .getResolvedReferenceCatalogEntity()
                .getResolvedLeafEntity()
                .getEntity());
    if (isExternal(catalog)) {
      throw new BadRequestException("Cannot rename table on external catalogs.");
    }
    CatalogHandlers.renameTable(baseCatalog, request);
  }

  public void commitTransaction(CommitTransactionRequest commitTransactionRequest) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.COMMIT_TRANSACTION;
    // TODO: The authz actually needs to detect hidden updateForStagedCreate UpdateTableRequests
    // and have some kind of per-item conditional privilege requirement if we want to make it
    // so that only the stageCreate updates need TABLE_CREATE whereas everything else only
    // needs TABLE_WRITE_PROPERTIES.
    authorizeCollectionOfTableLikeOperationOrThrow(
        op,
        PolarisEntitySubType.ICEBERG_TABLE,
        commitTransactionRequest.tableChanges().stream()
            .map(UpdateTableRequest::identifier)
            .toList());
    CatalogEntity catalog =
        CatalogEntity.of(
            resolutionManifest
                .getResolvedReferenceCatalogEntity()
                .getResolvedLeafEntity()
                .getEntity());
    if (isExternal(catalog)) {
      throw new BadRequestException("Cannot update table on external catalogs.");
    }

    if (!(baseCatalog instanceof IcebergCatalog)) {
      throw new BadRequestException(
          "Unsupported operation: commitTransaction with baseCatalog type: %s",
          baseCatalog.getClass().getName());
    }

    // Swap in TransactionWorkspaceMetaStoreManager for all mutations made by this baseCatalog to
    // only go into an in-memory collection that we can commit as a single atomic unit after all
    // validations.
    TransactionWorkspaceMetaStoreManager transactionMetaStoreManager =
        new TransactionWorkspaceMetaStoreManager(metaStoreManager);
    ((IcebergCatalog) baseCatalog).setMetaStoreManager(transactionMetaStoreManager);

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
                                      FeatureConfiguration.ALLOW_NAMESPACE_LOCATION_OVERLAP)) {
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
    List<EntityWithPath> pendingUpdates = transactionMetaStoreManager.getPendingUpdates();
    EntitiesResult result =
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

    return CatalogHandlers.listViews(viewCatalog, namespace);
  }

  public LoadViewResponse createView(Namespace namespace, CreateViewRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_VIEW;
    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
        op, TableIdentifier.of(namespace, request.name()));

    CatalogEntity catalog =
        CatalogEntity.of(
            resolutionManifest
                .getResolvedReferenceCatalogEntity()
                .getResolvedLeafEntity()
                .getEntity());
    if (isExternal(catalog)) {
      throw new BadRequestException("Cannot create view on external catalogs.");
    }
    return CatalogHandlers.createView(viewCatalog, namespace, request);
  }

  public LoadViewResponse loadView(TableIdentifier viewIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LOAD_VIEW;
    authorizeBasicTableLikeOperationOrThrow(op, PolarisEntitySubType.ICEBERG_VIEW, viewIdentifier);

    return CatalogHandlers.loadView(viewCatalog, viewIdentifier);
  }

  public LoadViewResponse replaceView(TableIdentifier viewIdentifier, UpdateTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.REPLACE_VIEW;
    authorizeBasicTableLikeOperationOrThrow(op, PolarisEntitySubType.ICEBERG_VIEW, viewIdentifier);

    CatalogEntity catalog =
        CatalogEntity.of(
            resolutionManifest
                .getResolvedReferenceCatalogEntity()
                .getResolvedLeafEntity()
                .getEntity());
    if (isExternal(catalog)) {
      throw new BadRequestException("Cannot replace view on external catalogs.");
    }
    return CatalogHandlers.updateView(viewCatalog, viewIdentifier, applyUpdateFilters(request));
  }

  public void dropView(TableIdentifier viewIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_VIEW;
    authorizeBasicTableLikeOperationOrThrow(op, PolarisEntitySubType.ICEBERG_VIEW, viewIdentifier);

    CatalogHandlers.dropView(viewCatalog, viewIdentifier);
  }

  public void viewExists(TableIdentifier viewIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.VIEW_EXISTS;
    authorizeBasicTableLikeOperationOrThrow(op, PolarisEntitySubType.ICEBERG_VIEW, viewIdentifier);

    // TODO: Just skip CatalogHandlers for this one maybe
    CatalogHandlers.loadView(viewCatalog, viewIdentifier);
  }

  public void renameView(RenameTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.RENAME_VIEW;
    authorizeRenameTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_VIEW, request.source(), request.destination());

    CatalogEntity catalog =
        CatalogEntity.of(
            resolutionManifest
                .getResolvedReferenceCatalogEntity()
                .getResolvedLeafEntity()
                .getEntity());
    if (isExternal(catalog)) {
      throw new BadRequestException("Cannot rename view on external catalogs.");
    }
    CatalogHandlers.renameView(viewCatalog, request);
  }

  @Override
  public void close() throws Exception {
    if (baseCatalog instanceof Closeable closeable) {
      closeable.close();
    }
  }
}
