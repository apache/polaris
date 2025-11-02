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

import static org.apache.polaris.core.config.FeatureConfiguration.ALLOW_FEDERATED_CATALOGS_CREDENTIAL_VENDING;
import static org.apache.polaris.core.config.FeatureConfiguration.LIST_PAGINATION_ENABLED;
import static org.apache.polaris.service.catalog.AccessDelegationMode.VENDED_CREDENTIALS;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.enterprise.inject.Instance;
import jakarta.ws.rs.core.SecurityContext;
import java.io.Closeable;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
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
import org.apache.iceberg.SnapshotRef;
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
import org.apache.iceberg.rest.credentials.ImmutableCredential;
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
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.catalog.ExternalCatalogFactory;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.ConnectionType;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.TransactionWorkspaceMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageUtil;
import org.apache.polaris.service.catalog.AccessDelegationMode;
import org.apache.polaris.service.catalog.SupportsNotifications;
import org.apache.polaris.service.catalog.common.CatalogHandler;
import org.apache.polaris.service.catalog.common.CatalogUtils;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.context.catalog.CallContextCatalogFactory;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
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
  private final ReservedProperties reservedProperties;
  private final CatalogHandlerUtils catalogHandlerUtils;
  private final PolarisEventListener polarisEventListener;
  private final StorageAccessConfigProvider storageAccessConfigProvider;

  // Catalog instance will be initialized after authorizing resolver successfully resolves
  // the catalog entity.
  protected Catalog baseCatalog = null;
  protected SupportsNamespaces namespaceCatalog = null;
  protected ViewCatalog viewCatalog = null;

  public static final String SNAPSHOTS_ALL = "all";
  public static final String SNAPSHOTS_REFS = "refs";

  public IcebergCatalogHandler(
      PolarisDiagnostics diagnostics,
      CallContext callContext,
      ResolutionManifestFactory resolutionManifestFactory,
      PolarisMetaStoreManager metaStoreManager,
      PolarisCredentialManager credentialManager,
      SecurityContext securityContext,
      CallContextCatalogFactory catalogFactory,
      String catalogName,
      PolarisAuthorizer authorizer,
      ReservedProperties reservedProperties,
      CatalogHandlerUtils catalogHandlerUtils,
      Instance<ExternalCatalogFactory> externalCatalogFactories,
      PolarisEventListener polarisEventListener,
      StorageAccessConfigProvider storageAccessConfigProvider) {
    super(
        diagnostics,
        callContext,
        resolutionManifestFactory,
        securityContext,
        catalogName,
        authorizer,
        credentialManager,
        externalCatalogFactories);
    this.metaStoreManager = metaStoreManager;
    this.catalogFactory = catalogFactory;
    this.reservedProperties = reservedProperties;
    this.catalogHandlerUtils = catalogHandlerUtils;
    this.polarisEventListener = polarisEventListener;
    this.storageAccessConfigProvider = storageAccessConfigProvider;
  }

  private CatalogEntity getResolvedCatalogEntity() {
    CatalogEntity catalogEntity = resolutionManifest.getResolvedCatalogEntity();
    diagnostics.checkNotNull(catalogEntity, "No catalog available");
    return catalogEntity;
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

  private boolean shouldDecodeToken() {
    return realmConfig.getConfig(LIST_PAGINATION_ENABLED, getResolvedCatalogEntity());
  }

  public ListNamespacesResponse listNamespaces(
      Namespace parent, String pageToken, Integer pageSize) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_NAMESPACES;
    authorizeBasicNamespaceOperationOrThrow(op, parent);

    if (baseCatalog instanceof IcebergCatalog polarisCatalog) {
      PageToken pageRequest = PageToken.build(pageToken, pageSize, this::shouldDecodeToken);
      Page<Namespace> results = polarisCatalog.listNamespaces(parent, pageRequest);
      return ListNamespacesResponse.builder()
          .addAll(results.items())
          .nextPageToken(results.encodedResponseToken())
          .build();
    } else {
      return catalogHandlerUtils.listNamespaces(namespaceCatalog, parent, pageToken, pageSize);
    }
  }

  @Override
  protected void initializeCatalog() {
    CatalogEntity resolvedCatalogEntity = getResolvedCatalogEntity();
    ConnectionConfigInfoDpo connectionConfigInfoDpo =
        resolvedCatalogEntity.getConnectionConfigInfoDpo();
    if (connectionConfigInfoDpo != null) {
      LOGGER
          .atInfo()
          .addKeyValue("remoteUrl", connectionConfigInfoDpo.getUri())
          .log("Initializing federated catalog");
      FeatureConfiguration.enforceFeatureEnabledOrThrow(
          realmConfig, FeatureConfiguration.ENABLE_CATALOG_FEDERATION);

      Catalog federatedCatalog;
      ConnectionType connectionType =
          ConnectionType.fromCode(connectionConfigInfoDpo.getConnectionTypeCode());

      // Use the unified factory pattern for all external catalog types
      Instance<ExternalCatalogFactory> externalCatalogFactory =
          externalCatalogFactories.select(
              Identifier.Literal.of(connectionType.getFactoryIdentifier()));
      if (externalCatalogFactory.isResolvable()) {
        federatedCatalog =
            externalCatalogFactory
                .get()
                .createCatalog(connectionConfigInfoDpo, getPolarisCredentialManager());
      } else {
        throw new UnsupportedOperationException(
            "External catalog factory for type '" + connectionType + "' is unavailable.");
      }
      // TODO: if the remote catalog is not RestCatalog, the corresponding table operation will use
      // environment to load the table metadata, the env may not contain credentials to access the
      // storage. In the future, we could leverage PolarisCredentialManager to inject storage
      // credentials for non-rest remote catalog
      this.baseCatalog = federatedCatalog;
    } else {
      LOGGER.atInfo().log("Initializing non-federated catalog");
      this.baseCatalog =
          catalogFactory.createCallContextCatalog(
              callContext, polarisPrincipal, securityContext, resolutionManifest);
    }
    this.namespaceCatalog =
        (baseCatalog instanceof SupportsNamespaces) ? (SupportsNamespaces) baseCatalog : null;
    this.viewCatalog = (baseCatalog instanceof ViewCatalog) ? (ViewCatalog) baseCatalog : null;
  }

  public ListNamespacesResponse listNamespaces(Namespace parent) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_NAMESPACES;
    authorizeBasicNamespaceOperationOrThrow(op, parent);

    return catalogHandlerUtils.listNamespaces(namespaceCatalog, parent);
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
      namespaceCatalog.createNamespace(
          namespace, reservedProperties.removeReservedProperties(request.properties()));
      Map<String, String> filteredProperties =
          reservedProperties.removeReservedProperties(
              resolutionManifest
                  .getPassthroughResolvedPath(namespace)
                  .getRawLeafEntity()
                  .getPropertiesAsMap());
      return CreateNamespaceResponse.builder()
          .withNamespace(namespace)
          .setProperties(filteredProperties)
          .build();
    } else {
      return catalogHandlerUtils.createNamespace(namespaceCatalog, request);
    }
  }

  public GetNamespaceResponse loadNamespaceMetadata(Namespace namespace) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LOAD_NAMESPACE_METADATA;
    authorizeBasicNamespaceOperationOrThrow(op, namespace);

    return catalogHandlerUtils.loadNamespace(namespaceCatalog, namespace);
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
    catalogHandlerUtils.loadNamespace(namespaceCatalog, namespace);
  }

  public void dropNamespace(Namespace namespace) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_NAMESPACE;
    authorizeBasicNamespaceOperationOrThrow(op, namespace);

    catalogHandlerUtils.dropNamespace(namespaceCatalog, namespace);
  }

  public UpdateNamespacePropertiesResponse updateNamespaceProperties(
      Namespace namespace, UpdateNamespacePropertiesRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.UPDATE_NAMESPACE_PROPERTIES;
    authorizeBasicNamespaceOperationOrThrow(op, namespace);

    return catalogHandlerUtils.updateNamespaceProperties(namespaceCatalog, namespace, request);
  }

  public ListTablesResponse listTables(Namespace namespace, String pageToken, Integer pageSize) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_TABLES;
    authorizeBasicNamespaceOperationOrThrow(op, namespace);

    if (baseCatalog instanceof IcebergCatalog polarisCatalog) {
      PageToken pageRequest = PageToken.build(pageToken, pageSize, this::shouldDecodeToken);
      Page<TableIdentifier> results = polarisCatalog.listTables(namespace, pageRequest);
      return ListTablesResponse.builder()
          .addAll(results.items())
          .nextPageToken(results.encodedResponseToken())
          .build();
    } else {
      return catalogHandlerUtils.listTables(baseCatalog, namespace, pageToken, pageSize);
    }
  }

  public ListTablesResponse listTables(Namespace namespace) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_TABLES;
    authorizeBasicNamespaceOperationOrThrow(op, namespace);

    return catalogHandlerUtils.listTables(baseCatalog, namespace);
  }

  /**
   * Create a table.
   *
   * @param namespace the namespace to create the table in
   * @param request the table creation request
   * @return ETagged {@link LoadTableResponse} to uniquely identify the table metadata
   */
  public LoadTableResponse createTableDirect(Namespace namespace, CreateTableRequest request) {
    return createTableDirect(
        namespace, request, EnumSet.noneOf(AccessDelegationMode.class), Optional.empty());
  }

  /**
   * Create a table.
   *
   * @param namespace the namespace to create the table in
   * @param request the table creation request
   * @return ETagged {@link LoadTableResponse} to uniquely identify the table metadata
   */
  public LoadTableResponse createTableDirectWithWriteDelegation(
      Namespace namespace,
      CreateTableRequest request,
      Optional<String> refreshCredentialsEndpoint) {
    return createTableDirect(
        namespace, request, EnumSet.of(VENDED_CREDENTIALS), refreshCredentialsEndpoint);
  }

  public void authorizeCreateTableDirect(
      Namespace namespace,
      CreateTableRequest request,
      EnumSet<AccessDelegationMode> delegationModes) {
    if (delegationModes.isEmpty()) {
      TableIdentifier identifier = TableIdentifier.of(namespace, request.name());
      authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
          PolarisAuthorizableOperation.CREATE_TABLE_DIRECT, identifier);
    } else {
      authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
          PolarisAuthorizableOperation.CREATE_TABLE_DIRECT_WITH_WRITE_DELEGATION,
          TableIdentifier.of(namespace, request.name()));
    }

    CatalogEntity catalog = getResolvedCatalogEntity();
    if (catalog.isStaticFacade()) {
      throw new BadRequestException("Cannot create table on static-facade external catalogs.");
    }
    checkAllowExternalCatalogCredentialVending(delegationModes);
  }

  public LoadTableResponse createTableDirect(
      Namespace namespace,
      CreateTableRequest request,
      EnumSet<AccessDelegationMode> delegationModes,
      Optional<String> refreshCredentialsEndpoint) {

    authorizeCreateTableDirect(namespace, request, delegationModes);

    request.validate();

    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, request.name());
    if (baseCatalog.tableExists(tableIdentifier)) {
      throw new AlreadyExistsException("Table already exists: %s", tableIdentifier);
    }

    Map<String, String> properties = Maps.newHashMap();
    properties.put("created-at", OffsetDateTime.now(ZoneOffset.UTC).toString());
    properties.putAll(reservedProperties.removeReservedProperties(request.properties()));

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
      return buildLoadTableResponseWithDelegationCredentials(
              tableIdentifier,
              tableMetadata,
              delegationModes,
              Set.of(
                  PolarisStorageActions.READ,
                  PolarisStorageActions.WRITE,
                  PolarisStorageActions.LIST),
              refreshCredentialsEndpoint)
          .build();
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
    properties.putAll(reservedProperties.removeReservedProperties(request.properties()));

    String location;
    if (request.location() != null) {
      // Even if the request provides a location, run it through the catalog's TableBuilder
      // to inherit any override behaviors if applicable.
      if (baseCatalog instanceof IcebergCatalog) {
        location =
            ((IcebergCatalog) baseCatalog).transformTableLikeLocation(ident, request.location());
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
    return createTableStaged(
        namespace, request, EnumSet.noneOf(AccessDelegationMode.class), Optional.empty());
  }

  public LoadTableResponse createTableStagedWithWriteDelegation(
      Namespace namespace,
      CreateTableRequest request,
      Optional<String> refreshCredentialsEndpoint) {
    return createTableStaged(
        namespace, request, EnumSet.of(VENDED_CREDENTIALS), refreshCredentialsEndpoint);
  }

  private void authorizeCreateTableStaged(
      Namespace namespace,
      CreateTableRequest request,
      EnumSet<AccessDelegationMode> delegationModes) {
    if (delegationModes.isEmpty()) {
      authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
          PolarisAuthorizableOperation.CREATE_TABLE_STAGED,
          TableIdentifier.of(namespace, request.name()));
    } else {
      authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
          PolarisAuthorizableOperation.CREATE_TABLE_STAGED_WITH_WRITE_DELEGATION,
          TableIdentifier.of(namespace, request.name()));
    }

    CatalogEntity catalog = getResolvedCatalogEntity();
    if (catalog.isStaticFacade()) {
      throw new BadRequestException("Cannot create table on static-facade external catalogs.");
    }
    checkAllowExternalCatalogCredentialVending(delegationModes);
  }

  public LoadTableResponse createTableStaged(
      Namespace namespace,
      CreateTableRequest request,
      EnumSet<AccessDelegationMode> delegationModes,
      Optional<String> refreshCredentialsEndpoint) {

    authorizeCreateTableStaged(namespace, request, delegationModes);

    TableIdentifier ident = TableIdentifier.of(namespace, request.name());
    TableMetadata metadata = stageTableCreateHelper(namespace, request);

    return buildLoadTableResponseWithDelegationCredentials(
            ident,
            metadata,
            delegationModes,
            Set.of(PolarisStorageActions.ALL),
            refreshCredentialsEndpoint)
        .build();
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

    return catalogHandlerUtils.registerTable(baseCatalog, namespace, request);
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
        op, Namespace.empty(), extraPassthroughNamespaces, extraPassthroughTableLikes, null);

    CatalogEntity catalog = getResolvedCatalogEntity();
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
   * @return the Polaris table entity for the table or null for external catalogs
   */
  private @Nullable IcebergTableLikeEntity getTableEntity(TableIdentifier tableIdentifier) {
    PolarisResolvedPathWrapper target = resolutionManifest.getResolvedPath(tableIdentifier);
    PolarisEntity rawLeafEntity = target.getRawLeafEntity();
    if (rawLeafEntity.getType() == PolarisEntityType.TABLE_LIKE) {
      return IcebergTableLikeEntity.of(rawLeafEntity);
    }
    return null; // could be an external catalog
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
    return loadTable(
        tableIdentifier,
        snapshots,
        ifNoneMatch,
        EnumSet.noneOf(AccessDelegationMode.class),
        Optional.empty());
  }

  public LoadTableResponse loadTableWithAccessDelegation(
      TableIdentifier tableIdentifier,
      String snapshots,
      Optional<String> refreshCredentialsEndpoint) {
    return loadTableWithAccessDelegationIfStale(
            tableIdentifier, null, snapshots, refreshCredentialsEndpoint)
        .get();
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
      TableIdentifier tableIdentifier,
      IfNoneMatch ifNoneMatch,
      String snapshots,
      Optional<String> refreshCredentialsEndpoint) {
    return loadTable(
        tableIdentifier,
        snapshots,
        ifNoneMatch,
        EnumSet.of(VENDED_CREDENTIALS),
        refreshCredentialsEndpoint);
  }

  private Set<PolarisStorageActions> authorizeLoadTable(
      TableIdentifier tableIdentifier, EnumSet<AccessDelegationMode> delegationModes) {
    if (delegationModes.isEmpty()) {
      authorizeBasicTableLikeOperationOrThrow(
          PolarisAuthorizableOperation.LOAD_TABLE,
          PolarisEntitySubType.ICEBERG_TABLE,
          tableIdentifier);
      return Set.of();
    }

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

    checkAllowExternalCatalogCredentialVending(delegationModes);

    return actionsRequested;
  }

  public Optional<LoadTableResponse> loadTable(
      TableIdentifier tableIdentifier,
      String snapshots,
      IfNoneMatch ifNoneMatch,
      EnumSet<AccessDelegationMode> delegationModes,
      Optional<String> refreshCredentialsEndpoint) {

    Set<PolarisStorageActions> actionsRequested =
        authorizeLoadTable(tableIdentifier, delegationModes);

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
      LoadTableResponse response =
          buildLoadTableResponseWithDelegationCredentials(
                  tableIdentifier,
                  tableMetadata,
                  delegationModes,
                  actionsRequested,
                  refreshCredentialsEndpoint)
              .build();
      return Optional.of(filterResponseToSnapshots(response, snapshots));
    } else if (table instanceof BaseMetadataTable) {
      // metadata tables are loaded on the client side, return NoSuchTableException for now
      throw new NoSuchTableException("Table does not exist: %s", tableIdentifier.toString());
    }

    throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
  }

  private LoadTableResponse.Builder buildLoadTableResponseWithDelegationCredentials(
      TableIdentifier tableIdentifier,
      TableMetadata tableMetadata,
      EnumSet<AccessDelegationMode> delegationModes,
      Set<PolarisStorageActions> actions,
      Optional<String> refreshCredentialsEndpoint) {
    LoadTableResponse.Builder responseBuilder =
        LoadTableResponse.builder().withTableMetadata(tableMetadata);
    PolarisResolvedPathWrapper resolvedStoragePath =
        CatalogUtils.findResolvedStorageEntity(resolutionManifest, tableIdentifier);

    if (resolvedStoragePath == null) {
      LOGGER.debug(
          "Unable to find storage configuration information for table {}", tableIdentifier);
      return responseBuilder;
    }

    if (baseCatalog instanceof IcebergCatalog
        || realmConfig.getConfig(
            ALLOW_FEDERATED_CATALOGS_CREDENTIAL_VENDING, getResolvedCatalogEntity())) {

      Set<String> tableLocations = StorageUtil.getLocationsUsedByTable(tableMetadata);

      // For non polaris' catalog, validate that table locations are within allowed locations
      if (!(baseCatalog instanceof IcebergCatalog)) {
        validateRemoteTableLocations(tableIdentifier, tableLocations, resolvedStoragePath);
      }

      StorageAccessConfig storageAccessConfig =
          storageAccessConfigProvider.getStorageAccessConfig(
              callContext,
              tableIdentifier,
              tableLocations,
              actions,
              refreshCredentialsEndpoint,
              resolvedStoragePath);
      Map<String, String> credentialConfig = storageAccessConfig.credentials();
      if (delegationModes.contains(VENDED_CREDENTIALS)) {
        if (!credentialConfig.isEmpty()) {
          responseBuilder.addAllConfig(credentialConfig);
          responseBuilder.addCredential(
              ImmutableCredential.builder()
                  .prefix(tableMetadata.location())
                  .config(credentialConfig)
                  .build());
        } else {
          Boolean skipCredIndirection =
              realmConfig.getConfig(FeatureConfiguration.SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION);
          Preconditions.checkArgument(
              !storageAccessConfig.supportsCredentialVending() || skipCredIndirection,
              "Credential vending was requested for table %s, but no credentials are available",
              tableIdentifier);
        }
      }
      responseBuilder.addAllConfig(storageAccessConfig.extraProperties());
    }

    return responseBuilder;
  }

  private void validateRemoteTableLocations(
      TableIdentifier tableIdentifier,
      Set<String> tableLocations,
      PolarisResolvedPathWrapper resolvedStoragePath) {

    try {
      // Delegate to common validation logic
      CatalogUtils.validateLocationsForTableLike(
          realmConfig, tableIdentifier, tableLocations, resolvedStoragePath);

      LOGGER
          .atInfo()
          .addKeyValue("tableIdentifier", tableIdentifier)
          .addKeyValue("tableLocations", tableLocations)
          .log("Validated federated table locations");
    } catch (ForbiddenException e) {
      LOGGER
          .atError()
          .addKeyValue("tableIdentifier", tableIdentifier)
          .addKeyValue("tableLocations", tableLocations)
          .log("Federated table locations validation failed");
      throw new ForbiddenException(
          "Table '%s' in remote catalog has locations outside catalog's allowed locations: %s",
          tableIdentifier, e.getMessage());
    }
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
                      && update instanceof MetadataUpdate.SetLocation setLocation) {
                    String requestedLocation = setLocation.location();
                    String filteredLocation =
                        ((IcebergCatalog) baseCatalog)
                            .transformTableLikeLocation(identifier, requestedLocation);
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

    // Ensure resolution manifest is initialized so we can determine whether
    // fine grained authz model is enabled at the catalog level
    ensureResolutionManifestForTable(tableIdentifier);

    EnumSet<PolarisAuthorizableOperation> authorizableOperations =
        getUpdateTableAuthorizableOperations(request);

    authorizeBasicTableLikeOperationsOrThrow(
        authorizableOperations, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);

    CatalogEntity catalog = getResolvedCatalogEntity();
    if (catalog.isStaticFacade()) {
      throw new BadRequestException("Cannot update table on static-facade external catalogs.");
    }
    return catalogHandlerUtils.updateTable(
        baseCatalog, tableIdentifier, applyUpdateFilters(request));
  }

  public LoadTableResponse updateTableForStagedCreate(
      TableIdentifier tableIdentifier, UpdateTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.UPDATE_TABLE_FOR_STAGED_CREATE;
    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(op, tableIdentifier);

    CatalogEntity catalog = getResolvedCatalogEntity();
    if (catalog.isStaticFacade()) {
      throw new BadRequestException("Cannot update table on static-facade external catalogs.");
    }
    return catalogHandlerUtils.updateTable(
        baseCatalog, tableIdentifier, applyUpdateFilters(request));
  }

  public void dropTableWithoutPurge(TableIdentifier tableIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_TABLE_WITHOUT_PURGE;
    authorizeBasicTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);

    catalogHandlerUtils.dropTable(baseCatalog, tableIdentifier);
  }

  public void dropTableWithPurge(TableIdentifier tableIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_TABLE_WITH_PURGE;
    authorizeBasicTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);

    CatalogEntity catalog = getResolvedCatalogEntity();
    if (catalog.isStaticFacade()) {
      throw new BadRequestException("Cannot drop table on static-facade external catalogs.");
    }
    catalogHandlerUtils.purgeTable(baseCatalog, tableIdentifier);
  }

  public void tableExists(TableIdentifier tableIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.TABLE_EXISTS;
    authorizeBasicTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);

    // TODO: Just skip CatalogHandlers for this one maybe
    catalogHandlerUtils.loadTable(baseCatalog, tableIdentifier);
  }

  public void renameTable(RenameTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.RENAME_TABLE;
    authorizeRenameTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_TABLE, request.source(), request.destination());

    CatalogEntity catalog = getResolvedCatalogEntity();
    if (catalog.isStaticFacade()) {
      throw new BadRequestException("Cannot rename table on static-facade external catalogs.");
    }
    catalogHandlerUtils.renameTable(baseCatalog, request);
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
    CatalogEntity catalog = getResolvedCatalogEntity();
    if (catalog.isStaticFacade()) {
      throw new BadRequestException("Cannot update table on static-facade external catalogs.");
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
        new TransactionWorkspaceMetaStoreManager(diagnostics, metaStoreManager);
    ((IcebergCatalog) baseCatalog).setMetaStoreManager(transactionMetaStoreManager);

    commitTransactionRequest.tableChanges().stream()
        .forEach(
            change -> {
              Table table = baseCatalog.loadTable(change.identifier());
              if (!(table instanceof BaseTable baseTable)) {
                throw new IllegalStateException(
                    "Cannot wrap catalog that does not produce BaseTable");
              }
              if (isCreate(change)) {
                throw new BadRequestException(
                    "Unsupported operation: commitTranaction with updateForStagedCreate: %s",
                    change);
              }

              TableOperations tableOps = baseTable.operations();
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
                        if (singleUpdate instanceof MetadataUpdate.SetLocation setLocation) {
                          if (!currentMetadata.location().equals(setLocation.location())
                              && !realmConfig.getConfig(
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
      // TODO: Retries and server-side cleanup on failure, review possible exceptions
      throw new CommitFailedException(
          "Transaction commit failed with status: %s, extraInfo: %s",
          result.getReturnStatus(), result.getExtraInformation());
    }
  }

  public ListTablesResponse listViews(Namespace namespace, String pageToken, Integer pageSize) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_VIEWS;
    authorizeBasicNamespaceOperationOrThrow(op, namespace);

    if (baseCatalog instanceof IcebergCatalog polarisCatalog) {
      PageToken pageRequest = PageToken.build(pageToken, pageSize, this::shouldDecodeToken);
      Page<TableIdentifier> results = polarisCatalog.listViews(namespace, pageRequest);
      return ListTablesResponse.builder()
          .addAll(results.items())
          .nextPageToken(results.encodedResponseToken())
          .build();
    } else if (baseCatalog instanceof ViewCatalog viewCatalog) {
      return catalogHandlerUtils.listViews(viewCatalog, namespace, pageToken, pageSize);
    } else {
      throw new BadRequestException(
          "Unsupported operation: listViews with baseCatalog type: %s",
          baseCatalog.getClass().getName());
    }
  }

  public ListTablesResponse listViews(Namespace namespace) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_VIEWS;
    authorizeBasicNamespaceOperationOrThrow(op, namespace);

    return catalogHandlerUtils.listViews(viewCatalog, namespace);
  }

  public LoadViewResponse createView(Namespace namespace, CreateViewRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_VIEW;
    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
        op, TableIdentifier.of(namespace, request.name()));

    CatalogEntity catalog = getResolvedCatalogEntity();
    if (catalog.isStaticFacade()) {
      throw new BadRequestException("Cannot create view on static-facade external catalogs.");
    }
    return catalogHandlerUtils.createView(viewCatalog, namespace, request);
  }

  public LoadViewResponse loadView(TableIdentifier viewIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LOAD_VIEW;
    authorizeBasicTableLikeOperationOrThrow(op, PolarisEntitySubType.ICEBERG_VIEW, viewIdentifier);

    return catalogHandlerUtils.loadView(viewCatalog, viewIdentifier);
  }

  public LoadViewResponse replaceView(TableIdentifier viewIdentifier, UpdateTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.REPLACE_VIEW;
    authorizeBasicTableLikeOperationOrThrow(op, PolarisEntitySubType.ICEBERG_VIEW, viewIdentifier);

    CatalogEntity catalog = getResolvedCatalogEntity();
    if (catalog.isStaticFacade()) {
      throw new BadRequestException("Cannot replace view on static-facade external catalogs.");
    }
    return catalogHandlerUtils.updateView(viewCatalog, viewIdentifier, applyUpdateFilters(request));
  }

  public void dropView(TableIdentifier viewIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_VIEW;
    authorizeBasicTableLikeOperationOrThrow(op, PolarisEntitySubType.ICEBERG_VIEW, viewIdentifier);

    catalogHandlerUtils.dropView(viewCatalog, viewIdentifier);
  }

  public void viewExists(TableIdentifier viewIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.VIEW_EXISTS;
    authorizeBasicTableLikeOperationOrThrow(op, PolarisEntitySubType.ICEBERG_VIEW, viewIdentifier);

    // TODO: Just skip CatalogHandlers for this one maybe
    catalogHandlerUtils.loadView(viewCatalog, viewIdentifier);
  }

  public void renameView(RenameTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.RENAME_VIEW;
    authorizeRenameTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_VIEW, request.source(), request.destination());

    CatalogEntity catalog = getResolvedCatalogEntity();
    if (catalog.isStaticFacade()) {
      throw new BadRequestException("Cannot rename view on static-facade external catalogs.");
    }
    catalogHandlerUtils.renameView(viewCatalog, request);
  }

  private @Nonnull LoadTableResponse filterResponseToSnapshots(
      LoadTableResponse loadTableResponse, String snapshots) {
    if (snapshots == null || snapshots.equalsIgnoreCase(SNAPSHOTS_ALL)) {
      return loadTableResponse;
    } else if (snapshots.equalsIgnoreCase(SNAPSHOTS_REFS)) {
      TableMetadata metadata = loadTableResponse.tableMetadata();

      Set<Long> referencedSnapshotIds =
          metadata.refs().values().stream()
              .map(SnapshotRef::snapshotId)
              .collect(Collectors.toSet());

      TableMetadata filteredMetadata =
          metadata.removeSnapshotsIf(s -> !referencedSnapshotIds.contains(s.snapshotId()));

      return LoadTableResponse.builder()
          .withTableMetadata(filteredMetadata)
          .addAllConfig(loadTableResponse.config())
          .addAllCredentials(loadTableResponse.credentials())
          .build();
    } else {
      throw new IllegalArgumentException("Unrecognized snapshots: " + snapshots);
    }
  }

  private EnumSet<PolarisAuthorizableOperation> getUpdateTableAuthorizableOperations(
      UpdateTableRequest request) {
    boolean useFineGrainedOperations =
        realmConfig.getConfig(
            FeatureConfiguration.ENABLE_FINE_GRAINED_UPDATE_TABLE_PRIVILEGES,
            getResolvedCatalogEntity());

    if (useFineGrainedOperations) {
      EnumSet<PolarisAuthorizableOperation> actions =
          request.updates().stream()
              .map(
                  update ->
                      switch (update) {
                        case MetadataUpdate.AssignUUID assignUuid ->
                            PolarisAuthorizableOperation.ASSIGN_TABLE_UUID;
                        case MetadataUpdate.UpgradeFormatVersion upgradeFormat ->
                            PolarisAuthorizableOperation.UPGRADE_TABLE_FORMAT_VERSION;
                        case MetadataUpdate.AddSchema addSchema ->
                            PolarisAuthorizableOperation.ADD_TABLE_SCHEMA;
                        case MetadataUpdate.SetCurrentSchema setCurrentSchema ->
                            PolarisAuthorizableOperation.SET_TABLE_CURRENT_SCHEMA;
                        case MetadataUpdate.AddPartitionSpec addPartitionSpec ->
                            PolarisAuthorizableOperation.ADD_TABLE_PARTITION_SPEC;
                        case MetadataUpdate.AddSortOrder addSortOrder ->
                            PolarisAuthorizableOperation.ADD_TABLE_SORT_ORDER;
                        case MetadataUpdate.SetDefaultSortOrder setDefaultSortOrder ->
                            PolarisAuthorizableOperation.SET_TABLE_DEFAULT_SORT_ORDER;
                        case MetadataUpdate.AddSnapshot addSnapshot ->
                            PolarisAuthorizableOperation.ADD_TABLE_SNAPSHOT;
                        case MetadataUpdate.SetSnapshotRef setSnapshotRef ->
                            PolarisAuthorizableOperation.SET_TABLE_SNAPSHOT_REF;
                        case MetadataUpdate.RemoveSnapshots removeSnapshots ->
                            PolarisAuthorizableOperation.REMOVE_TABLE_SNAPSHOTS;
                        case MetadataUpdate.RemoveSnapshotRef removeSnapshotRef ->
                            PolarisAuthorizableOperation.REMOVE_TABLE_SNAPSHOT_REF;
                        case MetadataUpdate.SetLocation setLocation ->
                            PolarisAuthorizableOperation.SET_TABLE_LOCATION;
                        case MetadataUpdate.SetProperties setProperties ->
                            PolarisAuthorizableOperation.SET_TABLE_PROPERTIES;
                        case MetadataUpdate.RemoveProperties removeProperties ->
                            PolarisAuthorizableOperation.REMOVE_TABLE_PROPERTIES;
                        case MetadataUpdate.SetStatistics setStatistics ->
                            PolarisAuthorizableOperation.SET_TABLE_STATISTICS;
                        case MetadataUpdate.RemoveStatistics removeStatistics ->
                            PolarisAuthorizableOperation.REMOVE_TABLE_STATISTICS;
                        case MetadataUpdate.RemovePartitionSpecs removePartitionSpecs ->
                            PolarisAuthorizableOperation.REMOVE_TABLE_PARTITION_SPECS;
                        default ->
                            PolarisAuthorizableOperation
                                .UPDATE_TABLE; // Fallback for unknown update types
                      })
              .collect(
                  () -> EnumSet.noneOf(PolarisAuthorizableOperation.class),
                  EnumSet::add,
                  EnumSet::addAll);

      // If there are no MetadataUpdates, then default to the UPDATE_TABLE operation.
      if (actions.isEmpty()) {
        actions.add(PolarisAuthorizableOperation.UPDATE_TABLE);
      }

      return actions;
    } else {
      return EnumSet.of(PolarisAuthorizableOperation.UPDATE_TABLE);
    }
  }

  private void checkAllowExternalCatalogCredentialVending(
      EnumSet<AccessDelegationMode> delegationModes) {

    if (delegationModes.isEmpty()) {
      return;
    }
    CatalogEntity catalogEntity = getResolvedCatalogEntity();

    LOGGER.info("Catalog type: {}", catalogEntity.getCatalogType());
    LOGGER.info(
        "allow external catalog credential vending: {}",
        realmConfig.getConfig(
            FeatureConfiguration.ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING, catalogEntity));
    if (catalogEntity
            .getCatalogType()
            .equals(org.apache.polaris.core.admin.model.Catalog.TypeEnum.EXTERNAL)
        && !realmConfig.getConfig(
            FeatureConfiguration.ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING, catalogEntity)) {
      throw new ForbiddenException(
          "Access Delegation is not enabled for this catalog. Please consult applicable "
              + "documentation for the catalog config property '%s' to enable this feature",
          FeatureConfiguration.ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING.catalogConfig());
    }
  }

  @Override
  public void close() throws Exception {
    if (baseCatalog instanceof Closeable closeable) {
      closeable.close();
    }
  }
}
