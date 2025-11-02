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

import static org.apache.polaris.service.catalog.AccessDelegationMode.VENDED_CREDENTIALS;
import static org.apache.polaris.service.catalog.validation.IcebergPropertiesValidation.validateIcebergProperties;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.ImmutableCreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ImmutableLoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.catalog.ExternalCatalogFactory;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.Resolver;
import org.apache.polaris.core.persistence.resolver.ResolverFactory;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.core.rest.PolarisEndpoints;
import org.apache.polaris.core.rest.PolarisResourcePaths;
import org.apache.polaris.service.catalog.AccessDelegationMode;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.api.IcebergRestCatalogApiService;
import org.apache.polaris.service.catalog.api.IcebergRestConfigurationApiService;
import org.apache.polaris.service.catalog.common.CatalogAdapter;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.context.catalog.CallContextCatalogFactory;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.apache.polaris.service.http.IcebergHttpUtil;
import org.apache.polaris.service.http.IfNoneMatch;
import org.apache.polaris.service.types.CommitTableRequest;
import org.apache.polaris.service.types.CommitViewRequest;
import org.apache.polaris.service.types.NotificationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An adapter between generated service types like `IcebergRestCatalogApiService` and
 * `IcebergCatalogHandler`.
 */
@RequestScoped
public class IcebergCatalogAdapter
    implements IcebergRestCatalogApiService, IcebergRestConfigurationApiService, CatalogAdapter {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergCatalogAdapter.class);

  private static final Set<Endpoint> DEFAULT_ENDPOINTS =
      ImmutableSet.<Endpoint>builder()
          .add(Endpoint.V1_LIST_NAMESPACES)
          .add(Endpoint.V1_LOAD_NAMESPACE)
          .add(Endpoint.V1_NAMESPACE_EXISTS)
          .add(Endpoint.V1_CREATE_NAMESPACE)
          .add(Endpoint.V1_UPDATE_NAMESPACE)
          .add(Endpoint.V1_DELETE_NAMESPACE)
          .add(Endpoint.V1_LIST_TABLES)
          .add(Endpoint.V1_LOAD_TABLE)
          .add(Endpoint.V1_TABLE_EXISTS)
          .add(Endpoint.V1_CREATE_TABLE)
          .add(Endpoint.V1_UPDATE_TABLE)
          .add(Endpoint.V1_DELETE_TABLE)
          .add(Endpoint.V1_RENAME_TABLE)
          .add(Endpoint.V1_REGISTER_TABLE)
          .add(Endpoint.V1_REPORT_METRICS)
          .add(Endpoint.V1_COMMIT_TRANSACTION)
          .build();

  private static final Set<Endpoint> VIEW_ENDPOINTS =
      ImmutableSet.<Endpoint>builder()
          .add(Endpoint.V1_LIST_VIEWS)
          .add(Endpoint.V1_LOAD_VIEW)
          .add(Endpoint.V1_VIEW_EXISTS)
          .add(Endpoint.V1_CREATE_VIEW)
          .add(Endpoint.V1_UPDATE_VIEW)
          .add(Endpoint.V1_DELETE_VIEW)
          .add(Endpoint.V1_RENAME_VIEW)
          .build();

  private final PolarisDiagnostics diagnostics;
  private final RealmContext realmContext;
  private final CallContext callContext;
  private final RealmConfig realmConfig;
  private final CallContextCatalogFactory catalogFactory;
  private final ResolutionManifestFactory resolutionManifestFactory;
  private final ResolverFactory resolverFactory;
  private final PolarisMetaStoreManager metaStoreManager;
  private final PolarisCredentialManager credentialManager;
  private final PolarisAuthorizer polarisAuthorizer;
  private final CatalogPrefixParser prefixParser;
  private final ReservedProperties reservedProperties;
  private final CatalogHandlerUtils catalogHandlerUtils;
  private final Instance<ExternalCatalogFactory> externalCatalogFactories;
  private final PolarisEventListener polarisEventListener;
  private final StorageAccessConfigProvider storageAccessConfigProvider;

  @Inject
  public IcebergCatalogAdapter(
      PolarisDiagnostics diagnostics,
      RealmContext realmContext,
      CallContext callContext,
      CallContextCatalogFactory catalogFactory,
      ResolverFactory resolverFactory,
      ResolutionManifestFactory resolutionManifestFactory,
      PolarisMetaStoreManager metaStoreManager,
      PolarisCredentialManager credentialManager,
      PolarisAuthorizer polarisAuthorizer,
      CatalogPrefixParser prefixParser,
      ReservedProperties reservedProperties,
      CatalogHandlerUtils catalogHandlerUtils,
      @Any Instance<ExternalCatalogFactory> externalCatalogFactories,
      PolarisEventListener polarisEventListener,
      StorageAccessConfigProvider storageAccessConfigProvider) {
    this.diagnostics = diagnostics;
    this.realmContext = realmContext;
    this.callContext = callContext;
    this.realmConfig = callContext.getRealmConfig();
    this.catalogFactory = catalogFactory;
    this.resolutionManifestFactory = resolutionManifestFactory;
    this.resolverFactory = resolverFactory;
    this.metaStoreManager = metaStoreManager;
    this.credentialManager = credentialManager;
    this.polarisAuthorizer = polarisAuthorizer;
    this.prefixParser = prefixParser;
    this.reservedProperties = reservedProperties;
    this.catalogHandlerUtils = catalogHandlerUtils;
    this.externalCatalogFactories = externalCatalogFactories;
    this.polarisEventListener = polarisEventListener;
    this.storageAccessConfigProvider = storageAccessConfigProvider;
  }

  /**
   * Execute operations on a catalog wrapper and ensure we close the BaseCatalog afterward. This
   * will typically ensure the underlying FileIO is closed.
   */
  private Response withCatalog(
      SecurityContext securityContext,
      String prefix,
      Function<IcebergCatalogHandler, Response> action) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    try (IcebergCatalogHandler wrapper = newHandlerWrapper(securityContext, catalogName)) {
      return action.apply(wrapper);
    } catch (RuntimeException e) {
      LOGGER.debug("RuntimeException while operating on catalog. Propagating to caller.", e);
      throw e;
    } catch (Exception e) {
      LOGGER.error("Error while operating on catalog", e);
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  IcebergCatalogHandler newHandlerWrapper(SecurityContext securityContext, String catalogName) {
    validatePrincipal(securityContext);

    return new IcebergCatalogHandler(
        diagnostics,
        callContext,
        resolutionManifestFactory,
        metaStoreManager,
        credentialManager,
        securityContext,
        catalogFactory,
        catalogName,
        polarisAuthorizer,
        reservedProperties,
        catalogHandlerUtils,
        externalCatalogFactories,
        polarisEventListener,
        storageAccessConfigProvider);
  }

  @Override
  public Response createNamespace(
      String prefix,
      CreateNamespaceRequest createNamespaceRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    validateIcebergProperties(realmConfig, createNamespaceRequest.properties());
    return withCatalog(
        securityContext,
        prefix,
        catalog -> Response.ok(catalog.createNamespace(createNamespaceRequest)).build());
  }

  @Override
  public Response listNamespaces(
      String prefix,
      String pageToken,
      Integer pageSize,
      String parent,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Optional<Namespace> namespaceOptional = Optional.ofNullable(parent).map(this::decodeNamespace);
    return withCatalog(
        securityContext,
        prefix,
        catalog ->
            Response.ok(
                    catalog.listNamespaces(
                        namespaceOptional.orElse(Namespace.of()), pageToken, pageSize))
                .build());
  }

  @Override
  public Response loadNamespaceMetadata(
      String prefix, String namespace, RealmContext realmContext, SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    return withCatalog(
        securityContext, prefix, catalog -> Response.ok(catalog.loadNamespaceMetadata(ns)).build());
  }

  /**
   * For situations where we typically expect a metadataLocation to be present in the response and
   * so expect to insert an etag header, this helper gracefully falls back to omitting the header if
   * unable to get metadata location and logs a warning.
   */
  private Response.ResponseBuilder tryInsertETagHeader(
      Response.ResponseBuilder builder,
      LoadTableResponse response,
      String namespace,
      String tableName) {
    if (response.metadataLocation() != null) {
      builder =
          builder.header(
              HttpHeaders.ETAG,
              IcebergHttpUtil.generateETagForMetadataFileLocation(response.metadataLocation()));
    } else {
      LOGGER
          .atWarn()
          .addKeyValue("namespace", namespace)
          .addKeyValue("tableName", tableName)
          .log("Response has null metadataLocation; omitting etag");
    }
    return builder;
  }

  @Override
  public Response namespaceExists(
      String prefix, String namespace, RealmContext realmContext, SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    return withCatalog(
        securityContext,
        prefix,
        catalog -> {
          catalog.namespaceExists(ns);
          return Response.status(Response.Status.NO_CONTENT).build();
        });
  }

  @Override
  public Response dropNamespace(
      String prefix, String namespace, RealmContext realmContext, SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    return withCatalog(
        securityContext,
        prefix,
        catalog -> {
          catalog.dropNamespace(ns);
          return Response.status(Response.Status.NO_CONTENT).build();
        });
  }

  @Override
  public Response updateProperties(
      String prefix,
      String namespace,
      UpdateNamespacePropertiesRequest updateNamespacePropertiesRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    validateIcebergProperties(realmConfig, updateNamespacePropertiesRequest.updates());
    Namespace ns = decodeNamespace(namespace);
    UpdateNamespacePropertiesRequest revisedRequest =
        UpdateNamespacePropertiesRequest.builder()
            .removeAll(
                reservedProperties.removeReservedProperties(
                    updateNamespacePropertiesRequest.removals()))
            .updateAll(
                reservedProperties.removeReservedProperties(
                    updateNamespacePropertiesRequest.updates()))
            .build();
    return withCatalog(
        securityContext,
        prefix,
        catalog -> Response.ok(catalog.updateNamespaceProperties(ns, revisedRequest)).build());
  }

  private EnumSet<AccessDelegationMode> parseAccessDelegationModes(String accessDelegationMode) {
    EnumSet<AccessDelegationMode> delegationModes =
        AccessDelegationMode.fromProtocolValuesList(accessDelegationMode);
    Preconditions.checkArgument(
        delegationModes.isEmpty() || delegationModes.contains(VENDED_CREDENTIALS),
        "Unsupported access delegation mode: %s",
        accessDelegationMode);
    return delegationModes;
  }

  @Override
  public Response createTable(
      String prefix,
      String namespace,
      CreateTableRequest createTableRequest,
      String accessDelegationMode,
      RealmContext realmContext,
      SecurityContext securityContext) {
    validateIcebergProperties(realmConfig, createTableRequest.properties());
    EnumSet<AccessDelegationMode> delegationModes =
        parseAccessDelegationModes(accessDelegationMode);
    Namespace ns = decodeNamespace(namespace);
    return withCatalog(
        securityContext,
        prefix,
        catalog -> {
          Optional<String> refreshCredentialsEndpoint =
              getRefreshCredentialsEndpoint(
                  delegationModes,
                  prefix,
                  TableIdentifier.of(namespace, createTableRequest.name()));
          if (createTableRequest.stageCreate()) {
            return Response.ok(
                    catalog.createTableStaged(
                        ns, createTableRequest, delegationModes, refreshCredentialsEndpoint))
                .build();
          } else {
            LoadTableResponse response =
                catalog.createTableDirect(
                    ns, createTableRequest, delegationModes, refreshCredentialsEndpoint);
            return tryInsertETagHeader(
                    Response.ok(response), response, namespace, createTableRequest.name())
                .build();
          }
        });
  }

  @Override
  public Response listTables(
      String prefix,
      String namespace,
      String pageToken,
      Integer pageSize,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    return withCatalog(
        securityContext,
        prefix,
        catalog -> Response.ok(catalog.listTables(ns, pageToken, pageSize)).build());
  }

  @Override
  public Response loadTable(
      String prefix,
      String namespace,
      String table,
      String accessDelegationMode,
      String ifNoneMatchString,
      String snapshots,
      RealmContext realmContext,
      SecurityContext securityContext) {
    EnumSet<AccessDelegationMode> delegationModes =
        parseAccessDelegationModes(accessDelegationMode);
    Namespace ns = decodeNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(ns, RESTUtil.decodeString(table));

    IfNoneMatch ifNoneMatch = IfNoneMatch.fromHeader(ifNoneMatchString);

    if (ifNoneMatch.isWildcard()) {
      throw new BadRequestException("If-None-Match may not take the value of '*'");
    }

    return withCatalog(
        securityContext,
        prefix,
        catalog -> {
          Optional<LoadTableResponse> response =
              catalog.loadTable(
                  tableIdentifier,
                  snapshots,
                  ifNoneMatch,
                  delegationModes,
                  getRefreshCredentialsEndpoint(delegationModes, prefix, tableIdentifier));

          if (response.isEmpty()) {
            return Response.notModified().build();
          }

          return tryInsertETagHeader(Response.ok(response.get()), response.get(), namespace, table)
              .build();
        });
  }

  private static Optional<String> getRefreshCredentialsEndpoint(
      EnumSet<AccessDelegationMode> delegationModes,
      String prefix,
      TableIdentifier tableIdentifier) {
    return delegationModes.contains(AccessDelegationMode.VENDED_CREDENTIALS)
        ? Optional.of(new PolarisResourcePaths(prefix).credentialsPath(tableIdentifier))
        : Optional.empty();
  }

  @Override
  public Response tableExists(
      String prefix,
      String namespace,
      String table,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(ns, RESTUtil.decodeString(table));
    return withCatalog(
        securityContext,
        prefix,
        catalog -> {
          catalog.tableExists(tableIdentifier);
          return Response.status(Response.Status.NO_CONTENT).build();
        });
  }

  @Override
  public Response dropTable(
      String prefix,
      String namespace,
      String table,
      Boolean purgeRequested,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(ns, RESTUtil.decodeString(table));
    return withCatalog(
        securityContext,
        prefix,
        catalog -> {
          if (purgeRequested != null && purgeRequested) {
            catalog.dropTableWithPurge(tableIdentifier);
          } else {
            catalog.dropTableWithoutPurge(tableIdentifier);
          }
          return Response.status(Response.Status.NO_CONTENT).build();
        });
  }

  @Override
  public Response registerTable(
      String prefix,
      String namespace,
      RegisterTableRequest registerTableRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    return withCatalog(
        securityContext,
        prefix,
        catalog -> {
          LoadTableResponse response = catalog.registerTable(ns, registerTableRequest);
          return tryInsertETagHeader(
                  Response.ok(response), response, namespace, registerTableRequest.name())
              .build();
        });
  }

  @Override
  public Response renameTable(
      String prefix,
      RenameTableRequest renameTableRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return withCatalog(
        securityContext,
        prefix,
        catalog -> {
          catalog.renameTable(renameTableRequest);
          return Response.ok(Response.Status.NO_CONTENT).build();
        });
  }

  @Override
  public Response updateTable(
      String prefix,
      String namespace,
      String table,
      CommitTableRequest commitTableRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    commitTableRequest.updates().stream()
        .filter(MetadataUpdate.SetProperties.class::isInstance)
        .map(MetadataUpdate.SetProperties.class::cast)
        .forEach(setProperties -> validateIcebergProperties(realmConfig, setProperties.updated()));

    UpdateTableRequest revisedRequest =
        UpdateTableRequest.create(
            commitTableRequest.identifier(),
            commitTableRequest.requirements(),
            commitTableRequest.updates().stream()
                .map(reservedProperties::removeReservedProperties)
                .toList());
    Namespace ns = decodeNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(ns, RESTUtil.decodeString(table));
    return withCatalog(
        securityContext,
        prefix,
        catalog -> {
          if (IcebergCatalogHandler.isCreate(revisedRequest)) {
            return Response.ok(catalog.updateTableForStagedCreate(tableIdentifier, revisedRequest))
                .build();
          } else {
            return Response.ok(catalog.updateTable(tableIdentifier, revisedRequest)).build();
          }
        });
  }

  @Override
  public Response createView(
      String prefix,
      String namespace,
      CreateViewRequest createViewRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    validateIcebergProperties(realmConfig, createViewRequest.properties());

    CreateViewRequest revisedRequest =
        ImmutableCreateViewRequest.copyOf(createViewRequest)
            .withProperties(
                reservedProperties.removeReservedProperties(createViewRequest.properties()));
    Namespace ns = decodeNamespace(namespace);
    return withCatalog(
        securityContext,
        prefix,
        catalog -> Response.ok(catalog.createView(ns, revisedRequest)).build());
  }

  @Override
  public Response listViews(
      String prefix,
      String namespace,
      String pageToken,
      Integer pageSize,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    return withCatalog(
        securityContext,
        prefix,
        catalog -> Response.ok(catalog.listViews(ns, pageToken, pageSize)).build());
  }

  @Override
  public Response loadCredentials(
      String prefix,
      String namespace,
      String table,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(ns, RESTUtil.decodeString(table));
    return withCatalog(
        securityContext,
        prefix,
        catalog -> {
          LoadTableResponse loadTableResponse =
              catalog.loadTableWithAccessDelegation(
                  tableIdentifier,
                  "all",
                  Optional.of(new PolarisResourcePaths(prefix).credentialsPath(tableIdentifier)));
          return Response.ok(
                  ImmutableLoadCredentialsResponse.builder()
                      .credentials(loadTableResponse.credentials())
                      .build())
              .build();
        });
  }

  @Override
  public Response loadView(
      String prefix,
      String namespace,
      String view,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(ns, RESTUtil.decodeString(view));
    return withCatalog(
        securityContext, prefix, catalog -> Response.ok(catalog.loadView(tableIdentifier)).build());
  }

  @Override
  public Response viewExists(
      String prefix,
      String namespace,
      String view,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(ns, RESTUtil.decodeString(view));
    return withCatalog(
        securityContext,
        prefix,
        catalog -> {
          catalog.viewExists(tableIdentifier);
          return Response.status(Response.Status.NO_CONTENT).build();
        });
  }

  @Override
  public Response dropView(
      String prefix,
      String namespace,
      String view,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(ns, RESTUtil.decodeString(view));
    return withCatalog(
        securityContext,
        prefix,
        catalog -> {
          catalog.dropView(tableIdentifier);
          return Response.status(Response.Status.NO_CONTENT).build();
        });
  }

  @Override
  public Response renameView(
      String prefix,
      RenameTableRequest renameTableRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return withCatalog(
        securityContext,
        prefix,
        catalog -> {
          catalog.renameView(renameTableRequest);
          return Response.status(Response.Status.NO_CONTENT).build();
        });
  }

  @Override
  public Response replaceView(
      String prefix,
      String namespace,
      String view,
      CommitViewRequest commitViewRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    UpdateTableRequest revisedRequest =
        UpdateTableRequest.create(
            commitViewRequest.identifier(),
            commitViewRequest.requirements(),
            commitViewRequest.updates().stream()
                .map(reservedProperties::removeReservedProperties)
                .toList());
    Namespace ns = decodeNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(ns, RESTUtil.decodeString(view));
    return withCatalog(
        securityContext,
        prefix,
        catalog -> Response.ok(catalog.replaceView(tableIdentifier, revisedRequest)).build());
  }

  @Override
  public Response commitTransaction(
      String prefix,
      CommitTransactionRequest commitTransactionRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    commitTransactionRequest.tableChanges().stream()
        .flatMap(updateTableRequest -> updateTableRequest.updates().stream())
        .filter(MetadataUpdate.SetProperties.class::isInstance)
        .map(MetadataUpdate.SetProperties.class::cast)
        .forEach(setProperties -> validateIcebergProperties(realmConfig, setProperties.updated()));

    CommitTransactionRequest revisedRequest =
        new CommitTransactionRequest(
            commitTransactionRequest.tableChanges().stream()
                .map(
                    r -> {
                      return UpdateTableRequest.create(
                          r.identifier(),
                          r.requirements(),
                          r.updates().stream()
                              .map(reservedProperties::removeReservedProperties)
                              .toList());
                    })
                .toList());
    return withCatalog(
        securityContext,
        prefix,
        catalog -> {
          catalog.commitTransaction(revisedRequest);
          return Response.status(Response.Status.NO_CONTENT).build();
        });
  }

  @Override
  public Response reportMetrics(
      String prefix,
      String namespace,
      String table,
      ReportMetricsRequest reportMetricsRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  @Override
  public Response sendNotification(
      String prefix,
      String namespace,
      String table,
      NotificationRequest notificationRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(ns, RESTUtil.decodeString(table));
    return withCatalog(
        securityContext,
        prefix,
        catalog -> {
          catalog.sendNotification(tableIdentifier, notificationRequest);
          return Response.status(Response.Status.NO_CONTENT).build();
        });
  }

  /** From IcebergRestConfigurationApiService. */
  @Override
  public Response getConfig(
      String warehouse, RealmContext realmContext, SecurityContext securityContext) {
    // 'warehouse' as an input here is catalogName.
    // 'warehouse' as an output will be treated by the client as a default catalog
    // storage
    //    base location.
    // 'prefix' as an output is the REST subpath that routes to the catalog
    // resource,
    //    which may be URL-escaped catalogName or potentially a different unique
    // identifier for
    //    the catalog being accessed.
    // TODO: Push this down into PolarisCatalogHandlerWrapper for authorizing "any" catalog
    // role in this catalog.
    PolarisPrincipal authenticatedPrincipal = (PolarisPrincipal) securityContext.getUserPrincipal();
    if (authenticatedPrincipal == null) {
      throw new NotAuthorizedException("Failed to find authenticatedPrincipal in SecurityContext");
    }
    if (warehouse == null) {
      throw new BadRequestException("Please specify a warehouse");
    }
    Resolver resolver = resolverFactory.createResolver(securityContext, warehouse);
    ResolverStatus resolverStatus = resolver.resolveAll();
    if (!resolverStatus.getStatus().equals(ResolverStatus.StatusEnum.SUCCESS)) {
      throw new NotFoundException("Unable to find warehouse %s", warehouse);
    }
    ResolvedPolarisEntity resolvedReferenceCatalog = resolver.getResolvedReferenceCatalog();
    Map<String, String> properties =
        PolarisEntity.of(resolvedReferenceCatalog.getEntity()).getPropertiesAsMap();

    String prefix = prefixParser.catalogNameToPrefix(realmContext, warehouse);
    return Response.ok(
            ConfigResponse.builder()
                .withDefaults(properties) // catalog properties are defaults
                .withOverrides(ImmutableMap.of("prefix", prefix))
                .withEndpoints(
                    ImmutableList.<Endpoint>builder()
                        .addAll(DEFAULT_ENDPOINTS)
                        .addAll(VIEW_ENDPOINTS)
                        .addAll(PolarisEndpoints.getSupportedGenericTableEndpoints(realmConfig))
                        .addAll(PolarisEndpoints.getSupportedPolicyEndpoints(realmConfig))
                        .build())
                .build())
        .build();
  }
}
