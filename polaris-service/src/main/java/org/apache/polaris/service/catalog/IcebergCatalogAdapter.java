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

import static org.apache.polaris.service.catalog.AccessDelegationMode.VENDED_CREDENTIALS;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.cache.EntityCacheEntry;
import org.apache.polaris.core.persistence.resolver.Resolver;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.service.catalog.api.IcebergRestCatalogApiService;
import org.apache.polaris.service.catalog.api.IcebergRestConfigurationApiService;
import org.apache.polaris.service.config.RealmEntityManagerFactory;
import org.apache.polaris.service.context.CallContextCatalogFactory;
import org.apache.polaris.service.types.CommitTableRequest;
import org.apache.polaris.service.types.CommitViewRequest;
import org.apache.polaris.service.types.NotificationRequest;

/**
 * {@link IcebergRestCatalogApiService} implementation that delegates operations to {@link
 * org.apache.iceberg.rest.CatalogHandlers} after finding the appropriate {@link Catalog} for the
 * current {@link RealmContext}.
 */
public class IcebergCatalogAdapter
    implements IcebergRestCatalogApiService, IcebergRestConfigurationApiService {

  private static final Set<Endpoint> DEFAULT_ENDPOINTS =
      ImmutableSet.<Endpoint>builder()
          .add(Endpoint.V1_LIST_NAMESPACES)
          .add(Endpoint.V1_LOAD_NAMESPACE)
          .add(Endpoint.V1_CREATE_NAMESPACE)
          .add(Endpoint.V1_UPDATE_NAMESPACE)
          .add(Endpoint.V1_DELETE_NAMESPACE)
          .add(Endpoint.V1_LIST_TABLES)
          .add(Endpoint.V1_LOAD_TABLE)
          .add(Endpoint.V1_CREATE_TABLE)
          .add(Endpoint.V1_UPDATE_TABLE)
          .add(Endpoint.V1_DELETE_TABLE)
          .add(Endpoint.V1_RENAME_TABLE)
          .add(Endpoint.V1_REGISTER_TABLE)
          .add(Endpoint.V1_REPORT_METRICS)
          .build();

  private static final Set<Endpoint> VIEW_ENDPOINTS =
      ImmutableSet.<Endpoint>builder()
          .add(Endpoint.V1_LIST_VIEWS)
          .add(Endpoint.V1_LOAD_VIEW)
          .add(Endpoint.V1_CREATE_VIEW)
          .add(Endpoint.V1_UPDATE_VIEW)
          .add(Endpoint.V1_DELETE_VIEW)
          .add(Endpoint.V1_RENAME_VIEW)
          .build();

  private static final Set<Endpoint> COMMIT_ENDPOINT =
      ImmutableSet.<Endpoint>builder()
          .add(Endpoint.create("POST", ResourcePaths.V1_TRANSACTIONS_COMMIT))
          .build();

  private final CallContextCatalogFactory catalogFactory;
  private final MetaStoreManagerFactory metaStoreManagerFactory;
  private final RealmEntityManagerFactory entityManagerFactory;
  private final PolarisAuthorizer polarisAuthorizer;

  public IcebergCatalogAdapter(
      CallContextCatalogFactory catalogFactory,
      RealmEntityManagerFactory entityManagerFactory,
      MetaStoreManagerFactory metaStoreManagerFactory,
      PolarisAuthorizer polarisAuthorizer) {
    this.catalogFactory = catalogFactory;
    this.entityManagerFactory = entityManagerFactory;
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.polarisAuthorizer = polarisAuthorizer;
  }

  private PolarisCatalogHandlerWrapper newHandlerWrapper(
      SecurityContext securityContext, String catalogName) {
    CallContext callContext = CallContext.getCurrentContext();
    AuthenticatedPolarisPrincipal authenticatedPrincipal =
        (AuthenticatedPolarisPrincipal) securityContext.getUserPrincipal();
    if (authenticatedPrincipal == null) {
      throw new NotAuthorizedException("Failed to find authenticatedPrincipal in SecurityContext");
    }

    PolarisEntityManager entityManager =
        entityManagerFactory.getOrCreateEntityManager(callContext.getRealmContext());

    return new PolarisCatalogHandlerWrapper(
        callContext,
        entityManager,
        metaStoreManagerFactory.getOrCreateMetaStoreManager(callContext.getRealmContext()),
        authenticatedPrincipal,
        catalogFactory,
        catalogName,
        polarisAuthorizer);
  }

  @Override
  public Response createNamespace(
      String prefix,
      CreateNamespaceRequest createNamespaceRequest,
      SecurityContext securityContext) {
    return Response.ok(
            newHandlerWrapper(securityContext, prefix).createNamespace(createNamespaceRequest))
        .build();
  }

  @Override
  public Response listNamespaces(
      String prefix,
      String pageToken,
      Integer pageSize,
      String parent,
      SecurityContext securityContext) {
    Optional<Namespace> namespaceOptional =
        Optional.ofNullable(parent).map(IcebergCatalogAdapter::decodeNamespace);
    return Response.ok(
            newHandlerWrapper(securityContext, prefix)
                .listNamespaces(namespaceOptional.orElse(Namespace.of())))
        .build();
  }

  @Override
  public Response loadNamespaceMetadata(
      String prefix, String namespace, SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    return Response.ok(newHandlerWrapper(securityContext, prefix).loadNamespaceMetadata(ns))
        .build();
  }

  private static Namespace decodeNamespace(String namespace) {
    return RESTUtil.decodeNamespace(URLEncoder.encode(namespace, Charset.defaultCharset()));
  }

  @Override
  public Response namespaceExists(
      String prefix, String namespace, SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    newHandlerWrapper(securityContext, prefix).namespaceExists(ns);
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  @Override
  public Response dropNamespace(String prefix, String namespace, SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    newHandlerWrapper(securityContext, prefix).dropNamespace(ns);
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  @Override
  public Response updateProperties(
      String prefix,
      String namespace,
      UpdateNamespacePropertiesRequest updateNamespacePropertiesRequest,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    return Response.ok(
            newHandlerWrapper(securityContext, prefix)
                .updateNamespaceProperties(ns, updateNamespacePropertiesRequest))
        .build();
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
      SecurityContext securityContext) {
    EnumSet<AccessDelegationMode> delegationModes =
        parseAccessDelegationModes(accessDelegationMode);
    Namespace ns = decodeNamespace(namespace);
    if (createTableRequest.stageCreate()) {
      if (delegationModes.isEmpty()) {
        return Response.ok(
                newHandlerWrapper(securityContext, prefix)
                    .createTableStaged(ns, createTableRequest))
            .build();
      } else {
        return Response.ok(
                newHandlerWrapper(securityContext, prefix)
                    .createTableStagedWithWriteDelegation(ns, createTableRequest))
            .build();
      }
    } else if (delegationModes.isEmpty()) {
      return Response.ok(
              newHandlerWrapper(securityContext, prefix).createTableDirect(ns, createTableRequest))
          .build();
    } else {
      return Response.ok(
              newHandlerWrapper(securityContext, prefix)
                  .createTableDirectWithWriteDelegation(ns, createTableRequest))
          .build();
    }
  }

  @Override
  public Response listTables(
      String prefix,
      String namespace,
      String pageToken,
      Integer pageSize,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    return Response.ok(newHandlerWrapper(securityContext, prefix).listTables(ns)).build();
  }

  @Override
  public Response loadTable(
      String prefix,
      String namespace,
      String table,
      String accessDelegationMode,
      String snapshots,
      SecurityContext securityContext) {
    EnumSet<AccessDelegationMode> delegationModes =
        parseAccessDelegationModes(accessDelegationMode);
    Namespace ns = decodeNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(ns, RESTUtil.decodeString(table));
    if (delegationModes.isEmpty()) {
      return Response.ok(
              newHandlerWrapper(securityContext, prefix).loadTable(tableIdentifier, snapshots))
          .build();
    } else {
      return Response.ok(
              newHandlerWrapper(securityContext, prefix)
                  .loadTableWithAccessDelegation(tableIdentifier, snapshots))
          .build();
    }
  }

  @Override
  public Response tableExists(
      String prefix, String namespace, String table, SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(ns, RESTUtil.decodeString(table));
    newHandlerWrapper(securityContext, prefix).tableExists(tableIdentifier);
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  @Override
  public Response dropTable(
      String prefix,
      String namespace,
      String table,
      Boolean purgeRequested,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(ns, RESTUtil.decodeString(table));

    if (purgeRequested != null && purgeRequested) {
      newHandlerWrapper(securityContext, prefix).dropTableWithPurge(tableIdentifier);
    } else {
      newHandlerWrapper(securityContext, prefix).dropTableWithoutPurge(tableIdentifier);
    }
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  @Override
  public Response registerTable(
      String prefix,
      String namespace,
      RegisterTableRequest registerTableRequest,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    return Response.ok(
            newHandlerWrapper(securityContext, prefix).registerTable(ns, registerTableRequest))
        .build();
  }

  @Override
  public Response renameTable(
      String prefix, RenameTableRequest renameTableRequest, SecurityContext securityContext) {
    newHandlerWrapper(securityContext, prefix).renameTable(renameTableRequest);
    return Response.ok(javax.ws.rs.core.Response.Status.NO_CONTENT).build();
  }

  @Override
  public Response updateTable(
      String prefix,
      String namespace,
      String table,
      CommitTableRequest commitTableRequest,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(ns, RESTUtil.decodeString(table));

    if (PolarisCatalogHandlerWrapper.isCreate(commitTableRequest)) {
      return Response.ok(
              newHandlerWrapper(securityContext, prefix)
                  .updateTableForStagedCreate(tableIdentifier, commitTableRequest))
          .build();
    } else {
      return Response.ok(
              newHandlerWrapper(securityContext, prefix)
                  .updateTable(tableIdentifier, commitTableRequest))
          .build();
    }
  }

  @Override
  public Response createView(
      String prefix,
      String namespace,
      CreateViewRequest createViewRequest,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    return Response.ok(newHandlerWrapper(securityContext, prefix).createView(ns, createViewRequest))
        .build();
  }

  @Override
  public Response listViews(
      String prefix,
      String namespace,
      String pageToken,
      Integer pageSize,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    return Response.ok(newHandlerWrapper(securityContext, prefix).listViews(ns)).build();
  }

  @Override
  public Response loadView(
      String prefix, String namespace, String view, SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(ns, RESTUtil.decodeString(view));
    return Response.ok(newHandlerWrapper(securityContext, prefix).loadView(tableIdentifier))
        .build();
  }

  @Override
  public Response viewExists(
      String prefix, String namespace, String view, SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(ns, RESTUtil.decodeString(view));
    newHandlerWrapper(securityContext, prefix).viewExists(tableIdentifier);
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  @Override
  public Response dropView(
      String prefix, String namespace, String view, SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(ns, RESTUtil.decodeString(view));
    newHandlerWrapper(securityContext, prefix).dropView(tableIdentifier);
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  @Override
  public Response renameView(
      String prefix, RenameTableRequest renameTableRequest, SecurityContext securityContext) {
    newHandlerWrapper(securityContext, prefix).renameView(renameTableRequest);
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  @Override
  public Response replaceView(
      String prefix,
      String namespace,
      String view,
      CommitViewRequest commitViewRequest,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(ns, RESTUtil.decodeString(view));
    return Response.ok(
            newHandlerWrapper(securityContext, prefix)
                .replaceView(tableIdentifier, commitViewRequest))
        .build();
  }

  @Override
  public Response commitTransaction(
      String prefix,
      CommitTransactionRequest commitTransactionRequest,
      SecurityContext securityContext) {
    newHandlerWrapper(securityContext, prefix).commitTransaction(commitTransactionRequest);
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  @Override
  public Response reportMetrics(
      String prefix,
      String namespace,
      String table,
      ReportMetricsRequest reportMetricsRequest,
      SecurityContext securityContext) {
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  @Override
  public Response sendNotification(
      String prefix,
      String namespace,
      String table,
      NotificationRequest notificationRequest,
      SecurityContext securityContext) {
    Namespace ns = decodeNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(ns, RESTUtil.decodeString(table));
    newHandlerWrapper(securityContext, prefix)
        .sendNotification(tableIdentifier, notificationRequest);
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  /** From IcebergRestConfigurationApiService. */
  @Override
  public Response getConfig(String warehouse, SecurityContext securityContext) {
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
    PolarisEntityManager entityManager =
        entityManagerFactory.getOrCreateEntityManager(
            CallContext.getCurrentContext().getRealmContext());
    AuthenticatedPolarisPrincipal authenticatedPrincipal =
        (AuthenticatedPolarisPrincipal) securityContext.getUserPrincipal();
    if (authenticatedPrincipal == null) {
      throw new NotAuthorizedException("Failed to find authenticatedPrincipal in SecurityContext");
    }
    if (warehouse == null) {
      throw new BadRequestException("Please specify a warehouse");
    }
    Resolver resolver =
        entityManager.prepareResolver(
            CallContext.getCurrentContext(), authenticatedPrincipal, warehouse);
    ResolverStatus resolverStatus = resolver.resolveAll();
    if (!resolverStatus.getStatus().equals(ResolverStatus.StatusEnum.SUCCESS)) {
      throw new NotFoundException("Unable to find warehouse %s", warehouse);
    }
    EntityCacheEntry resolvedReferenceCatalog = resolver.getResolvedReferenceCatalog();
    Map<String, String> properties =
        PolarisEntity.of(resolvedReferenceCatalog.getEntity()).getPropertiesAsMap();

    return Response.ok(
            ConfigResponse.builder()
                .withDefaults(properties) // catalog properties are defaults
                .withOverrides(ImmutableMap.of("prefix", warehouse))
                .withEndpoints(
                    ImmutableList.<Endpoint>builder()
                        .addAll(DEFAULT_ENDPOINTS)
                        .addAll(VIEW_ENDPOINTS)
                        .addAll(COMMIT_ENDPOINT)
                        .build())
                .build())
        .build();
  }
}
