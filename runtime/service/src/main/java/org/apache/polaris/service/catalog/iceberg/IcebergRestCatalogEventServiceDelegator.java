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

import jakarta.annotation.Priority;
import jakarta.decorator.Decorator;
import jakarta.decorator.Delegate;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.api.IcebergRestCatalogApiService;
import org.apache.polaris.service.catalog.common.CatalogAdapter;
import org.apache.polaris.service.events.IcebergRestCatalogEvents;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterCheckExistsNamespaceEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterCheckExistsTableEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterCheckExistsViewEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterCreateNamespaceEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterCreateTableEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterCreateViewEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterDropNamespaceEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterDropTableEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterDropViewEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterListNamespacesEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterListTablesEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterListViewsEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterLoadCredentialsEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterLoadNamespaceMetadataEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterLoadTableEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterLoadViewEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterRegisterTableEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterRenameTableEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterRenameViewEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterReplaceViewEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterSendNotificationEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterUpdateNamespacePropertiesEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterUpdateTableEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeCheckExistsNamespaceEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeCheckExistsTableEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeCheckExistsViewEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeCreateNamespaceEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeCreateTableEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeCreateViewEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeDropNamespaceEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeDropTableEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeDropViewEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeListNamespacesEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeListTablesEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeListViewsEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeLoadCredentialsEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeLoadNamespaceMetadataEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeLoadTableEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeLoadViewEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeRegisterTableEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeRenameTableEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeRenameViewEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeReplaceViewEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeSendNotificationEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeUpdateNamespacePropertiesEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeUpdateTableEvent;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.apache.polaris.service.types.CommitTableRequest;
import org.apache.polaris.service.types.CommitViewRequest;
import org.apache.polaris.service.types.NotificationRequest;

@Decorator
@Priority(1000)
public class IcebergRestCatalogEventServiceDelegator
    implements IcebergRestCatalogApiService, CatalogAdapter {

  @Inject @Delegate IcebergCatalogAdapter delegate;
  @Inject PolarisEventListener polarisEventListener;
  @Inject PolarisEventMetadataFactory eventMetadataFactory;
  @Inject CatalogPrefixParser prefixParser;

  @Override
  public Response createNamespace(
      String prefix,
      CreateNamespaceRequest createNamespaceRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    polarisEventListener.onBeforeCreateNamespace(
        new BeforeCreateNamespaceEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            createNamespaceRequest));
    Response resp =
        delegate.createNamespace(prefix, createNamespaceRequest, realmContext, securityContext);
    CreateNamespaceResponse createNamespaceResponse = (CreateNamespaceResponse) resp.getEntity();
    polarisEventListener.onAfterCreateNamespace(
        new AfterCreateNamespaceEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            createNamespaceResponse.namespace(),
            createNamespaceResponse.properties()));
    return resp;
  }

  @Override
  public Response listNamespaces(
      String prefix,
      String pageToken,
      Integer pageSize,
      String parent,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    polarisEventListener.onBeforeListNamespaces(
        new BeforeListNamespacesEvent(
            PolarisEvent.createEventId(), eventMetadataFactory.create(), catalogName, parent));
    Response resp =
        delegate.listNamespaces(prefix, pageToken, pageSize, parent, realmContext, securityContext);
    polarisEventListener.onAfterListNamespaces(
        new AfterListNamespacesEvent(
            PolarisEvent.createEventId(), eventMetadataFactory.create(), catalogName, parent));
    return resp;
  }

  @Override
  public Response loadNamespaceMetadata(
      String prefix, String namespace, RealmContext realmContext, SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    polarisEventListener.onBeforeLoadNamespaceMetadata(
        new BeforeLoadNamespaceMetadataEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            decodeNamespace(namespace)));
    Response resp =
        delegate.loadNamespaceMetadata(prefix, namespace, realmContext, securityContext);
    GetNamespaceResponse getNamespaceResponse = (GetNamespaceResponse) resp.getEntity();
    polarisEventListener.onAfterLoadNamespaceMetadata(
        new AfterLoadNamespaceMetadataEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            getNamespaceResponse.namespace(),
            getNamespaceResponse.properties()));
    return resp;
  }

  @Override
  public Response namespaceExists(
      String prefix, String namespace, RealmContext realmContext, SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeCheckExistsNamespace(
        new BeforeCheckExistsNamespaceEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj));
    Response resp = delegate.namespaceExists(prefix, namespace, realmContext, securityContext);
    polarisEventListener.onAfterCheckExistsNamespace(
        new AfterCheckExistsNamespaceEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj));
    return resp;
  }

  @Override
  public Response dropNamespace(
      String prefix, String namespace, RealmContext realmContext, SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    polarisEventListener.onBeforeDropNamespace(
        new BeforeDropNamespaceEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            decodeNamespace(namespace)));
    Response resp = delegate.dropNamespace(prefix, namespace, realmContext, securityContext);
    polarisEventListener.onAfterDropNamespace(
        new AfterDropNamespaceEvent(
            PolarisEvent.createEventId(), eventMetadataFactory.create(), catalogName, namespace));
    return resp;
  }

  @Override
  public Response updateProperties(
      String prefix,
      String namespace,
      UpdateNamespacePropertiesRequest updateNamespacePropertiesRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeUpdateNamespaceProperties(
        new BeforeUpdateNamespacePropertiesEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            updateNamespacePropertiesRequest));
    Response resp =
        delegate.updateProperties(
            prefix, namespace, updateNamespacePropertiesRequest, realmContext, securityContext);
    polarisEventListener.onAfterUpdateNamespaceProperties(
        new AfterUpdateNamespacePropertiesEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            (UpdateNamespacePropertiesResponse) resp.getEntity()));
    return resp;
  }

  @Override
  public Response createTable(
      String prefix,
      String namespace,
      CreateTableRequest createTableRequest,
      String accessDelegationMode,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeCreateTable(
        new BeforeCreateTableEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            createTableRequest,
            accessDelegationMode));
    Response resp =
        delegate.createTable(
            prefix,
            namespace,
            createTableRequest,
            accessDelegationMode,
            realmContext,
            securityContext);
    if (!createTableRequest.stageCreate()) {
      polarisEventListener.onAfterCreateTable(
          new AfterCreateTableEvent(
              PolarisEvent.createEventId(),
              eventMetadataFactory.create(),
              catalogName,
              namespaceObj,
              createTableRequest.name(),
              (LoadTableResponse) resp.getEntity()));
    }
    return resp;
  }

  @Override
  public Response listTables(
      String prefix,
      String namespace,
      String pageToken,
      Integer pageSize,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeListTables(
        new BeforeListTablesEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj));
    Response resp =
        delegate.listTables(prefix, namespace, pageToken, pageSize, realmContext, securityContext);
    polarisEventListener.onAfterListTables(
        new AfterListTablesEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj));
    return resp;
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
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeLoadTable(
        new BeforeLoadTableEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            table,
            accessDelegationMode,
            ifNoneMatchString,
            snapshots));
    Response resp =
        delegate.loadTable(
            prefix,
            namespace,
            table,
            accessDelegationMode,
            ifNoneMatchString,
            snapshots,
            realmContext,
            securityContext);
    polarisEventListener.onAfterLoadTable(
        new AfterLoadTableEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            table,
            (LoadTableResponse) resp.getEntity()));
    return resp;
  }

  @Override
  public Response tableExists(
      String prefix,
      String namespace,
      String table,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeCheckExistsTable(
        new BeforeCheckExistsTableEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            table));
    Response resp = delegate.tableExists(prefix, namespace, table, realmContext, securityContext);
    polarisEventListener.onAfterCheckExistsTable(
        new AfterCheckExistsTableEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            table));
    return resp;
  }

  @Override
  public Response dropTable(
      String prefix,
      String namespace,
      String table,
      Boolean purgeRequested,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeDropTable(
        new BeforeDropTableEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            table,
            purgeRequested));
    Response resp =
        delegate.dropTable(prefix, namespace, table, purgeRequested, realmContext, securityContext);
    polarisEventListener.onAfterDropTable(
        new AfterDropTableEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            table,
            purgeRequested));
    return resp;
  }

  @Override
  public Response registerTable(
      String prefix,
      String namespace,
      RegisterTableRequest registerTableRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeRegisterTable(
        new BeforeRegisterTableEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            registerTableRequest));
    Response resp =
        delegate.registerTable(
            prefix, namespace, registerTableRequest, realmContext, securityContext);
    polarisEventListener.onAfterRegisterTable(
        new AfterRegisterTableEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            registerTableRequest.name(),
            (LoadTableResponse) resp.getEntity()));
    return resp;
  }

  @Override
  public Response renameTable(
      String prefix,
      RenameTableRequest renameTableRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    polarisEventListener.onBeforeRenameTable(
        new BeforeRenameTableEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            renameTableRequest));
    Response resp = delegate.renameTable(prefix, renameTableRequest, realmContext, securityContext);
    polarisEventListener.onAfterRenameTable(
        new AfterRenameTableEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            renameTableRequest));
    return resp;
  }

  @Override
  public Response updateTable(
      String prefix,
      String namespace,
      String table,
      CommitTableRequest commitTableRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeUpdateTable(
        new BeforeUpdateTableEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            table,
            commitTableRequest));
    Response resp =
        delegate.updateTable(
            prefix, namespace, table, commitTableRequest, realmContext, securityContext);
    polarisEventListener.onAfterUpdateTable(
        new AfterUpdateTableEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            table,
            commitTableRequest,
            (LoadTableResponse) resp.getEntity()));
    return resp;
  }

  @Override
  public Response createView(
      String prefix,
      String namespace,
      CreateViewRequest createViewRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeCreateView(
        new BeforeCreateViewEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            createViewRequest));
    Response resp =
        delegate.createView(prefix, namespace, createViewRequest, realmContext, securityContext);
    polarisEventListener.onAfterCreateView(
        new AfterCreateViewEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            createViewRequest.name(),
            (LoadViewResponse) resp.getEntity()));
    return resp;
  }

  @Override
  public Response listViews(
      String prefix,
      String namespace,
      String pageToken,
      Integer pageSize,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeListViews(
        new BeforeListViewsEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj));
    Response resp =
        delegate.listViews(prefix, namespace, pageToken, pageSize, realmContext, securityContext);
    polarisEventListener.onAfterListViews(
        new AfterListViewsEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj));
    return resp;
  }

  @Override
  public Response loadCredentials(
      String prefix,
      String namespace,
      String table,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeLoadCredentials(
        new BeforeLoadCredentialsEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            table));
    Response resp =
        delegate.loadCredentials(prefix, namespace, table, realmContext, securityContext);
    polarisEventListener.onAfterLoadCredentials(
        new AfterLoadCredentialsEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            table));
    return resp;
  }

  @Override
  public Response loadView(
      String prefix,
      String namespace,
      String view,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeLoadView(
        new BeforeLoadViewEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            view));
    Response resp = delegate.loadView(prefix, namespace, view, realmContext, securityContext);
    polarisEventListener.onAfterLoadView(
        new AfterLoadViewEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            view,
            (LoadViewResponse) resp.getEntity()));
    return resp;
  }

  @Override
  public Response viewExists(
      String prefix,
      String namespace,
      String view,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeCheckExistsView(
        new BeforeCheckExistsViewEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            view));
    Response resp = delegate.viewExists(prefix, namespace, view, realmContext, securityContext);
    polarisEventListener.onAfterCheckExistsView(
        new AfterCheckExistsViewEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            view));
    return resp;
  }

  @Override
  public Response dropView(
      String prefix,
      String namespace,
      String view,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeDropView(
        new BeforeDropViewEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            view));
    Response resp = delegate.dropView(prefix, namespace, view, realmContext, securityContext);
    polarisEventListener.onAfterDropView(
        new AfterDropViewEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            view));
    return resp;
  }

  @Override
  public Response renameView(
      String prefix,
      RenameTableRequest renameTableRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    polarisEventListener.onBeforeRenameView(
        new BeforeRenameViewEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            renameTableRequest));
    Response resp = delegate.renameView(prefix, renameTableRequest, realmContext, securityContext);
    polarisEventListener.onAfterRenameView(
        new AfterRenameViewEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            renameTableRequest));
    return resp;
  }

  @Override
  public Response replaceView(
      String prefix,
      String namespace,
      String view,
      CommitViewRequest commitViewRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeReplaceView(
        new BeforeReplaceViewEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            view,
            commitViewRequest));
    Response resp =
        delegate.replaceView(
            prefix, namespace, view, commitViewRequest, realmContext, securityContext);
    polarisEventListener.onAfterReplaceView(
        new AfterReplaceViewEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            view,
            commitViewRequest,
            (LoadViewResponse) resp.getEntity()));
    return resp;
  }

  @Override
  public Response commitTransaction(
      String prefix,
      CommitTransactionRequest commitTransactionRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    polarisEventListener.onBeforeCommitTransaction(
        new IcebergRestCatalogEvents.BeforeCommitTransactionEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            commitTransactionRequest));
    Response resp =
        delegate.commitTransaction(prefix, commitTransactionRequest, realmContext, securityContext);
    polarisEventListener.onAfterCommitTransaction(
        new IcebergRestCatalogEvents.AfterCommitTransactionEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            commitTransactionRequest));
    return resp;
  }

  /** This API is currently a no-op in Polaris. */
  @Override
  public Response reportMetrics(
      String prefix,
      String namespace,
      String table,
      ReportMetricsRequest reportMetricsRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return delegate.reportMetrics(
        prefix, namespace, table, reportMetricsRequest, realmContext, securityContext);
  }

  @Override
  public Response sendNotification(
      String prefix,
      String namespace,
      String table,
      NotificationRequest notificationRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onBeforeSendNotification(
        new BeforeSendNotificationEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            table,
            notificationRequest));
    Response resp =
        delegate.sendNotification(
            prefix, namespace, table, notificationRequest, realmContext, securityContext);
    polarisEventListener.onAfterSendNotification(
        new AfterSendNotificationEvent(
            PolarisEvent.createEventId(),
            eventMetadataFactory.create(),
            catalogName,
            namespaceObj,
            table));
    return resp;
  }
}
