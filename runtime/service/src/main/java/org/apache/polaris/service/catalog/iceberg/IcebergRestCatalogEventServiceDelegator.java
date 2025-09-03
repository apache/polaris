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
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterCheckNamespaceExistsEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterCheckTableExistsEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.AfterCheckViewExistsEvent;
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
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeCheckNamespaceExistsEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeCheckTableExistsEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents.BeforeCheckViewExistsEvent;
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
import org.apache.polaris.service.events.PolarisEventListener;
import org.apache.polaris.service.types.CommitTableRequest;
import org.apache.polaris.service.types.CommitViewRequest;
import org.apache.polaris.service.types.NotificationRequest;

@Decorator
@Priority(1000)
public class IcebergRestCatalogEventServiceDelegator implements IcebergRestCatalogApiService {

  @Inject @Delegate IcebergCatalogAdapter delegate;
  @Inject PolarisEventListener polarisEventListener;
  @Inject CatalogPrefixParser prefixParser;

  @Override
  public Response createNamespace(
      String prefix,
      CreateNamespaceRequest createNamespaceRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = getCatalogFromPrefix(prefix, realmContext);
    polarisEventListener.onBeforeCreateNamespace(
        new BeforeCreateNamespaceEvent(catalogName, createNamespaceRequest));
    Response resp =
        delegate.createNamespace(prefix, createNamespaceRequest, realmContext, securityContext);
    CreateNamespaceResponse createNamespaceResponse = (CreateNamespaceResponse) resp.getEntity();
    polarisEventListener.onAfterCreateNamespace(
        new AfterCreateNamespaceEvent(
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
    String catalogName = getCatalogFromPrefix(prefix, realmContext);
    polarisEventListener.onBeforeListNamespaces(new BeforeListNamespacesEvent(catalogName, parent));
    Response resp =
        delegate.listNamespaces(prefix, pageToken, pageSize, parent, realmContext, securityContext);
    polarisEventListener.onAfterListNamespaces(new AfterListNamespacesEvent(catalogName, parent));
    return resp;
  }

  @Override
  public Response loadNamespaceMetadata(
      String prefix, String namespace, RealmContext realmContext, SecurityContext securityContext) {
    String catalogName = getCatalogFromPrefix(prefix, realmContext);
    polarisEventListener.onBeforeLoadNamespaceMetadata(
        new BeforeLoadNamespaceMetadataEvent(catalogName, namespace));
    Response resp =
        delegate.loadNamespaceMetadata(prefix, namespace, realmContext, securityContext);
    GetNamespaceResponse getNamespaceResponse = (GetNamespaceResponse) resp.getEntity();
    polarisEventListener.onAfterLoadNamespaceMetadata(
        new AfterLoadNamespaceMetadataEvent(
            catalogName, getNamespaceResponse.namespace(), getNamespaceResponse.properties()));
    return resp;
  }

  @Override
  public Response namespaceExists(
      String prefix, String namespace, RealmContext realmContext, SecurityContext securityContext) {
    String catalogName = getCatalogFromPrefix(prefix, realmContext);
    polarisEventListener.onBeforeCheckNamespaceExists(
        new BeforeCheckNamespaceExistsEvent(catalogName, namespace));
    Response resp = delegate.namespaceExists(prefix, namespace, realmContext, securityContext);
    polarisEventListener.onAfterCheckNamespaceExists(
        new AfterCheckNamespaceExistsEvent(catalogName, namespace));
    return resp;
  }

  @Override
  public Response dropNamespace(
      String prefix, String namespace, RealmContext realmContext, SecurityContext securityContext) {
    String catalogName = getCatalogFromPrefix(prefix, realmContext);
    polarisEventListener.onBeforeDropNamespace(
        new BeforeDropNamespaceEvent(catalogName, namespace));
    Response resp = delegate.dropNamespace(prefix, namespace, realmContext, securityContext);
    polarisEventListener.onAfterDropNamespace(new AfterDropNamespaceEvent(catalogName, namespace));
    return resp;
  }

  @Override
  public Response updateProperties(
      String prefix,
      String namespace,
      UpdateNamespacePropertiesRequest updateNamespacePropertiesRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = getCatalogFromPrefix(prefix, realmContext);
    polarisEventListener.onBeforeUpdateNamespaceProperties(
        new BeforeUpdateNamespacePropertiesEvent(
            catalogName, namespace, updateNamespacePropertiesRequest));
    Response resp =
        delegate.updateProperties(
            prefix, namespace, updateNamespacePropertiesRequest, realmContext, securityContext);
    polarisEventListener.onAfterUpdateNamespaceProperties(
        new AfterUpdateNamespacePropertiesEvent(
            catalogName, namespace, (UpdateNamespacePropertiesResponse) resp.getEntity()));
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
    String catalogName = getCatalogFromPrefix(prefix, realmContext);
    if (!createTableRequest.stageCreate()) {
      polarisEventListener.onBeforeCreateTable(
          new BeforeCreateTableEvent(
              catalogName, namespace, createTableRequest, accessDelegationMode));
    }
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
          new AfterCreateTableEvent(catalogName, namespace, (LoadTableResponse) resp.getEntity()));
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
    String catalogName = getCatalogFromPrefix(prefix, realmContext);
    polarisEventListener.onBeforeListTables(new BeforeListTablesEvent(catalogName, namespace));
    Response resp =
        delegate.listTables(prefix, namespace, pageToken, pageSize, realmContext, securityContext);
    polarisEventListener.onAfterListTables(new AfterListTablesEvent(catalogName, namespace));
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
    String catalogName = getCatalogFromPrefix(prefix, realmContext);
    polarisEventListener.onBeforeLoadTable(
        new BeforeLoadTableEvent(
            catalogName, namespace, table, accessDelegationMode, ifNoneMatchString, snapshots));
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
        new AfterLoadTableEvent(catalogName, namespace, (LoadTableResponse) resp.getEntity()));
    return resp;
  }

  @Override
  public Response tableExists(
      String prefix,
      String namespace,
      String table,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = getCatalogFromPrefix(prefix, realmContext);
    polarisEventListener.onBeforeCheckTableExists(
        new BeforeCheckTableExistsEvent(catalogName, namespace, table));
    Response resp = delegate.tableExists(prefix, namespace, table, realmContext, securityContext);
    polarisEventListener.onAfterCheckTableExists(
        new AfterCheckTableExistsEvent(catalogName, namespace, table));
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
    String catalogName = getCatalogFromPrefix(prefix, realmContext);
    polarisEventListener.onBeforeDropTable(
        new BeforeDropTableEvent(catalogName, namespace, table, purgeRequested));
    Response resp =
        delegate.dropTable(prefix, namespace, table, purgeRequested, realmContext, securityContext);
    polarisEventListener.onAfterDropTable(
        new AfterDropTableEvent(catalogName, namespace, table, purgeRequested));
    return resp;
  }

  @Override
  public Response registerTable(
      String prefix,
      String namespace,
      RegisterTableRequest registerTableRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = getCatalogFromPrefix(prefix, realmContext);
    polarisEventListener.onBeforeRegisterTable(
        new BeforeRegisterTableEvent(catalogName, namespace, registerTableRequest));
    Response resp =
        delegate.registerTable(
            prefix, namespace, registerTableRequest, realmContext, securityContext);
    polarisEventListener.onAfterRegisterTable(
        new AfterRegisterTableEvent(catalogName, namespace, (LoadTableResponse) resp.getEntity()));
    return resp;
  }

  @Override
  public Response renameTable(
      String prefix,
      RenameTableRequest renameTableRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = getCatalogFromPrefix(prefix, realmContext);
    polarisEventListener.onBeforeRenameTable(
        new BeforeRenameTableEvent(catalogName, renameTableRequest));
    Response resp = delegate.renameTable(prefix, renameTableRequest, realmContext, securityContext);
    polarisEventListener.onAfterRenameTable(
        new AfterRenameTableEvent(catalogName, renameTableRequest));
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
    String catalogName = getCatalogFromPrefix(prefix, realmContext);
    polarisEventListener.onBeforeUpdateTable(
        new BeforeUpdateTableEvent(catalogName, namespace, table, commitTableRequest));
    Response resp =
        delegate.updateTable(
            prefix, namespace, table, commitTableRequest, realmContext, securityContext);
    polarisEventListener.onAfterUpdateTable(
        new AfterUpdateTableEvent(
            catalogName, namespace, table, (LoadTableResponse) resp.getEntity()));
    return resp;
  }

  @Override
  public Response createView(
      String prefix,
      String namespace,
      CreateViewRequest createViewRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = getCatalogFromPrefix(prefix, realmContext);
    polarisEventListener.onBeforeCreateView(
        new BeforeCreateViewEvent(catalogName, namespace, createViewRequest));
    Response resp =
        delegate.createView(prefix, namespace, createViewRequest, realmContext, securityContext);
    polarisEventListener.onAfterCreateView(
        new AfterCreateViewEvent(catalogName, namespace, (LoadViewResponse) resp.getEntity()));
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
    String catalogName = getCatalogFromPrefix(prefix, realmContext);
    polarisEventListener.onBeforeListViews(new BeforeListViewsEvent(catalogName, namespace));
    Response resp =
        delegate.listViews(prefix, namespace, pageToken, pageSize, realmContext, securityContext);
    polarisEventListener.onAfterListViews(new AfterListViewsEvent(catalogName, namespace));
    return resp;
  }

  @Override
  public Response loadCredentials(
      String prefix,
      String namespace,
      String table,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = getCatalogFromPrefix(prefix, realmContext);
    polarisEventListener.onBeforeLoadCredentials(
        new BeforeLoadCredentialsEvent(catalogName, namespace, table));
    Response resp =
        delegate.loadCredentials(prefix, namespace, table, realmContext, securityContext);
    polarisEventListener.onAfterLoadCredentials(
        new AfterLoadCredentialsEvent(catalogName, namespace, table));
    return resp;
  }

  @Override
  public Response loadView(
      String prefix,
      String namespace,
      String view,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = getCatalogFromPrefix(prefix, realmContext);
    polarisEventListener.onBeforeLoadView(new BeforeLoadViewEvent(catalogName, namespace, view));
    Response resp = delegate.loadView(prefix, namespace, view, realmContext, securityContext);
    polarisEventListener.onAfterLoadView(
        new AfterLoadViewEvent(catalogName, namespace, (LoadViewResponse) resp.getEntity()));
    return resp;
  }

  @Override
  public Response viewExists(
      String prefix,
      String namespace,
      String view,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = getCatalogFromPrefix(prefix, realmContext);
    polarisEventListener.onBeforeCheckViewExists(
        new BeforeCheckViewExistsEvent(catalogName, namespace, view));
    Response resp = delegate.viewExists(prefix, namespace, view, realmContext, securityContext);
    polarisEventListener.onAfterCheckViewExists(
        new AfterCheckViewExistsEvent(catalogName, namespace, view));
    return resp;
  }

  @Override
  public Response dropView(
      String prefix,
      String namespace,
      String view,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = getCatalogFromPrefix(prefix, realmContext);
    polarisEventListener.onBeforeDropView(new BeforeDropViewEvent(catalogName, namespace, view));
    Response resp = delegate.dropView(prefix, namespace, view, realmContext, securityContext);
    polarisEventListener.onAfterDropView(new AfterDropViewEvent(catalogName, namespace, view));
    return resp;
  }

  @Override
  public Response renameView(
      String prefix,
      RenameTableRequest renameTableRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = getCatalogFromPrefix(prefix, realmContext);
    polarisEventListener.onBeforeRenameView(
        new BeforeRenameViewEvent(catalogName, renameTableRequest));
    Response resp = delegate.renameView(prefix, renameTableRequest, realmContext, securityContext);
    polarisEventListener.onAfterRenameView(
        new AfterRenameViewEvent(catalogName, renameTableRequest));
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
    String catalogName = getCatalogFromPrefix(prefix, realmContext);
    polarisEventListener.onBeforeReplaceView(
        new BeforeReplaceViewEvent(catalogName, namespace, view, commitViewRequest));
    Response resp =
        delegate.replaceView(
            prefix, namespace, view, commitViewRequest, realmContext, securityContext);
    polarisEventListener.onAfterReplaceView(
        new AfterReplaceViewEvent(
            catalogName, namespace, view, (LoadViewResponse) resp.getEntity()));
    return resp;
  }

  /**
   * Table Committed Events are already instrumented at a more granular level than the API itself.
   */
  @Override
  public Response commitTransaction(
      String prefix,
      CommitTransactionRequest commitTransactionRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return delegate.commitTransaction(
        prefix, commitTransactionRequest, realmContext, securityContext);
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
    String catalogName = getCatalogFromPrefix(prefix, realmContext);
    polarisEventListener.onBeforeSendNotification(
        new BeforeSendNotificationEvent(catalogName, namespace, table, notificationRequest));
    Response resp =
        delegate.sendNotification(
            prefix, namespace, table, notificationRequest, realmContext, securityContext);
    polarisEventListener.onAfterSendNotification(
        new AfterSendNotificationEvent(catalogName, namespace, table, (boolean) resp.getEntity()));
    return resp;
  }

  private String getCatalogFromPrefix(String prefix, RealmContext realmContext) {
    return prefixParser.prefixToCatalogName(realmContext, prefix);
  }
}
