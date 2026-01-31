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

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Priority;
import jakarta.decorator.Decorator;
import jakarta.decorator.Delegate;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.util.List;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RegisterViewRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.api.IcebergRestCatalogApiService;
import org.apache.polaris.service.catalog.common.CatalogAdapter;
import org.apache.polaris.service.events.EventAttributeMap;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.events.PolarisEventType;
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
  @Inject EventAttributeMap eventAttributeMap;

  // Constructor for testing - allows manual dependency injection
  @VisibleForTesting
  public IcebergRestCatalogEventServiceDelegator(
      IcebergCatalogAdapter delegate,
      PolarisEventListener polarisEventListener,
      PolarisEventMetadataFactory eventMetadataFactory,
      CatalogPrefixParser prefixParser,
      EventAttributeMap eventAttributeMap) {
    this.delegate = delegate;
    this.polarisEventListener = polarisEventListener;
    this.eventMetadataFactory = eventMetadataFactory;
    this.prefixParser = prefixParser;
    this.eventAttributeMap = eventAttributeMap;
  }

  // Default constructor for CDI
  public IcebergRestCatalogEventServiceDelegator() {}

  @Override
  public Response createNamespace(
      String prefix,
      CreateNamespaceRequest createNamespaceRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_CREATE_NAMESPACE,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.CREATE_NAMESPACE_REQUEST, createNamespaceRequest)));
    Response resp =
        delegate.createNamespace(prefix, createNamespaceRequest, realmContext, securityContext);
    CreateNamespaceResponse createNamespaceResponse = (CreateNamespaceResponse) resp.getEntity();
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_CREATE_NAMESPACE,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, createNamespaceResponse.namespace())
                .put(EventAttributes.NAMESPACE_PROPERTIES, createNamespaceResponse.properties())));
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
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_LIST_NAMESPACES,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.PARENT_NAMESPACE_FQN, parent)));
    Response resp =
        delegate.listNamespaces(prefix, pageToken, pageSize, parent, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_LIST_NAMESPACES,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.PARENT_NAMESPACE_FQN, parent)));
    return resp;
  }

  @Override
  public Response loadNamespaceMetadata(
      String prefix, String namespace, RealmContext realmContext, SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_LOAD_NAMESPACE_METADATA,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, decodeNamespace(namespace))));
    Response resp =
        delegate.loadNamespaceMetadata(prefix, namespace, realmContext, securityContext);
    GetNamespaceResponse getNamespaceResponse = (GetNamespaceResponse) resp.getEntity();
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_LOAD_NAMESPACE_METADATA,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, getNamespaceResponse.namespace())
                .put(EventAttributes.NAMESPACE_PROPERTIES, getNamespaceResponse.properties())));
    return resp;
  }

  @Override
  public Response namespaceExists(
      String prefix, String namespace, RealmContext realmContext, SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_CHECK_EXISTS_NAMESPACE,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)));
    Response resp = delegate.namespaceExists(prefix, namespace, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_CHECK_EXISTS_NAMESPACE,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)));
    return resp;
  }

  @Override
  public Response dropNamespace(
      String prefix, String namespace, RealmContext realmContext, SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_DROP_NAMESPACE,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, decodeNamespace(namespace))));
    Response resp = delegate.dropNamespace(prefix, namespace, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_DROP_NAMESPACE,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE_FQN, namespace)));
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
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_UPDATE_NAMESPACE_PROPERTIES,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(
                    EventAttributes.UPDATE_NAMESPACE_PROPERTIES_REQUEST,
                    updateNamespacePropertiesRequest)));
    Response resp =
        delegate.updateProperties(
            prefix, namespace, updateNamespacePropertiesRequest, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_UPDATE_NAMESPACE_PROPERTIES,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(
                    EventAttributes.UPDATE_NAMESPACE_PROPERTIES_RESPONSE,
                    (UpdateNamespacePropertiesResponse) resp.getEntity())));
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
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_CREATE_TABLE,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.CREATE_TABLE_REQUEST, createTableRequest)
                .put(EventAttributes.ACCESS_DELEGATION_MODE, accessDelegationMode)));
    Response resp =
        delegate.createTable(
            prefix,
            namespace,
            createTableRequest,
            accessDelegationMode,
            realmContext,
            securityContext);
    if (!createTableRequest.stageCreate()) {
      polarisEventListener.onEvent(
          new PolarisEvent(
              PolarisEventType.AFTER_CREATE_TABLE,
              eventMetadataFactory.create(),
              new EventAttributeMap()
                  .put(EventAttributes.CATALOG_NAME, catalogName)
                  .put(EventAttributes.NAMESPACE, namespaceObj)
                  .put(EventAttributes.TABLE_NAME, createTableRequest.name())
                  .put(EventAttributes.LOAD_TABLE_RESPONSE, (LoadTableResponse) resp.getEntity())));
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
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_LIST_TABLES,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)));
    Response resp =
        delegate.listTables(prefix, namespace, pageToken, pageSize, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_LIST_TABLES,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)));
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
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_LOAD_TABLE,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.TABLE_NAME, table)
                .put(EventAttributes.ACCESS_DELEGATION_MODE, accessDelegationMode)
                .put(EventAttributes.IF_NONE_MATCH_STRING, ifNoneMatchString)
                .put(EventAttributes.SNAPSHOTS, snapshots)));
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

    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_LOAD_TABLE,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.TABLE_NAME, table)
                .put(EventAttributes.LOAD_TABLE_RESPONSE, (LoadTableResponse) resp.getEntity())));
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
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_CHECK_EXISTS_TABLE,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.TABLE_NAME, table)));
    Response resp = delegate.tableExists(prefix, namespace, table, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_CHECK_EXISTS_TABLE,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.TABLE_NAME, table)));
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
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_DROP_TABLE,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.TABLE_NAME, table)
                .put(EventAttributes.PURGE_REQUESTED, purgeRequested)));
    Response resp =
        delegate.dropTable(prefix, namespace, table, purgeRequested, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_DROP_TABLE,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.TABLE_NAME, table)
                .put(EventAttributes.PURGE_REQUESTED, purgeRequested)));
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
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_REGISTER_TABLE,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.REGISTER_TABLE_REQUEST, registerTableRequest)));
    Response resp =
        delegate.registerTable(
            prefix, namespace, registerTableRequest, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_REGISTER_TABLE,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.TABLE_NAME, registerTableRequest.name())
                .put(EventAttributes.LOAD_TABLE_RESPONSE, (LoadTableResponse) resp.getEntity())));
    return resp;
  }

  @Override
  public Response renameTable(
      String prefix,
      RenameTableRequest renameTableRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_RENAME_TABLE,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.RENAME_TABLE_REQUEST, renameTableRequest)));
    Response resp = delegate.renameTable(prefix, renameTableRequest, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_RENAME_TABLE,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.RENAME_TABLE_REQUEST, renameTableRequest)));
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
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_UPDATE_TABLE,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.TABLE_NAME, table)
                .put(EventAttributes.UPDATE_TABLE_REQUEST, commitTableRequest)));
    Response resp =
        delegate.updateTable(
            prefix, namespace, table, commitTableRequest, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_UPDATE_TABLE,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.TABLE_NAME, table)
                .put(EventAttributes.UPDATE_TABLE_REQUEST, commitTableRequest)
                .put(
                    EventAttributes.TABLE_METADATA,
                    ((LoadTableResponse) resp.getEntity()).tableMetadata())));
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
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_CREATE_VIEW,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.CREATE_VIEW_REQUEST, createViewRequest)));
    Response resp =
        delegate.createView(prefix, namespace, createViewRequest, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_CREATE_VIEW,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.VIEW_NAME, createViewRequest.name())
                .put(EventAttributes.LOAD_VIEW_RESPONSE, (LoadViewResponse) resp.getEntity())));
    return resp;
  }

  @Override
  public Response registerView(
      String prefix,
      String namespace,
      RegisterViewRequest registerViewRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    Namespace namespaceObj = decodeNamespace(namespace);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_REGISTER_VIEW,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.REGISTER_VIEW_REQUEST, registerViewRequest)));
    Response resp =
        delegate.registerView(
            prefix, namespace, registerViewRequest, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_REGISTER_VIEW,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.VIEW_NAME, registerViewRequest.name())
                .put(EventAttributes.LOAD_VIEW_RESPONSE, (LoadViewResponse) resp.getEntity())));
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
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_LIST_VIEWS,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)));
    Response resp =
        delegate.listViews(prefix, namespace, pageToken, pageSize, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_LIST_VIEWS,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)));
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
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_LOAD_CREDENTIALS,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.TABLE_NAME, table)));
    Response resp =
        delegate.loadCredentials(prefix, namespace, table, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_LOAD_CREDENTIALS,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.TABLE_NAME, table)));
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
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_LOAD_VIEW,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.VIEW_NAME, view)));
    Response resp = delegate.loadView(prefix, namespace, view, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_LOAD_VIEW,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.VIEW_NAME, view)
                .put(EventAttributes.LOAD_VIEW_RESPONSE, (LoadViewResponse) resp.getEntity())));
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
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_CHECK_EXISTS_VIEW,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.VIEW_NAME, view)));
    Response resp = delegate.viewExists(prefix, namespace, view, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_CHECK_EXISTS_VIEW,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.VIEW_NAME, view)));
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
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_DROP_VIEW,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.VIEW_NAME, view)));
    Response resp = delegate.dropView(prefix, namespace, view, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_DROP_VIEW,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.VIEW_NAME, view)));
    return resp;
  }

  @Override
  public Response renameView(
      String prefix,
      RenameTableRequest renameTableRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_RENAME_VIEW,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.RENAME_TABLE_REQUEST, renameTableRequest)));
    Response resp = delegate.renameView(prefix, renameTableRequest, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_RENAME_VIEW,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.RENAME_TABLE_REQUEST, renameTableRequest)));
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
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_REPLACE_VIEW,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.VIEW_NAME, view)
                .put(EventAttributes.COMMIT_VIEW_REQUEST, commitViewRequest)));
    Response resp =
        delegate.replaceView(
            prefix, namespace, view, commitViewRequest, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_REPLACE_VIEW,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.VIEW_NAME, view)
                .put(EventAttributes.COMMIT_VIEW_REQUEST, commitViewRequest)
                .put(EventAttributes.LOAD_VIEW_RESPONSE, (LoadViewResponse) resp.getEntity())));
    return resp;
  }

  @Override
  public Response commitTransaction(
      String prefix,
      CommitTransactionRequest commitTransactionRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_COMMIT_TRANSACTION,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.COMMIT_TRANSACTION_REQUEST, commitTransactionRequest)));
    for (UpdateTableRequest req : commitTransactionRequest.tableChanges()) {
      polarisEventListener.onEvent(
          new PolarisEvent(
              PolarisEventType.BEFORE_UPDATE_TABLE,
              eventMetadataFactory.create(),
              new EventAttributeMap()
                  .put(EventAttributes.CATALOG_NAME, catalogName)
                  .put(EventAttributes.NAMESPACE, req.identifier().namespace())
                  .put(EventAttributes.TABLE_NAME, req.identifier().name())
                  .put(EventAttributes.UPDATE_TABLE_REQUEST, req)));
    }
    Response resp =
        delegate.commitTransaction(prefix, commitTransactionRequest, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_COMMIT_TRANSACTION,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.COMMIT_TRANSACTION_REQUEST, commitTransactionRequest)));
    List<TableMetadata> tableMetadataList =
        eventAttributeMap.getRequired(EventAttributes.TABLE_METADATAS);
    for (int i = 0; i < commitTransactionRequest.tableChanges().size(); i++) {
      UpdateTableRequest req = commitTransactionRequest.tableChanges().get(i);
      TableMetadata tableMetadata =
          tableMetadataList != null && i < tableMetadataList.size()
              ? tableMetadataList.get(i)
              : null;
      polarisEventListener.onEvent(
          new PolarisEvent(
              PolarisEventType.AFTER_UPDATE_TABLE,
              eventMetadataFactory.create(),
              new EventAttributeMap()
                  .put(EventAttributes.CATALOG_NAME, catalogName)
                  .put(EventAttributes.NAMESPACE, req.identifier().namespace())
                  .put(EventAttributes.TABLE_NAME, req.identifier().name())
                  .put(EventAttributes.UPDATE_TABLE_REQUEST, req)
                  .put(EventAttributes.TABLE_METADATA, tableMetadata)));
    }
    return resp;
  }

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
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_SEND_NOTIFICATION,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.TABLE_NAME, table)
                .put(EventAttributes.NOTIFICATION_REQUEST, notificationRequest)));
    Response resp =
        delegate.sendNotification(
            prefix, namespace, table, notificationRequest, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_SEND_NOTIFICATION,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE, namespaceObj)
                .put(EventAttributes.TABLE_NAME, table)));
    return resp;
  }
}
