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

package org.apache.polaris.service.catalog.generic;

import jakarta.annotation.Priority;
import jakarta.decorator.Decorator;
import jakarta.decorator.Delegate;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.api.PolarisCatalogGenericTableApiService;
import org.apache.polaris.service.catalog.common.CatalogAdapter;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.events.PolarisEventType;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.apache.polaris.service.types.CreateGenericTableRequest;
import org.apache.polaris.service.types.LoadGenericTableResponse;

@Decorator
@Priority(1000)
public class CatalogGenericTableEventServiceDelegator
    implements PolarisCatalogGenericTableApiService, CatalogAdapter {

  @Inject @Delegate GenericTableCatalogAdapter delegate;
  @Inject PolarisEventListener polarisEventListener;
  @Inject PolarisEventMetadataFactory eventMetadataFactory;
  @Inject CatalogPrefixParser prefixParser;

  @Override
  public Response createGenericTable(
      String prefix,
      String namespace,
      CreateGenericTableRequest createGenericTableRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.BEFORE_CREATE_GENERIC_TABLE, eventMetadataFactory.create())
            .attribute(EventAttributes.CATALOG_NAME, catalogName)
            .attribute(EventAttributes.NAMESPACE_NAME, namespace)
            .attribute(EventAttributes.CREATE_GENERIC_TABLE_REQUEST, createGenericTableRequest)
            .build());
    Response resp =
        delegate.createGenericTable(
            prefix, namespace, createGenericTableRequest, realmContext, securityContext);
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.AFTER_CREATE_GENERIC_TABLE, eventMetadataFactory.create())
            .attribute(EventAttributes.CATALOG_NAME, catalogName)
            .attribute(EventAttributes.NAMESPACE_NAME, namespace)
            .attribute(
                EventAttributes.GENERIC_TABLE,
                ((LoadGenericTableResponse) resp.getEntity()).getTable())
            .build());
    return resp;
  }

  @Override
  public Response dropGenericTable(
      String prefix,
      String namespace,
      String genericTable,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.BEFORE_DROP_GENERIC_TABLE, eventMetadataFactory.create())
            .attribute(EventAttributes.CATALOG_NAME, catalogName)
            .attribute(EventAttributes.NAMESPACE_NAME, namespace)
            .attribute(EventAttributes.GENERIC_TABLE_NAME, genericTable)
            .build());
    Response resp =
        delegate.dropGenericTable(prefix, namespace, genericTable, realmContext, securityContext);
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.AFTER_DROP_GENERIC_TABLE, eventMetadataFactory.create())
            .attribute(EventAttributes.CATALOG_NAME, catalogName)
            .attribute(EventAttributes.NAMESPACE_NAME, namespace)
            .attribute(EventAttributes.GENERIC_TABLE_NAME, genericTable)
            .build());
    return resp;
  }

  @Override
  public Response listGenericTables(
      String prefix,
      String namespace,
      String pageToken,
      Integer pageSize,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.BEFORE_LIST_GENERIC_TABLES, eventMetadataFactory.create())
            .attribute(EventAttributes.CATALOG_NAME, catalogName)
            .attribute(EventAttributes.NAMESPACE_NAME, namespace)
            .build());
    Response resp =
        delegate.listGenericTables(
            prefix, namespace, pageToken, pageSize, realmContext, securityContext);
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.AFTER_LIST_GENERIC_TABLES, eventMetadataFactory.create())
            .attribute(EventAttributes.CATALOG_NAME, catalogName)
            .attribute(EventAttributes.NAMESPACE_NAME, namespace)
            .build());
    return resp;
  }

  @Override
  public Response loadGenericTable(
      String prefix,
      String namespace,
      String genericTable,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.BEFORE_LOAD_GENERIC_TABLE, eventMetadataFactory.create())
            .attribute(EventAttributes.CATALOG_NAME, catalogName)
            .attribute(EventAttributes.NAMESPACE_NAME, namespace)
            .attribute(EventAttributes.GENERIC_TABLE_NAME, genericTable)
            .build());
    Response resp =
        delegate.loadGenericTable(prefix, namespace, genericTable, realmContext, securityContext);
    polarisEventListener.onEvent(
        PolarisEvent.builder(
                PolarisEventType.AFTER_LOAD_GENERIC_TABLE, eventMetadataFactory.create())
            .attribute(EventAttributes.CATALOG_NAME, catalogName)
            .attribute(EventAttributes.NAMESPACE_NAME, namespace)
            .attribute(
                EventAttributes.GENERIC_TABLE,
                ((LoadGenericTableResponse) resp.getEntity()).getTable())
            .build());
    return resp;
  }
}
