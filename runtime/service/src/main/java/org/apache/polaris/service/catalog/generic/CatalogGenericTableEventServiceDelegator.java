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
import org.apache.polaris.service.events.CatalogGenericTableServiceEvents;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.apache.polaris.service.types.CreateGenericTableRequest;
import org.apache.polaris.service.types.LoadGenericTableResponse;

@Decorator
@Priority(1000)
public class CatalogGenericTableEventServiceDelegator
    implements PolarisCatalogGenericTableApiService, CatalogAdapter {

  @Inject @Delegate GenericTableCatalogAdapter delegate;
  @Inject PolarisEventListener polarisEventListener;
  @Inject CatalogPrefixParser prefixParser;

  @Override
  public Response createGenericTable(
      String prefix,
      String namespace,
      CreateGenericTableRequest createGenericTableRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(prefix);
    polarisEventListener.onBeforeCreateGenericTable(
        new CatalogGenericTableServiceEvents.BeforeCreateGenericTableEvent(
            catalogName, namespace, createGenericTableRequest));
    Response resp =
        delegate.createGenericTable(
            prefix, namespace, createGenericTableRequest, realmContext, securityContext);
    polarisEventListener.onAfterCreateGenericTable(
        new CatalogGenericTableServiceEvents.AfterCreateGenericTableEvent(
            catalogName, namespace, ((LoadGenericTableResponse) resp.getEntity()).getTable()));
    return resp;
  }

  @Override
  public Response dropGenericTable(
      String prefix,
      String namespace,
      String genericTable,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(prefix);
    polarisEventListener.onBeforeDropGenericTable(
        new CatalogGenericTableServiceEvents.BeforeDropGenericTableEvent(
            catalogName, namespace, genericTable));
    Response resp =
        delegate.dropGenericTable(prefix, namespace, genericTable, realmContext, securityContext);
    polarisEventListener.onAfterDropGenericTable(
        new CatalogGenericTableServiceEvents.AfterDropGenericTableEvent(
            catalogName, namespace, genericTable));
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
    String catalogName = prefixParser.prefixToCatalogName(prefix);
    polarisEventListener.onBeforeListGenericTables(
        new CatalogGenericTableServiceEvents.BeforeListGenericTablesEvent(catalogName, namespace));
    Response resp =
        delegate.listGenericTables(
            prefix, namespace, pageToken, pageSize, realmContext, securityContext);
    polarisEventListener.onAfterListGenericTables(
        new CatalogGenericTableServiceEvents.AfterListGenericTablesEvent(catalogName, namespace));
    return resp;
  }

  @Override
  public Response loadGenericTable(
      String prefix,
      String namespace,
      String genericTable,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(prefix);
    polarisEventListener.onBeforeLoadGenericTable(
        new CatalogGenericTableServiceEvents.BeforeLoadGenericTableEvent(
            catalogName, namespace, genericTable));
    Response resp =
        delegate.loadGenericTable(prefix, namespace, genericTable, realmContext, securityContext);
    polarisEventListener.onAfterLoadGenericTable(
        new CatalogGenericTableServiceEvents.AfterLoadGenericTableEvent(
            catalogName, namespace, ((LoadGenericTableResponse) resp.getEntity()).getTable()));
    return resp;
  }
}
