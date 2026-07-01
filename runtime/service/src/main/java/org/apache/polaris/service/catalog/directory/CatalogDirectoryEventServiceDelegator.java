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

package org.apache.polaris.service.catalog.directory;

import jakarta.annotation.Priority;
import jakarta.decorator.Decorator;
import jakarta.decorator.Delegate;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.api.PolarisCatalogDirectoryApiService;
import org.apache.polaris.service.catalog.common.CatalogAdapter;
import org.apache.polaris.service.events.EventAttributeMap;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventDispatcher;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.events.PolarisEventType;
import org.apache.polaris.service.types.CreateDirectoryRequest;
import org.apache.polaris.service.types.LoadDirectoryResponse;

@Decorator
@Priority(1000)
public class CatalogDirectoryEventServiceDelegator
    implements PolarisCatalogDirectoryApiService, CatalogAdapter {

  @Inject @Delegate DirectoryCatalogAdapter delegate;
  @Inject PolarisEventDispatcher polarisEventDispatcher;
  @Inject PolarisEventMetadataFactory eventMetadataFactory;
  @Inject CatalogPrefixParser prefixParser;

  @Override
  public Response createDirectory(
      String prefix,
      String namespace,
      CreateDirectoryRequest createDirectoryRequest,
      String polarisDirectoryAccessDelegation,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(prefix);
    polarisEventDispatcher.dispatch(
        new PolarisEvent(
            PolarisEventType.BEFORE_CREATE_DIRECTORY,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE_NAME, namespace)
                .put(EventAttributes.CREATE_DIRECTORY_REQUEST, createDirectoryRequest)));
    Response resp =
        delegate.createDirectory(
            prefix,
            namespace,
            createDirectoryRequest,
            polarisDirectoryAccessDelegation,
            realmContext,
            securityContext);
    polarisEventDispatcher.dispatch(
        new PolarisEvent(
            PolarisEventType.AFTER_CREATE_DIRECTORY,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE_NAME, namespace)
                .put(
                    EventAttributes.DIRECTORY,
                    ((LoadDirectoryResponse) resp.getEntity()).getDirectory())));
    return resp;
  }

  @Override
  public Response dropDirectory(
      String prefix,
      String namespace,
      String directory,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(prefix);
    polarisEventDispatcher.dispatch(
        new PolarisEvent(
            PolarisEventType.BEFORE_DROP_DIRECTORY,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE_NAME, namespace)
                .put(EventAttributes.DIRECTORY_NAME, directory)));
    Response resp =
        delegate.dropDirectory(prefix, namespace, directory, realmContext, securityContext);
    polarisEventDispatcher.dispatch(
        new PolarisEvent(
            PolarisEventType.AFTER_DROP_DIRECTORY,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE_NAME, namespace)
                .put(EventAttributes.DIRECTORY_NAME, directory)));
    return resp;
  }

  @Override
  public Response listDirectories(
      String prefix,
      String namespace,
      String pageToken,
      Integer pageSize,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(prefix);
    polarisEventDispatcher.dispatch(
        new PolarisEvent(
            PolarisEventType.BEFORE_LIST_DIRECTORIES,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE_NAME, namespace)));
    Response resp =
        delegate.listDirectories(
            prefix, namespace, pageToken, pageSize, realmContext, securityContext);
    polarisEventDispatcher.dispatch(
        new PolarisEvent(
            PolarisEventType.AFTER_LIST_DIRECTORIES,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE_NAME, namespace)));
    return resp;
  }

  @Override
  public Response loadDirectory(
      String prefix,
      String namespace,
      String directory,
      String polarisDirectoryAccessDelegation,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(prefix);
    polarisEventDispatcher.dispatch(
        new PolarisEvent(
            PolarisEventType.BEFORE_LOAD_DIRECTORY,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE_NAME, namespace)
                .put(EventAttributes.DIRECTORY_NAME, directory)));
    Response resp =
        delegate.loadDirectory(
            prefix,
            namespace,
            directory,
            polarisDirectoryAccessDelegation,
            realmContext,
            securityContext);
    polarisEventDispatcher.dispatch(
        new PolarisEvent(
            PolarisEventType.AFTER_LOAD_DIRECTORY,
            eventMetadataFactory.create(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, catalogName)
                .put(EventAttributes.NAMESPACE_NAME, namespace)
                .put(
                    EventAttributes.DIRECTORY,
                    ((LoadDirectoryResponse) resp.getEntity()).getDirectory())));
    return resp;
  }
}
