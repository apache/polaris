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
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.catalog.api.IcebergRestConfigurationApiService;
import org.apache.polaris.service.events.AttributeMap;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.events.PolarisEventType;
import org.apache.polaris.service.events.listeners.PolarisEventListener;

@Decorator
@Priority(1000)
public class IcebergRestConfigurationEventServiceDelegator
    implements IcebergRestConfigurationApiService {

  @Inject @Delegate IcebergCatalogAdapter delegate;
  @Inject PolarisEventListener polarisEventListener;
  @Inject PolarisEventMetadataFactory eventMetadataFactory;

  @VisibleForTesting
  public IcebergRestConfigurationEventServiceDelegator(
      IcebergCatalogAdapter delegate,
      PolarisEventListener polarisEventListener,
      PolarisEventMetadataFactory eventMetadataFactory) {
    this.delegate = delegate;
    this.polarisEventListener = polarisEventListener;
    this.eventMetadataFactory = eventMetadataFactory;
  }

  public IcebergRestConfigurationEventServiceDelegator() {}

  @Override
  public Response getConfig(
      String warehouse, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_GET_CONFIG,
            eventMetadataFactory.create(),
            new AttributeMap().put(EventAttributes.WAREHOUSE, warehouse)));
    Response resp = delegate.getConfig(warehouse, realmContext, securityContext);
    polarisEventListener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_GET_CONFIG,
            eventMetadataFactory.create(),
            new AttributeMap()
                .put(EventAttributes.CONFIG_RESPONSE, (ConfigResponse) resp.getEntity())));
    return resp;
  }
}
