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
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.catalog.api.IcebergRestConfigurationApiService;
import org.apache.polaris.service.events.IcebergRestCatalogEvents;
import org.apache.polaris.service.events.listeners.PolarisEventListener;

@Decorator
@Priority(1000)
public class IcebergRestConfigurationEventServiceDelegator
    implements IcebergRestConfigurationApiService {

  @Inject @Delegate IcebergCatalogAdapter delegate;
  @Inject PolarisEventListener polarisEventListener;

  @Override
  public Response getConfig(
      String warehouse, RealmContext realmContext, SecurityContext securityContext) {
    polarisEventListener.onBeforeGetConfig(
        new IcebergRestCatalogEvents.BeforeGetConfigEvent(warehouse));
    Response resp = delegate.getConfig(warehouse, realmContext, securityContext);
    polarisEventListener.onAfterGetConfig(
        new IcebergRestCatalogEvents.AfterGetConfigEvent(resp.readEntity(ConfigResponse.class)));
    return resp;
  }
}
