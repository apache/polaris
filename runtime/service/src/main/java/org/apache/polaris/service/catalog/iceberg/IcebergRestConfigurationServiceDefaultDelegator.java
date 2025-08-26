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
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Default;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.admin.EventsServiceDelegator;
import org.apache.polaris.service.admin.MainService;
import org.apache.polaris.service.catalog.api.IcebergRestConfigurationApiService;
import org.apache.polaris.service.catalog.common.CatalogAdapter;

@RequestScoped
@Default
@EventsServiceDelegator
@Alternative
@Priority(1000) // Will allow downstream project-specific delegators to be added and used
public class IcebergRestConfigurationServiceDefaultDelegator
    implements IcebergRestConfigurationApiService, CatalogAdapter {
  private final IcebergCatalogAdapter delegate;

  @Inject
  public IcebergRestConfigurationServiceDefaultDelegator(
      @MainService IcebergCatalogAdapter catalogAdapter) {
    this.delegate = catalogAdapter;
  }

  @Override
  public Response getConfig(
      String warehouse, RealmContext realmContext, SecurityContext securityContext) {
    return delegate.getConfig(warehouse, realmContext, securityContext);
  }
}
