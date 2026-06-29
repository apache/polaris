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
package org.apache.polaris.service.catalog.config;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.catalog.api.PolarisCatalogConfigApiService;
import org.apache.polaris.service.catalog.common.CatalogAdapter;

@RequestScoped
public class PolarisCatalogConfigAdapter implements PolarisCatalogConfigApiService, CatalogAdapter {
  private final CatalogConfigHandler configHandler;

  @Inject
  public PolarisCatalogConfigAdapter(CatalogConfigHandler configHandler) {
    this.configHandler = configHandler;
  }

  @Override
  public Response getPolarisConfig(
      String warehouse, RealmContext realmContext, SecurityContext securityContext) {
    if (warehouse == null) {
      throw new BadRequestException("Please specify a warehouse");
    }
    return Response.ok(configHandler.getConfig(warehouse, validatePrincipal(securityContext)))
        .build();
  }
}
