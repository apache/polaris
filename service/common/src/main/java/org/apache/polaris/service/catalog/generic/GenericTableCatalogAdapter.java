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

import jakarta.enterprise.context.RequestScoped;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.catalog.api.PolarisCatalogGenericTableApiService;
import org.apache.polaris.service.types.CreateGenericTableRequest;

@RequestScoped
public class GenericTableCatalogAdapter implements PolarisCatalogGenericTableApiService {
  @Override
  public Response createGenericTable(
      String prefix,
      String namespace,
      CreateGenericTableRequest createGenericTableRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return Response.status(501).build(); // not implemented
  }

  @Override
  public Response dropGenericTable(
      String prefix,
      String namespace,
      String genericTable,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return Response.status(501).build(); // not implemented
  }

  @Override
  public Response listGenericTables(
      String prefix,
      String namespace,
      String pageToken,
      Integer pageSize,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return Response.status(501).build(); // not implemented
  }

  @Override
  public Response loadGenericTable(
      String prefix,
      String namespace,
      String genericTable,
      RealmContext realmContext,
      SecurityContext securityContext) {
    return Response.status(501).build(); // not implemented
  }
}
