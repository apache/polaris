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
package org.apache.polaris.service.lineage;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.lineage.api.OpenLineageIngestProvider;
import org.apache.polaris.service.lineage.api.OpenLineageIngestRequest;
import org.apache.polaris.service.lineage.api.OpenLineageIngestResult;
import org.apache.polaris.service.lineage.api.PolarisLineageEvent;
import org.apache.polaris.service.lineage.api.PolarisOpenLineageApiService;

/**
 * Adapter between the JAX-RS OpenLineage resource and the {@link OpenLineageIngestProvider}.
 *
 * <p>Responsible for translating the HTTP request context into an {@link OpenLineageIngestRequest}
 * and mapping the provider result back to a JAX-RS {@link Response}. Provider implementations do
 * not interact with JAX-RS types.
 */
@RequestScoped
public class OpenLineageAdapter implements PolarisOpenLineageApiService {

  private final OpenLineageIngestProvider provider;

  @Inject
  public OpenLineageAdapter(OpenLineageIngestProvider provider) {
    this.provider = provider;
  }

  @Override
  public Response sendLineageEvent(
      PolarisLineageEvent event, RealmContext realmContext, SecurityContext securityContext) {
    OpenLineageIngestRequest request =
        new OpenLineageIngestRequest(event.event(), realmContext.getRealmIdentifier());
    OpenLineageIngestResult result = provider.ingest(request);
    return toResponse(result);
  }

  private static Response toResponse(OpenLineageIngestResult result) {
    return switch (result) {
      case ACCEPTED -> Response.status(Response.Status.CREATED).build();
      case REJECTED -> Response.status(Response.Status.BAD_REQUEST).build();
      case UNAVAILABLE -> Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    };
  }
}
