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
import java.util.ArrayList;
import java.util.List;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.lineage.api.OpenLineageBatchIngestResponse;
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
 *
 * <p>Batch ingest is aggregated here rather than on the provider seam: the adapter fans a batch out
 * into per-event {@link OpenLineageIngestProvider#ingest} calls and assembles the per-event
 * outcomes into an {@link OpenLineageBatchIngestResponse}. This keeps the provider contract a
 * single event in / single result out.
 *
 * <p>When {@link FeatureConfiguration#ENABLE_OPENLINEAGE_INGEST} is disabled for the realm, both
 * endpoints return {@code 404 Not Found} without invoking the provider. The routes stay mounted
 * whenever the extension is assembled, so this flag is the runtime switch that turns ingest on or
 * off (and it also gates whether the endpoints are advertised during discovery).
 */
@RequestScoped
public class OpenLineageAdapter implements PolarisOpenLineageApiService {

  private final OpenLineageIngestProvider provider;
  private final RealmConfig realmConfig;

  @Inject
  public OpenLineageAdapter(OpenLineageIngestProvider provider, RealmConfig realmConfig) {
    this.provider = provider;
    this.realmConfig = realmConfig;
  }

  @Override
  public Response sendLineageEvent(
      PolarisLineageEvent event, RealmContext realmContext, SecurityContext securityContext) {
    if (!openLineageEnabled()) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    OpenLineageIngestResult result = ingestOne(event, realmContext);
    return toResponse(result);
  }

  @Override
  public Response sendLineageEventBatch(
      List<PolarisLineageEvent> events,
      RealmContext realmContext,
      SecurityContext securityContext) {
    if (!openLineageEnabled()) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    int successful = 0;
    List<OpenLineageBatchIngestResponse.FailedEvent> failed = new ArrayList<>();

    for (int i = 0; i < events.size(); i++) {
      OpenLineageIngestResult result = ingestOne(events.get(i), realmContext);
      switch (result) {
        case ACCEPTED -> successful++;
        case REJECTED ->
            failed.add(new OpenLineageBatchIngestResponse.FailedEvent(i, false, "Event rejected"));
        case UNAVAILABLE ->
            failed.add(
                new OpenLineageBatchIngestResponse.FailedEvent(
                    i, true, "Ingest backend unavailable"));
      }
    }

    OpenLineageBatchIngestResponse.Status status;
    if (failed.isEmpty()) {
      status = OpenLineageBatchIngestResponse.Status.SUCCESS;
    } else if (successful == 0) {
      status = OpenLineageBatchIngestResponse.Status.FAILURE;
    } else {
      status = OpenLineageBatchIngestResponse.Status.PARTIAL;
    }

    OpenLineageBatchIngestResponse body =
        new OpenLineageBatchIngestResponse(
            status,
            new OpenLineageBatchIngestResponse.Summary(events.size(), successful, failed.size()),
            failed);
    return Response.status(Response.Status.OK).entity(body).build();
  }

  private boolean openLineageEnabled() {
    return realmConfig.getConfig(FeatureConfiguration.ENABLE_OPENLINEAGE_INGEST);
  }

  private OpenLineageIngestResult ingestOne(PolarisLineageEvent event, RealmContext realmContext) {
    OpenLineageIngestRequest request =
        new OpenLineageIngestRequest(event.event(), realmContext.getRealmIdentifier());
    return provider.ingest(request);
  }

  private static Response toResponse(OpenLineageIngestResult result) {
    return switch (result) {
      case ACCEPTED -> Response.status(Response.Status.CREATED).build();
      case REJECTED -> Response.status(Response.Status.BAD_REQUEST).build();
      case UNAVAILABLE -> Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    };
  }
}
