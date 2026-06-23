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
package org.apache.polaris.service.lineage.api;

import io.micrometer.core.annotation.Timed;
import io.micrometer.core.aop.MeterTag;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.polaris.core.context.RealmContext;
import org.eclipse.microprofile.faulttolerance.Timeout;

/**
 * JAX-RS resource for the OpenLineage ingest endpoint.
 *
 * <p>Implements the <a
 * href="https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.yml">OpenLineage
 * HTTP API spec</a>. Mounted at the standard OpenLineage path ({@code POST /api/v1/lineage}) so
 * that any engine already using the OpenLineage HTTP transport (Spark, Flink, Airflow, Trino, dbt)
 * can target Polaris by URL change alone — no client-side rewriting.
 *
 * <p>The body is parsed into a {@link PolarisLineageEvent} which Jackson resolves to one of {@code
 * OfRunEvent} / {@code OfJobEvent} / {@code OfDatasetEvent} based on the {@code schemaURL} field.
 * Unrecognized {@code schemaURL} values fall back to {@code RunEvent}, matching Marquez behavior.
 *
 * <p>This resource is hand-written rather than generated from the OpenLineage JSON Schema because
 * the OpenAPI generator's Java template does not faithfully translate the spec's {@code oneOf}
 * over event variants.
 */
@Path("/api/v1/lineage")
public class PolarisOpenLineageApi {

  private final PolarisOpenLineageApiService service;

  @Inject
  public PolarisOpenLineageApi(PolarisOpenLineageApiService service) {
    this.service = service;
  }

  @POST
  @Consumes("application/json")
  @RolesAllowed("**")
  @Timed("polaris.OpenLineageApi.sendLineageEvent")
  @Timeout
  public Response sendLineageEvent(
      @NotNull @Valid PolarisLineageEvent event,
      @Context @MeterTag(key = "realm_id", expression = "realmIdentifier")
          RealmContext realmContext,
      @Context @MeterTag(key = "principal", expression = "userPrincipal")
          SecurityContext securityContext) {
    return service.sendLineageEvent(event, realmContext, securityContext);
  }
}
