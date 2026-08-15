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

import jakarta.ws.rs.core.Response;
import java.util.List;

/**
 * API/runtime delegation seam between the JAX-RS OpenLineage resource and the runtime. Mirrors the
 * pattern used by other Polaris API modules where the JAX-RS resource sits in the API module and
 * delegates to a CDI-scoped implementation in {@code polaris-runtime-service}.
 *
 * <p>This interface is the HTTP/runtime boundary: it accepts the parsed {@link PolarisLineageEvent}
 * and returns a JAX-RS {@link Response}. Request-scoped context ({@code RealmContext}, {@code
 * SecurityContext}) is injected into the implementation via CDI rather than threaded through these
 * methods. It is intentionally <em>not</em> the extension point for downstream OpenLineage ingest
 * behavior — that is the {@code OpenLineageIngestProvider} SPI in the OpenLineage extension module.
 */
public interface PolarisOpenLineageApiService {

  /**
   * Handle an OpenLineage event accepted at the ingest endpoint. Implementations translate the
   * event into the extension's ingest request type and delegate to the ingest provider.
   *
   * @param event the parsed OpenLineage event, dispatched to the correct {@code RunEvent}, {@code
   *     JobEvent}, or {@code DatasetEvent} variant by Jackson based on the {@code schemaURL} field.
   * @return the JAX-RS response. OpenLineage clients expect {@code 201 Created} with no body on
   *     success.
   */
  Response sendLineageEvent(PolarisLineageEvent event);

  /**
   * Handle a batch of OpenLineage events accepted at the batch ingest endpoint. Implementations
   * translate each event, delegate to the ingest provider, and aggregate the per-event outcomes.
   *
   * @param events the parsed OpenLineage events, each dispatched to the correct variant by Jackson.
   * @return the JAX-RS response. Per the OpenLineage batch API this is {@code 200 OK} with an
   *     {@link OpenLineageBatchIngestResponse} body summarizing received/successful/failed counts.
   */
  Response sendLineageEventBatch(List<PolarisLineageEvent> events);
}
