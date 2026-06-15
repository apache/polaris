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
import jakarta.ws.rs.core.SecurityContext;
import org.apache.polaris.core.context.RealmContext;

/**
 * API/runtime delegation seam between the JAX-RS OpenLineage resource and the runtime. Mirrors the
 * pattern used by other Polaris API modules where the JAX-RS resource sits in the API module and
 * delegates to a CDI-scoped implementation in {@code polaris-runtime-service}.
 *
 * <p>This interface is the HTTP/runtime boundary: it accepts JAX-RS and runtime types ({@link
 * PolarisLineageEvent}, {@link RealmContext}, {@link SecurityContext}) and returns a JAX-RS {@link
 * Response}. It is intentionally <em>not</em> the extension point for downstream OpenLineage ingest
 * behavior — that is {@link OpenLineageIngestProvider}.
 */
public interface PolarisOpenLineageApiService {

  /**
   * Handle an OpenLineage event accepted at the ingest endpoint. Implementations are responsible
   * for translating the request into an {@link OpenLineageIngestRequest} and delegating to an
   * {@link OpenLineageIngestProvider}.
   *
   * @param event the parsed OpenLineage event, dispatched to the correct {@code RunEvent}, {@code
   *     JobEvent}, or {@code DatasetEvent} variant by Jackson based on the {@code schemaURL} field.
   * @return the JAX-RS response. OpenLineage clients expect {@code 201 Created} with no body on
   *     success.
   */
  Response sendLineageEvent(
      PolarisLineageEvent event, RealmContext realmContext, SecurityContext securityContext);
}
