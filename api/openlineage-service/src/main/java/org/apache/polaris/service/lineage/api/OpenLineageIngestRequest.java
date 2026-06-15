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

import io.openlineage.server.OpenLineage;

/**
 * Polaris-owned request type passed to {@link OpenLineageIngestProvider}.
 *
 * <p>Carries the already-dispatched OpenLineage event and the realm identifier. Provider
 * implementations do not need to know about the JAX-RS binding, {@link PolarisLineageEvent}
 * wrappers, or {@code schemaURL} fallback semantics — those concerns are resolved before this
 * object is constructed.
 */
public final class OpenLineageIngestRequest {

  private final OpenLineage.BaseEvent event;
  private final String realmId;

  public OpenLineageIngestRequest(OpenLineage.BaseEvent event, String realmId) {
    this.event = event;
    this.realmId = realmId;
  }

  /** The parsed OpenLineage event (one of {@code RunEvent}, {@code JobEvent}, {@code DatasetEvent}). */
  public OpenLineage.BaseEvent event() {
    return event;
  }

  /** The realm identifier from the ingest request context. */
  public String realmId() {
    return realmId;
  }
}
