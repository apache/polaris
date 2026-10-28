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

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.openlineage.server.OpenLineage;

/**
 * Polaris-side wrapper for an OpenLineage event accepted by the lineage ingest endpoint.
 *
 * <p>Targets the OpenLineage spec as implemented by {@code io.openlineage:openlineage-java} 1.48.0.
 * The spec itself is at <a
 * href="https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json">OpenLineage.json</a>;
 * the event model is defined under {@code /definitions/BaseEvent}, {@code RunEvent}, {@code
 * JobEvent}, and {@code DatasetEvent}.
 *
 * <p>The OpenLineage spec models events as a {@code oneOf} over {@code RunEvent}, {@code JobEvent},
 * and {@code DatasetEvent}, discriminated by the value of the {@code schemaURL} field. The official
 * {@code openlineage-java} library ships POJOs for these three event types in {@link
 * io.openlineage.server.OpenLineage} but does not annotate them for polymorphic JSON
 * deserialization. This class wraps the underlying event and delegates the {@code schemaURL}-keyed
 * dispatch to {@link PolarisLineageEventDeserializer}.
 *
 * <p>Wrappers compose the OpenLineage event by reference rather than extending it because the
 * {@code OpenLineage.*Event} classes are {@code final}. {@link #event()} returns the underlying
 * event for follow-up code that needs typed access to fields (inputs, outputs, columnLineage facet,
 * etc.), and is also the JSON value ({@link JsonValue}) so serialization round-trips to the raw
 * OpenLineage event carrying its {@code schemaURL}.
 *
 * <p>This pattern follows Marquez (the OpenLineage reference server), which similarly dispatches on
 * {@code schemaURL}.
 */
@JsonDeserialize(using = PolarisLineageEventDeserializer.class)
public final class PolarisLineageEvent {

  private final OpenLineage.BaseEvent event;

  public PolarisLineageEvent(OpenLineage.BaseEvent event) {
    this.event = event;
  }

  /**
   * Returns the underlying OpenLineage event ({@code RunEvent}, {@code JobEvent}, or {@code
   * DatasetEvent}).
   */
  @JsonValue
  public OpenLineage.BaseEvent event() {
    return event;
  }
}
