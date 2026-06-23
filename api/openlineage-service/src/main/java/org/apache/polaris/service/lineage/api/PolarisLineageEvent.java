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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import io.openlineage.server.OpenLineage;

/**
 * Polaris-side wrapper for an OpenLineage event accepted by the lineage ingest endpoint.
 *
 * <p>Targets the OpenLineage spec as implemented by {@code io.openlineage:openlineage-java}
 * 1.48.0. The spec itself is at <a
 * href="https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json">OpenLineage.json</a>;
 * the event model is defined under {@code /definitions/BaseEvent}, {@code RunEvent}, {@code
 * JobEvent}, and {@code DatasetEvent}.
 *
 * <p>The OpenLineage spec models events as a {@code oneOf} over {@code RunEvent}, {@code
 * JobEvent}, and {@code DatasetEvent}, discriminated by the value of the {@code schemaURL} field.
 * The official {@code openlineage-java} library ships POJOs for these three event types in {@link
 * io.openlineage.server.OpenLineage} but does not annotate them for polymorphic JSON
 * deserialization. This class provides that polymorphism on top.
 *
 * <p>Wrappers compose the OpenLineage event by reference rather than extending it because the
 * {@code OpenLineage.*Event} classes are {@code final}. {@link #event()} returns the underlying
 * event for follow-up code that needs typed access to fields (inputs, outputs, columnLineage
 * facet, etc.).
 *
 * <p>This pattern follows Marquez (the OpenLineage reference server), which uses the same
 * {@code @JsonTypeInfo} + {@code @JsonTypeIdResolver} discrimination on {@code schemaURL}.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.CUSTOM,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "schemaURL",
    defaultImpl = PolarisLineageEvent.OfRunEvent.class,
    visible = true)
@JsonTypeIdResolver(LineageEventTypeResolver.class)
public abstract sealed class PolarisLineageEvent
    permits PolarisLineageEvent.OfRunEvent,
        PolarisLineageEvent.OfJobEvent,
        PolarisLineageEvent.OfDatasetEvent {

  /** Returns the underlying OpenLineage event. */
  public abstract OpenLineage.BaseEvent event();

  /** Wraps an {@link OpenLineage.RunEvent}. */
  public static final class OfRunEvent extends PolarisLineageEvent {
    private final OpenLineage.RunEvent event;

    @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
    public OfRunEvent(OpenLineage.RunEvent event) {
      this.event = event;
    }

    @JsonValue
    @Override
    public OpenLineage.RunEvent event() {
      return event;
    }
  }

  /** Wraps an {@link OpenLineage.JobEvent}. */
  public static final class OfJobEvent extends PolarisLineageEvent {
    private final OpenLineage.JobEvent event;

    @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
    public OfJobEvent(OpenLineage.JobEvent event) {
      this.event = event;
    }

    @JsonValue
    @Override
    public OpenLineage.JobEvent event() {
      return event;
    }
  }

  /** Wraps an {@link OpenLineage.DatasetEvent}. */
  public static final class OfDatasetEvent extends PolarisLineageEvent {
    private final OpenLineage.DatasetEvent event;

    @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
    public OfDatasetEvent(OpenLineage.DatasetEvent event) {
      this.event = event;
    }

    @JsonValue
    @Override
    public OpenLineage.DatasetEvent event() {
      return event;
    }
  }
}
