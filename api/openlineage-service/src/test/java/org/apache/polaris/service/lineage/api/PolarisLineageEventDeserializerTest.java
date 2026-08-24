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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.server.OpenLineage;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@code schemaURL} → event-variant dispatch performed by {@link
 * PolarisLineageEventDeserializer} when Jackson deserializes a {@link PolarisLineageEvent}, plus
 * the serialization round-trip enabled by {@link PolarisLineageEvent#event()}.
 *
 * <p>Dispatch is the only real logic in the ingest path: the deserializer inspects the trailing
 * segment of {@code schemaURL} to decide which {@link OpenLineage} event type the body deserializes
 * into. The no-op integration tests return {@code 201} for every well-formed event and so cannot
 * distinguish a correct dispatch from a silent fallback; these tests pin the dispatch itself so a
 * follow-up PR that starts consuming {@link PolarisLineageEvent#event()} cannot regress it
 * unnoticed. The serialization tests guard the custom Jackson wiring in the other direction.
 */
class PolarisLineageEventDeserializerTest {

  private static final String SCHEMA_BASE = "https://openlineage.io/spec/2-0-2/OpenLineage.json";

  // The event payloads below mirror those exercised end-to-end by OpenLineageServiceIT so the unit
  // and integration layers agree on the wire shape.
  private static final String RUN_EVENT =
      """
      {
        "eventTime": "2024-01-01T00:00:00Z",
        "eventType": "START",
        "run": {"runId": "123e4567-e89b-12d3-a456-426614174000"},
        "job": {"namespace": "test", "name": "job"},
        "producer": "https://example.com",
        "schemaURL": "%s#/$defs/RunEvent",
        "inputs": [{"namespace": "db", "name": "orders_raw"}],
        "outputs": [{"namespace": "db", "name": "orders_daily"}]
      }
      """
          .formatted(SCHEMA_BASE);

  private static final String JOB_EVENT =
      """
      {
        "eventTime": "2024-01-01T00:00:00Z",
        "job": {"namespace": "test", "name": "job"},
        "producer": "https://example.com",
        "schemaURL": "%s#/$defs/JobEvent"
      }
      """
          .formatted(SCHEMA_BASE);

  private static final String DATASET_EVENT =
      """
      {
        "eventTime": "2024-01-01T00:00:00Z",
        "dataset": {"namespace": "db", "name": "orders_daily"},
        "producer": "https://example.com",
        "schemaURL": "%s#/$defs/DatasetEvent"
      }
      """
          .formatted(SCHEMA_BASE);

  private static final String UNKNOWN_SCHEMA_URL_EVENT =
      """
      {
        "eventTime": "2024-01-01T00:00:00Z",
        "eventType": "START",
        "run": {"runId": "123e4567-e89b-12d3-a456-426614174000"},
        "job": {"namespace": "test", "name": "job"},
        "producer": "https://example.com",
        "schemaURL": "https://example.com/UnknownEvent"
      }
      """;

  private static final String MISSING_SCHEMA_URL_EVENT =
      """
      {
        "eventTime": "2024-01-01T00:00:00Z",
        "eventType": "START",
        "run": {"runId": "123e4567-e89b-12d3-a456-426614174000"},
        "job": {"namespace": "test", "name": "job"},
        "producer": "https://example.com"
      }
      """;

  // A well-formed JobEvent whose schemaURL ends in "/JobEvent" but does NOT carry the OpenLineage
  // schema prefix. The discriminator must not be trusted, so this falls back to RunEvent.
  private static final String SPOOFED_PREFIX_JOB_EVENT =
      """
      {
        "eventTime": "2024-01-01T00:00:00Z",
        "job": {"namespace": "test", "name": "job"},
        "producer": "https://example.com",
        "schemaURL": "https://evil.example.com/spec/2-0-2/OpenLineage.json#/$defs/JobEvent"
      }
      """;

  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .findAndRegisterModules()
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

  private static PolarisLineageEvent parse(String json) throws Exception {
    return MAPPER.readValue(json, PolarisLineageEvent.class);
  }

  @Test
  void runEventSchemaUrlDispatchesToRunEvent() throws Exception {
    assertThat(parse(RUN_EVENT).event()).isInstanceOf(OpenLineage.RunEvent.class);
  }

  @Test
  void jobEventSchemaUrlDispatchesToJobEvent() throws Exception {
    assertThat(parse(JOB_EVENT).event()).isInstanceOf(OpenLineage.JobEvent.class);
  }

  @Test
  void datasetEventSchemaUrlDispatchesToDatasetEvent() throws Exception {
    assertThat(parse(DATASET_EVENT).event()).isInstanceOf(OpenLineage.DatasetEvent.class);
  }

  @Test
  void unrecognizedSchemaUrlFallsBackToRunEvent() throws Exception {
    assertThat(parse(UNKNOWN_SCHEMA_URL_EVENT).event()).isInstanceOf(OpenLineage.RunEvent.class);
  }

  @Test
  void missingSchemaUrlFallsBackToRunEvent() throws Exception {
    assertThat(parse(MISSING_SCHEMA_URL_EVENT).event()).isInstanceOf(OpenLineage.RunEvent.class);
  }

  @Test
  void schemaUrlWithNonOpenLineagePrefixFallsBackToRunEvent() throws Exception {
    assertThat(parse(SPOOFED_PREFIX_JOB_EVENT).event()).isInstanceOf(OpenLineage.RunEvent.class);
  }

  @Test
  void runEventRoundTripsToJsonCarryingSchemaUrl() throws Exception {
    PolarisLineageEvent parsed = parse(RUN_EVENT);
    JsonNode serialized = MAPPER.valueToTree(parsed);

    // @JsonValue means the wrapper serializes as the raw OpenLineage event, not a nested object.
    assertThat(serialized.path("schemaURL").asText()).endsWith("/RunEvent");
    assertThat(serialized.path("run").path("runId").asText())
        .isEqualTo("123e4567-e89b-12d3-a456-426614174000");
    assertThat(
            MAPPER.readValue(MAPPER.writeValueAsString(parsed), PolarisLineageEvent.class).event())
        .isInstanceOf(OpenLineage.RunEvent.class);
  }

  @Test
  void datasetEventRoundTripsToJsonCarryingSchemaUrl() throws Exception {
    PolarisLineageEvent parsed = parse(DATASET_EVENT);
    JsonNode serialized = MAPPER.valueToTree(parsed);

    assertThat(serialized.path("schemaURL").asText()).endsWith("/DatasetEvent");
    assertThat(
            MAPPER.readValue(MAPPER.writeValueAsString(parsed), PolarisLineageEvent.class).event())
        .isInstanceOf(OpenLineage.DatasetEvent.class);
  }
}
