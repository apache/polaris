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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import io.openlineage.server.OpenLineage;
import java.io.IOException;

/**
 * Deserializes an OpenLineage event JSON body into a {@link PolarisLineageEvent} wrapping the
 * correct {@link OpenLineage.RunEvent}, {@link OpenLineage.JobEvent}, or {@link
 * OpenLineage.DatasetEvent} variant by inspecting the trailing path segment of the {@code
 * schemaURL} field.
 *
 * <p>The OpenLineage spec requires every event to include a {@code schemaURL} pointing at the
 * variant's JSON schema fragment, e.g. {@code
 * https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent}. The fragment name is the
 * stable discriminator across spec versions.
 *
 * <p>If {@code schemaURL} is missing or unrecognized, the body is parsed as a {@code RunEvent} —
 * the most common variant and the same fallback Marquez (the OpenLineage reference server) uses.
 *
 * <p>This replaces the earlier {@code @JsonTypeInfo} + {@code TypeIdResolver} + sealed-wrapper
 * approach: a single deserializer keeps the {@code schemaURL} dispatch in one place and avoids the
 * write-side type-id round-trip that the resolver never actually emitted.
 */
public class PolarisLineageEventDeserializer extends JsonDeserializer<PolarisLineageEvent> {

  @Override
  public PolarisLineageEvent deserialize(JsonParser parser, DeserializationContext context)
      throws IOException {
    ObjectCodec codec = parser.getCodec();
    JsonNode node = codec.readTree(parser);
    JsonNode schemaUrl = node.get("schemaURL");
    Class<? extends OpenLineage.BaseEvent> target =
        variantFor(schemaUrl == null ? null : schemaUrl.asText());
    return new PolarisLineageEvent(codec.treeToValue(node, target));
  }

  private static Class<? extends OpenLineage.BaseEvent> variantFor(String schemaUrl) {
    if (schemaUrl == null) {
      return OpenLineage.RunEvent.class;
    }
    String[] segments = schemaUrl.split("/");
    String tail = segments[segments.length - 1];
    return switch (tail) {
      case "JobEvent" -> OpenLineage.JobEvent.class;
      case "DatasetEvent" -> OpenLineage.DatasetEvent.class;
      default -> OpenLineage.RunEvent.class;
    };
  }
}
