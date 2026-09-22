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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.openlineage.server.OpenLineage;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.List;
import org.apache.polaris.service.lineage.api.PolarisLineageEvent;

/** Builders for the three OpenLineage event shapes, so tests can stay about behaviour. */
final class LineageTestEvents {

  static final URI PRODUCER = URI.create("https://example.test/producer");
  static final URI SCHEMA_URL = URI.create("https://openlineage.io/spec/1-0-5/OpenLineage.json");
  static final ZonedDateTime EVENT_TIME = ZonedDateTime.parse("2026-09-19T12:00:00Z");

  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .findAndRegisterModules()
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

  private LineageTestEvents() {}

  static OpenLineage.InputDataset input(String namespace, String name) {
    return new OpenLineage.InputDataset(namespace, name, null, null);
  }

  static OpenLineage.OutputDataset output(String namespace, String name) {
    return new OpenLineage.OutputDataset(namespace, name, null, null);
  }

  static OpenLineage.StaticDataset staticDataset(String namespace, String name) {
    return new OpenLineage.StaticDataset(namespace, name, null);
  }

  static OpenLineage.RunEvent runEvent(
      List<OpenLineage.InputDataset> inputs, List<OpenLineage.OutputDataset> outputs) {
    return new OpenLineage.RunEvent(
        EVENT_TIME,
        PRODUCER,
        SCHEMA_URL,
        OpenLineage.RunEvent.EventType.COMPLETE,
        null,
        null,
        inputs,
        outputs);
  }

  static OpenLineage.JobEvent jobEvent(
      List<OpenLineage.InputDataset> inputs, List<OpenLineage.OutputDataset> outputs) {
    return new OpenLineage.JobEvent(EVENT_TIME, PRODUCER, SCHEMA_URL, null, inputs, outputs);
  }

  static OpenLineage.DatasetEvent datasetEvent(OpenLineage.StaticDataset dataset) {
    return new OpenLineage.DatasetEvent(EVENT_TIME, PRODUCER, SCHEMA_URL, dataset);
  }

  /**
   * An input dataset carrying {@code facets}. The facets are built as JSON because the model's
   * dataset facets can only be populated by deserialization.
   */
  static OpenLineage.InputDataset inputWithFacets(
      String namespace, String name, ObjectNode facets) {
    return dataset(OpenLineage.InputDataset.class, namespace, name, facets);
  }

  static OpenLineage.OutputDataset outputWithFacets(
      String namespace, String name, ObjectNode facets) {
    return dataset(OpenLineage.OutputDataset.class, namespace, name, facets);
  }

  /**
   * The dataset facets OpenLineage Spark 1.53.0 emits for a table in an Iceberg REST catalog: a
   * {@code catalog} facet built from the catalog's {@code uri} and {@code warehouse}, and a {@code
   * TABLE} symlink named by Spark's {@code Identifier.toString()}.
   */
  static ObjectNode sparkFacets(String catalogUri, String warehouse, String qualifiedTable) {
    return facets(
        catalogFacet("iceberg", "rest", catalogUri, warehouse),
        symlinksFacet(symlink(catalogUri, qualifiedTable, "TABLE")));
  }

  static ObjectNode facets(ObjectNode catalog, ObjectNode symlinks) {
    ObjectNode facets = MAPPER.createObjectNode();
    if (catalog != null) {
      facets.set("catalog", catalog);
    }
    if (symlinks != null) {
      facets.set("symlinks", symlinks);
    }
    return facets;
  }

  /** A {@code catalog} facet. A {@code null} field is left out, as OpenLineage does. */
  static ObjectNode catalogFacet(
      String framework, String type, String metadataUri, String warehouseUri) {
    ObjectNode facet = baseFacet("CatalogDatasetFacet");
    facet.put("name", "polaris").put("source", "spark");
    putIfNotNull(facet, "framework", framework);
    putIfNotNull(facet, "type", type);
    putIfNotNull(facet, "metadataUri", metadataUri);
    putIfNotNull(facet, "warehouseUri", warehouseUri);
    return facet;
  }

  static ObjectNode symlinksFacet(ObjectNode... identifiers) {
    ObjectNode facet = baseFacet("SymlinksDatasetFacet");
    ArrayNode array = facet.putArray("identifiers");
    for (ObjectNode identifier : identifiers) {
      array.add(identifier);
    }
    return facet;
  }

  static ObjectNode symlink(String namespace, String name, String type) {
    ObjectNode identifier = MAPPER.createObjectNode();
    putIfNotNull(identifier, "namespace", namespace);
    putIfNotNull(identifier, "name", name);
    putIfNotNull(identifier, "type", type);
    return identifier;
  }

  /** Parses {@code json} the way the ingest endpoint does. */
  static OpenLineage.BaseEvent parseEvent(String json) {
    try {
      return MAPPER.readValue(json, PolarisLineageEvent.class).event();
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private static ObjectNode baseFacet(String facetType) {
    ObjectNode facet = MAPPER.createObjectNode();
    facet.put("_producer", PRODUCER.toString());
    facet.put(
        "_schemaURL",
        "https://openlineage.io/spec/facets/1-0-0/" + facetType + ".json#/$defs/" + facetType);
    return facet;
  }

  private static void putIfNotNull(ObjectNode node, String field, String value) {
    if (value != null) {
      node.put(field, value);
    }
  }

  private static <T> T dataset(Class<T> type, String namespace, String name, ObjectNode facets) {
    ObjectNode dataset = MAPPER.createObjectNode();
    putIfNotNull(dataset, "namespace", namespace);
    putIfNotNull(dataset, "name", name);
    dataset.set("facets", facets);
    try {
      return MAPPER.treeToValue(dataset, type);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
