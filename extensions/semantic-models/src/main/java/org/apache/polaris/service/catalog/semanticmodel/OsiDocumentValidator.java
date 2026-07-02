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
package org.apache.polaris.service.catalog.semanticmodel;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Utf8;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.BadRequestException;

/**
 * Validates an OSI (Open Semantic Interchange) semantic-model document against the bundled OSI JSON
 * Schema and against Polaris's configured size caps.
 *
 * <p>The Polaris API represents a document as a {@code version} string plus a {@code
 * semantic_model} JSON string (see the {@code SemanticModelDocument} API type). This validator
 * reconstructs the canonical OSI document {@code {"version": ..., "semantic_model": ...}} and
 * validates it against the frozen v0.1.1 schema (draft 2020-12), which is strict: unknown fields
 * fail because the schema declares {@code additionalProperties: false}.
 *
 * <p>Version evolution is an explicit user action: documents declaring an unsupported OSI spec
 * version are rejected rather than silently accepted or migrated.
 */
public final class OsiDocumentValidator {

  /** The single OSI spec version this build bundles and supports. */
  public static final String SUPPORTED_SPEC_VERSION = "0.1.1";

  private static final String SCHEMA_RESOURCE = "/osi/v0.1.1/osi-schema.json";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final JsonSchema SCHEMA = loadSchema();

  private OsiDocumentValidator() {}

  private static JsonSchema loadSchema() {
    try (InputStream in = OsiDocumentValidator.class.getResourceAsStream(SCHEMA_RESOURCE)) {
      if (in == null) {
        throw new IllegalStateException("Bundled OSI schema not found: " + SCHEMA_RESOURCE);
      }
      JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
      return factory.getSchema(in);
    } catch (java.io.IOException e) {
      throw new UncheckedIOException("Failed to load bundled OSI schema", e);
    }
  }

  /**
   * Validates the given OSI document. Throws {@link BadRequestException} (HTTP 400) with
   * JSON-Pointer field paths on any schema or size-cap failure.
   *
   * @param specVersion the declared OSI spec version
   * @param semanticModelJson the OSI {@code semantic_model} content, serialized as a JSON string
   * @param maxDocumentBytes maximum allowed size, in UTF-8 bytes, of the semantic-model content
   * @param maxExpressionBytes maximum allowed size, in UTF-8 bytes, of any single expression
   * @return the parsed OSI {@code semantic_model} node, for downstream source resolution
   */
  public static JsonNode validate(
      String specVersion, String semanticModelJson, int maxDocumentBytes, int maxExpressionBytes) {
    if (!SUPPORTED_SPEC_VERSION.equals(specVersion)) {
      throw new BadRequestException(
          "Unsupported OSI spec version '%s'; this server supports '%s'",
          specVersion, SUPPORTED_SPEC_VERSION);
    }

    if (semanticModelJson == null || semanticModelJson.isBlank()) {
      throw new BadRequestException("Semantic model document must not be empty");
    }

    int documentBytes = Utf8.encodedLength(semanticModelJson);
    if (documentBytes > maxDocumentBytes) {
      throw new BadRequestException(
          "Semantic model document of %d bytes exceeds the maximum of %d bytes",
          documentBytes, maxDocumentBytes);
    }

    JsonNode semanticModelNode;
    try {
      semanticModelNode = MAPPER.readTree(semanticModelJson);
    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
      throw new BadRequestException(
          "Field 'semantic_model' is not valid JSON: %s", e.getOriginalMessage());
    }

    // Reconstruct the canonical OSI document {version, semantic_model} for schema validation.
    ObjectNode osiDocument = MAPPER.createObjectNode();
    osiDocument.put("version", specVersion);
    osiDocument.set("semantic_model", semanticModelNode);

    Set<ValidationMessage> errors = SCHEMA.validate(osiDocument);
    if (!errors.isEmpty()) {
      String details =
          errors.stream()
              .map(m -> m.getInstanceLocation() + ": " + m.getMessage())
              .sorted()
              .collect(Collectors.joining("; "));
      throw new BadRequestException(
          "Semantic model document failed OSI schema validation: %s", details);
    }

    enforceExpressionCap(semanticModelNode, maxExpressionBytes);

    return semanticModelNode;
  }

  /**
   * Walks the document for SQL/expression fragments (the {@code expression} string inside each
   * {@code dialects[]} entry) and rejects any that exceed the per-expression cap.
   */
  private static void enforceExpressionCap(JsonNode node, int maxExpressionBytes) {
    if (node.isObject()) {
      JsonNode dialects = node.get("dialects");
      if (dialects != null && dialects.isArray()) {
        for (JsonNode dialect : dialects) {
          JsonNode expr = dialect.get("expression");
          if (expr != null && expr.isTextual()) {
            int bytes = Utf8.encodedLength(expr.asText());
            if (bytes > maxExpressionBytes) {
              throw new BadRequestException(
                  "An expression of %d bytes exceeds the maximum of %d bytes",
                  bytes, maxExpressionBytes);
            }
          }
        }
      }
      node.forEach(child -> enforceExpressionCap(child, maxExpressionBytes));
    } else if (node.isArray()) {
      node.forEach(child -> enforceExpressionCap(child, maxExpressionBytes));
    }
  }
}
