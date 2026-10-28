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
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.ValidationMessage;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.service.catalog.semanticmodel.types.SemanticModelDocument;

/** Validates Ossie semantic-model documents against the bundled JSON Schema for their version. */
final class DefaultSemanticDocumentValidator implements SemanticDocumentValidator {

  static final DefaultSemanticDocumentValidator INSTANCE = new DefaultSemanticDocumentValidator();

  private DefaultSemanticDocumentValidator() {}

  @Override
  public void validate(SemanticModelDocument document, JsonNode semanticModel) {
    if (!semanticModel.isObject()) {
      throw new BadRequestException("Field 'semantic_model' must be a JSON object");
    }

    OssieSpecVersion ossieVersion = OssieSpecVersion.fromDocumentVersion(document.getVersion());
    JsonNode documentForSchema = documentBodyForSchemaValidation(document, semanticModel);
    JsonSchema schema = OssieJsonSchemas.forVersion(ossieVersion);
    Set<ValidationMessage> violations = schema.validate(documentForSchema);
    if (!violations.isEmpty()) {
      String details =
          violations.stream()
              .map(DefaultSemanticDocumentValidator::toClientMessage)
              .distinct()
              .sorted()
              .collect(Collectors.joining("; "));
      throw new BadRequestException(
          "Semantic model document does not conform to Apache Ossie schema: %s", details);
    }
  }

  /**
   * Returns the node to validate against the schema. The Ossie schema requires a root {@code
   * version}, which clients usually carry only on the document wrapper, so it is added to a shallow
   * copy when missing.
   */
  private static JsonNode documentBodyForSchemaValidation(
      SemanticModelDocument document, JsonNode semanticModel) {
    JsonNode embeddedVersion = semanticModel.get("version");
    if (embeddedVersion == null || embeddedVersion.isNull()) {
      ObjectNode forValidation = JsonNodeFactory.instance.objectNode();
      forValidation.setAll((ObjectNode) semanticModel);
      forValidation.put("version", document.getVersion());
      return forValidation;
    }
    if (!embeddedVersion.isTextual()) {
      throw new BadRequestException("Field 'semantic_model.version' must be a string when present");
    }
    String embedded = embeddedVersion.asText();
    if (!embedded.equals(document.getVersion())) {
      throw new BadRequestException(
          "Field 'semantic_model.version' (%s) must match document 'version' (%s)",
          embedded, document.getVersion());
    }
    return semanticModel;
  }

  /**
   * Formats a schema violation with a JSON Pointer rooted at {@code semantic_model}, matching the
   * pointers used for source-resolution errors.
   */
  private static String toClientMessage(ValidationMessage message) {
    return "/semantic_model" + message.getInstanceLocation() + ": " + message.getError();
  }
}
