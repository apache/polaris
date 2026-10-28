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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.service.catalog.semanticmodel.types.SemanticModelDocument;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class SemanticDocumentValidatorTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String VALID_BODY =
      "{\"name\":\"m\",\"datasets\":[{\"name\":\"d\",\"source\":\"sales.store_sales\"}]}";

  private final SemanticDocumentValidator validator = DefaultSemanticDocumentValidator.INSTANCE;

  @Test
  void validateAcceptsDocumentWithMatchingEmbeddedVersion() throws Exception {
    String body =
        "{\"version\":\"0.2.0.dev0\",\"name\":\"m\",\"datasets\":[{\"name\":\"d\",\"source\":\"sales.t\"}]}";
    assertThatCode(() -> validator.validate(document(body), MAPPER.readTree(body)))
        .doesNotThrowAnyException();
  }

  @Test
  void validateAcceptsMinimalValidDocument() throws Exception {
    assertThatCode(() -> validator.validate(document(VALID_BODY), MAPPER.readTree(VALID_BODY)))
        .doesNotThrowAnyException();
  }

  @Test
  void validateDoesNotMutateParsedBody() throws Exception {
    JsonNode semanticModel = MAPPER.readTree(VALID_BODY);
    validator.validate(document(VALID_BODY), semanticModel);
    assertThat(semanticModel.has("version")).isFalse();
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "{\"datasets\":[{\"name\":\"d\",\"source\":\"sales.t\"}]}"
            + " | /semantic_model: required property 'name' not found",
        "{\"name\":\"m\",\"datasets\":[{\"name\":\"d\"}]}"
            + " | /semantic_model/datasets/0: required property 'source' not found",
      })
  void validateRejectsSchemaViolationWithPointer(String body, String expectedViolation)
      throws Exception {
    assertThatThrownBy(() -> validator.validate(document(body), MAPPER.readTree(body)))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("does not conform to Apache Ossie schema")
        .hasMessageContaining(expectedViolation);
  }

  @Test
  void validateRejectsUnsupportedSpecVersion() throws Exception {
    SemanticModelDocument document =
        SemanticModelDocument.builder().setVersion("9.9.9").setSemanticModel(VALID_BODY).build();
    assertThatThrownBy(() -> validator.validate(document, MAPPER.readTree(VALID_BODY)))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("Unsupported Apache Ossie spec version");
  }

  @Test
  void validateRejectsMismatchedEmbeddedVersion() throws Exception {
    String bodyWithVersion =
        "{\"version\":\"0.1.0\",\"name\":\"m\",\"datasets\":[{\"name\":\"d\",\"source\":\"s.t\"}]}";
    assertThatThrownBy(
            () -> validator.validate(document(bodyWithVersion), MAPPER.readTree(bodyWithVersion)))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("must match document 'version'");
  }

  @Test
  void validateRejectsNonObjectBody() throws Exception {
    assertThatThrownBy(() -> validator.validate(document("[]"), MAPPER.readTree("[]")))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("must be a JSON object");
  }

  private static SemanticModelDocument document(String semanticModel) {
    return SemanticModelDocument.builder()
        .setVersion("0.2.0.dev0")
        .setSemanticModel(semanticModel)
        .build();
  }
}
