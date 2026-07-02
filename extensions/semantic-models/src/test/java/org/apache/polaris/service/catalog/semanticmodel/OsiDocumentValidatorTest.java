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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.exceptions.BadRequestException;
import org.junit.jupiter.api.Test;

class OsiDocumentValidatorTest {

  private static final int MAX_DOC = 1024 * 1024;
  private static final int MAX_EXPR = 16 * 1024;

  private static final String VALID_MODEL =
      "[{\"name\":\"m\",\"datasets\":[{\"name\":\"d\",\"source\":\"sales.store_sales\"}]}]";

  @Test
  void acceptsValidDocument() {
    var node = OsiDocumentValidator.validate("0.1.1", VALID_MODEL, MAX_DOC, MAX_EXPR);
    assertThat(node.isArray()).isTrue();
    assertThat(node.get(0).get("name").asText()).isEqualTo("m");
  }

  @Test
  void rejectsUnsupportedSpecVersion() {
    assertThatThrownBy(() -> OsiDocumentValidator.validate("0.2.0", VALID_MODEL, MAX_DOC, MAX_EXPR))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("Unsupported OSI spec version");
  }

  @Test
  void rejectsUnknownField() {
    String withUnknown =
        "[{\"name\":\"m\",\"bogus\":1,\"datasets\":[{\"name\":\"d\",\"source\":\"a.b\"}]}]";
    assertThatThrownBy(() -> OsiDocumentValidator.validate("0.1.1", withUnknown, MAX_DOC, MAX_EXPR))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("OSI schema validation");
  }

  @Test
  void rejectsMissingRequiredField() {
    // dataset missing the required 'source'
    String missingSource = "[{\"name\":\"m\",\"datasets\":[{\"name\":\"d\"}]}]";
    assertThatThrownBy(
            () -> OsiDocumentValidator.validate("0.1.1", missingSource, MAX_DOC, MAX_EXPR))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("OSI schema validation");
  }

  @Test
  void rejectsInvalidJson() {
    assertThatThrownBy(() -> OsiDocumentValidator.validate("0.1.1", "{not json", MAX_DOC, MAX_EXPR))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("not valid JSON");
  }

  @Test
  void rejectsOversizeDocument() {
    assertThatThrownBy(() -> OsiDocumentValidator.validate("0.1.1", VALID_MODEL, 10, MAX_EXPR))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("exceeds the maximum");
  }

  @Test
  void rejectsOversizeExpression() {
    String bigExpr = "SUM(" + "x".repeat(100) + ")";
    String model =
        "[{\"name\":\"m\",\"datasets\":[{\"name\":\"d\",\"source\":\"a.b\"}],"
            + "\"metrics\":[{\"name\":\"total\",\"expression\":{\"dialects\":"
            + "[{\"dialect\":\"ANSI_SQL\",\"expression\":\""
            + bigExpr
            + "\"}]}}]}]";
    // Schema-valid, but the expression exceeds a tiny per-expression cap.
    assertThatThrownBy(() -> OsiDocumentValidator.validate("0.1.1", model, MAX_DOC, 10))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("expression");
  }
}
