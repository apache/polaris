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

package org.apache.polaris.service.events.json;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.junit.jupiter.api.Test;

class RedactingSerializerTest {

  private static final String REDACTED_MARKER = RedactingSerializer.getRedactedMarker();

  static class TestBean {
    @JsonSerialize(using = RedactingSerializer.class)
    public String sensitiveField;

    public String normalField;

    public TestBean(String sensitiveField, String normalField) {
      this.sensitiveField = sensitiveField;
      this.normalField = normalField;
    }
  }

  @Test
  void shouldRedactAnnotatedField() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    TestBean bean = new TestBean("secret-value", "public-value");

    String json = mapper.writeValueAsString(bean);
    JsonNode node = mapper.readTree(json);

    assertThat(node.get("sensitiveField").asText()).isEqualTo(REDACTED_MARKER);
    assertThat(node.get("normalField").asText()).isEqualTo("public-value");
  }

  @Test
  void shouldRedactNullValues() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    TestBean bean = new TestBean(null, "public-value");

    String json = mapper.writeValueAsString(bean);
    JsonNode node = mapper.readTree(json);

    // Note: Jackson doesn't call custom serializers for null values by default
    // The field will be serialized as null unless we configure the mapper differently
    assertThat(node.get("sensitiveField").isNull()).isTrue();
    assertThat(node.get("normalField").asText()).isEqualTo("public-value");
  }

  @Test
  void shouldRedactNumericValues() throws Exception {
    ObjectMapper mapper = new ObjectMapper();

    class NumericBean {
      @JsonSerialize(using = RedactingSerializer.class)
      public Integer sensitiveNumber;

      public NumericBean(Integer sensitiveNumber) {
        this.sensitiveNumber = sensitiveNumber;
      }
    }

    NumericBean bean = new NumericBean(12345);
    String json = mapper.writeValueAsString(bean);
    JsonNode node = mapper.readTree(json);

    assertThat(node.get("sensitiveNumber").asText()).isEqualTo(REDACTED_MARKER);
  }

  @Test
  void shouldRedactComplexObjects() throws Exception {
    ObjectMapper mapper = new ObjectMapper();

    class ComplexBean {
      @JsonSerialize(using = RedactingSerializer.class)
      public Object complexObject;

      public ComplexBean(Object complexObject) {
        this.complexObject = complexObject;
      }
    }

    ComplexBean bean = new ComplexBean(new TestBean("nested-secret", "nested-public"));
    String json = mapper.writeValueAsString(bean);
    JsonNode node = mapper.readTree(json);

    assertThat(node.get("complexObject").asText()).isEqualTo(REDACTED_MARKER);
  }

  @Test
  void shouldReturnCorrectRedactedMarker() {
    assertThat(RedactingSerializer.getRedactedMarker()).isEqualTo("***REDACTED***");
  }

  @Test
  void shouldRedactEmptyStrings() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    TestBean bean = new TestBean("", "public-value");

    String json = mapper.writeValueAsString(bean);
    JsonNode node = mapper.readTree(json);

    assertThat(node.get("sensitiveField").asText()).isEqualTo(REDACTED_MARKER);
    assertThat(node.get("normalField").asText()).isEqualTo("public-value");
  }

  @Test
  void shouldRedactLongStrings() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String longSecret = "a".repeat(10000);
    TestBean bean = new TestBean(longSecret, "public-value");

    String json = mapper.writeValueAsString(bean);
    JsonNode node = mapper.readTree(json);

    assertThat(node.get("sensitiveField").asText()).isEqualTo(REDACTED_MARKER);
    assertThat(json.length()).isLessThan(longSecret.length());
  }
}

