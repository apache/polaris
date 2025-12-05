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
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.util.Set;
import org.junit.jupiter.api.Test;

class FieldRedactionSerializerModifierTest {

  private static final String REDACTED_MARKER = RedactingSerializer.getRedactedMarker();

  static class TestBean {
    public String password;
    public String username;
    public String apiKey;
    public String publicData;

    public TestBean(String password, String username, String apiKey, String publicData) {
      this.password = password;
      this.username = username;
      this.apiKey = apiKey;
      this.publicData = publicData;
    }
  }

  @Test
  void shouldRedactExactFieldNames() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.setSerializerModifier(
        new FieldRedactionSerializerModifier(Set.of("password", "apiKey")));
    mapper.registerModule(module);

    TestBean bean = new TestBean("secret123", "john_doe", "key-12345", "public info");
    String json = mapper.writeValueAsString(bean);
    JsonNode node = mapper.readTree(json);

    assertThat(node.get("password").asText()).isEqualTo(REDACTED_MARKER);
    assertThat(node.get("apiKey").asText()).isEqualTo(REDACTED_MARKER);
    assertThat(node.get("username").asText()).isEqualTo("john_doe");
    assertThat(node.get("publicData").asText()).isEqualTo("public info");
  }

  @Test
  void shouldRedactFieldsWithWildcardPrefix() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.setSerializerModifier(new FieldRedactionSerializerModifier(Set.of("*Key")));
    mapper.registerModule(module);

    TestBean bean = new TestBean("secret123", "john_doe", "key-12345", "public info");
    String json = mapper.writeValueAsString(bean);
    JsonNode node = mapper.readTree(json);

    assertThat(node.get("apiKey").asText()).isEqualTo(REDACTED_MARKER);
    assertThat(node.get("password").asText()).isEqualTo("secret123");
    assertThat(node.get("username").asText()).isEqualTo("john_doe");
    assertThat(node.get("publicData").asText()).isEqualTo("public info");
  }

  @Test
  void shouldRedactFieldsWithWildcardSuffix() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.setSerializerModifier(new FieldRedactionSerializerModifier(Set.of("api*")));
    mapper.registerModule(module);

    TestBean bean = new TestBean("secret123", "john_doe", "key-12345", "public info");
    String json = mapper.writeValueAsString(bean);
    JsonNode node = mapper.readTree(json);

    assertThat(node.get("apiKey").asText()).isEqualTo(REDACTED_MARKER);
    assertThat(node.get("password").asText()).isEqualTo("secret123");
    assertThat(node.get("username").asText()).isEqualTo("john_doe");
    assertThat(node.get("publicData").asText()).isEqualTo("public info");
  }

  @Test
  void shouldRedactFieldsWithMultiplePatterns() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.setSerializerModifier(
        new FieldRedactionSerializerModifier(Set.of("*word", "api*", "username")));
    mapper.registerModule(module);

    TestBean bean = new TestBean("secret123", "john_doe", "key-12345", "public info");
    String json = mapper.writeValueAsString(bean);
    JsonNode node = mapper.readTree(json);

    assertThat(node.get("password").asText()).isEqualTo(REDACTED_MARKER); // matches *word
    assertThat(node.get("apiKey").asText()).isEqualTo(REDACTED_MARKER); // matches api*
    assertThat(node.get("username").asText()).isEqualTo(REDACTED_MARKER); // exact match
    assertThat(node.get("publicData").asText()).isEqualTo("public info");
  }

  @Test
  void shouldNotRedactWhenNoPatterns() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.setSerializerModifier(new FieldRedactionSerializerModifier(Set.of()));
    mapper.registerModule(module);

    TestBean bean = new TestBean("secret123", "john_doe", "key-12345", "public info");
    String json = mapper.writeValueAsString(bean);
    JsonNode node = mapper.readTree(json);

    assertThat(node.get("password").asText()).isEqualTo("secret123");
    assertThat(node.get("apiKey").asText()).isEqualTo("key-12345");
    assertThat(node.get("username").asText()).isEqualTo("john_doe");
    assertThat(node.get("publicData").asText()).isEqualTo("public info");
  }

  @Test
  void shouldHandleWildcardInMiddle() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.setSerializerModifier(new FieldRedactionSerializerModifier(Set.of("*Data")));
    mapper.registerModule(module);

    TestBean bean = new TestBean("secret123", "john_doe", "key-12345", "public info");
    String json = mapper.writeValueAsString(bean);
    JsonNode node = mapper.readTree(json);

    assertThat(node.get("publicData").asText()).isEqualTo(REDACTED_MARKER);
    assertThat(node.get("password").asText()).isEqualTo("secret123");
    assertThat(node.get("apiKey").asText()).isEqualTo("key-12345");
    assertThat(node.get("username").asText()).isEqualTo("john_doe");
  }
}

