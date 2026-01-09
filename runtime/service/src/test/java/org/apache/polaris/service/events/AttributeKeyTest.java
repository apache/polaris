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
package org.apache.polaris.service.events;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.reflect.TypeToken;
import org.junit.jupiter.api.Test;

class AttributeKeyTest {

  private static final String TEST_KEY = "test_key";

  @Test
  void testCreateAndAccessors() {
    AttributeKey<String> key = new AttributeKey<>(TEST_KEY, String.class);

    assertThat(key.name()).isEqualTo(TEST_KEY);
    assertThat(key.type()).isEqualTo(TypeToken.of(String.class));
  }

  @Test
  void testToString() {
    AttributeKey<String> key = new AttributeKey<>(TEST_KEY, String.class);

    assertThat(key.toString()).contains(TEST_KEY).contains("String");
  }

  @Test
  void testNullNameThrows() {
    assertThatThrownBy(() -> new AttributeKey<>(null, String.class))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void testNullTypeThrows() {
    assertThatThrownBy(() -> new AttributeKey<>(TEST_KEY, (Class<String>) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void testJsonSerializesToName() throws JsonProcessingException {
    AttributeKey<String> key = new AttributeKey<>(TEST_KEY, String.class);
    ObjectMapper mapper = new ObjectMapper();

    String json = mapper.writeValueAsString(key);

    assertThat(json).isEqualTo("\"" + TEST_KEY + "\"");
  }

  @Test
  void testAllowedTypes() {
    assertThat(new AttributeKey<>("s", String.class)).isNotNull();
    assertThat(new AttributeKey<>("b", Boolean.class)).isNotNull();
    assertThat(new AttributeKey<>("i", Integer.class)).isNotNull();
    assertThat(new AttributeKey<>("l", Long.class)).isNotNull();
    assertThat(new AttributeKey<>("d", Double.class)).isNotNull();
    assertThat(new AttributeKey<>("list", new TypeToken<java.util.List<String>>() {})).isNotNull();
    assertThat(new AttributeKey<>("map", new TypeToken<java.util.Map<String, String>>() {}))
        .isNotNull();
    assertThat(new AttributeKey<>("set", new TypeToken<java.util.Set<String>>() {})).isNotNull();
  }

  @Test
  void testDisallowedTypes() {
    assertThatThrownBy(() -> new AttributeKey<>("x", java.util.Optional.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not allowed");
    assertThatThrownBy(() -> new AttributeKey<>("x", java.util.function.Function.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not allowed");
    assertThatThrownBy(() -> new AttributeKey<>("x", Throwable.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not allowed");
    assertThatThrownBy(() -> new AttributeKey<>("x", StringBuilder.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not allowed");
  }
}
