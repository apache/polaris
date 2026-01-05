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
import org.junit.jupiter.api.Test;

class AttributeKeyTest {

  @Test
  void testCreateAndAccessors() {
    AttributeKey<String> key = AttributeKey.of("test_key", String.class);

    assertThat(key.name()).isEqualTo("test_key");
    assertThat(key.type()).isEqualTo(String.class);
  }

  @Test
  void testTypeCastValidValue() {
    AttributeKey<String> key = AttributeKey.of("test_key", String.class);

    String result = key.type().cast("hello");

    assertThat(result).isEqualTo("hello");
  }

  @Test
  void testTypeCastNullValue() {
    AttributeKey<String> key = AttributeKey.of("test_key", String.class);

    String result = key.type().cast(null);

    assertThat(result).isNull();
  }

  @Test
  void testTypeCastInvalidType() {
    AttributeKey<String> key = AttributeKey.of("test_key", String.class);

    assertThatThrownBy(() -> key.type().cast(123))
        .isInstanceOf(ClassCastException.class)
        .hasMessageContaining("Integer")
        .hasMessageContaining("String");
  }

  @Test
  void testEqualsAndHashCode() {
    AttributeKey<String> key1 = AttributeKey.of("test_key", String.class);
    AttributeKey<String> key2 = AttributeKey.of("test_key", String.class);
    AttributeKey<String> key3 = AttributeKey.of("other_key", String.class);
    AttributeKey<Integer> key4 = AttributeKey.of("test_key", Integer.class);

    assertThat(key1).isEqualTo(key2);
    assertThat(key1.hashCode()).isEqualTo(key2.hashCode());
    assertThat(key1).isNotEqualTo(key3);
    assertThat(key1).isNotEqualTo(key4);
  }

  @Test
  void testToString() {
    AttributeKey<String> key = AttributeKey.of("test_key", String.class);

    assertThat(key.toString()).contains("test_key").contains("String");
  }

  @Test
  void testNullNameThrows() {
    assertThatThrownBy(() -> AttributeKey.of(null, String.class))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void testNullTypeThrows() {
    assertThatThrownBy(() -> AttributeKey.of("test_key", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void testJsonSerializesToName() throws JsonProcessingException {
    AttributeKey<String> key = AttributeKey.of("catalog_name", String.class);
    ObjectMapper mapper = new ObjectMapper();

    String json = mapper.writeValueAsString(key);

    assertThat(json).isEqualTo("\"catalog_name\"");
  }

  @Test
  void testAllowedTypes() {
    assertThat(AttributeKey.of("s", String.class)).isNotNull();
    assertThat(AttributeKey.of("b", Boolean.class)).isNotNull();
    assertThat(AttributeKey.of("i", Integer.class)).isNotNull();
    assertThat(AttributeKey.of("l", Long.class)).isNotNull();
    assertThat(AttributeKey.of("d", Double.class)).isNotNull();
    assertThat(AttributeKey.of("list", java.util.List.class)).isNotNull();
    assertThat(AttributeKey.of("map", java.util.Map.class)).isNotNull();
    assertThat(AttributeKey.of("set", java.util.Set.class)).isNotNull();
  }

  @Test
  void testDisallowedTypes() {
    assertThatThrownBy(() -> AttributeKey.of("x", java.util.Optional.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not allowed");
    assertThatThrownBy(() -> AttributeKey.of("x", java.util.function.Function.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not allowed");
    assertThatThrownBy(() -> AttributeKey.of("x", Throwable.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not allowed");
    assertThatThrownBy(() -> AttributeKey.of("x", StringBuilder.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not allowed");
  }
}
