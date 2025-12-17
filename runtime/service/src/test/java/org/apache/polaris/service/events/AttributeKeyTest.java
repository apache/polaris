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

import org.junit.jupiter.api.Test;

class AttributeKeyTest {

  @Test
  void testCreateAndAccessors() {
    AttributeKey<String> key = AttributeKey.of("test_key", String.class);

    assertThat(key.name()).isEqualTo("test_key");
    assertThat(key.type()).isEqualTo(String.class);
  }

  @Test
  void testCastValidValue() {
    AttributeKey<String> key = AttributeKey.of("test_key", String.class);

    String result = key.cast("hello");

    assertThat(result).isEqualTo("hello");
  }

  @Test
  void testCastNullValue() {
    AttributeKey<String> key = AttributeKey.of("test_key", String.class);

    String result = key.cast(null);

    assertThat(result).isNull();
  }

  @Test
  void testCastInvalidType() {
    AttributeKey<String> key = AttributeKey.of("test_key", String.class);

    assertThatThrownBy(() -> key.cast(123))
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
}
