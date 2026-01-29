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

class AttributeMapTest {

  private static final AttributeKey<String> STRING_KEY =
      new AttributeKey<>("string_key", String.class);
  private static final AttributeKey<Integer> INT_KEY = new AttributeKey<>("int_key", Integer.class);

  @Test
  void testPutAndGet() {
    EventAttributeMap map = new EventAttributeMap();
    map.put(STRING_KEY, "value");

    assertThat(map.get(STRING_KEY)).hasValue("value");
  }

  @Test
  void testGetReturnsEmptyForMissingKey() {
    EventAttributeMap map = new EventAttributeMap();

    assertThat(map.get(STRING_KEY)).isEmpty();
  }

  @Test
  void testGetRequired() {
    EventAttributeMap map = new EventAttributeMap();
    map.put(STRING_KEY, "value");

    assertThat(map.getRequired(STRING_KEY)).isEqualTo("value");
  }

  @Test
  void testGetRequiredThrowsForMissingKey() {
    EventAttributeMap map = new EventAttributeMap();

    assertThatThrownBy(() -> map.getRequired(STRING_KEY))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Required attribute string_key not found");
  }

  @Test
  void testContains() {
    EventAttributeMap map = new EventAttributeMap();
    map.put(STRING_KEY, "value");

    assertThat(map.contains(STRING_KEY)).isTrue();
    assertThat(map.contains(INT_KEY)).isFalse();
  }

  @Test
  void testNullValueIsIgnored() {
    EventAttributeMap map = new EventAttributeMap();
    map.put(STRING_KEY, null);

    assertThat(map.contains(STRING_KEY)).isFalse();
    assertThat(map.isEmpty()).isTrue();
  }

  @Test
  void testSize() {
    EventAttributeMap map = new EventAttributeMap();
    assertThat(map.size()).isEqualTo(0);

    map.put(STRING_KEY, "value");
    assertThat(map.size()).isEqualTo(1);

    map.put(INT_KEY, 42);
    assertThat(map.size()).isEqualTo(2);
  }

  @Test
  void testIsEmpty() {
    EventAttributeMap map = new EventAttributeMap();
    assertThat(map.isEmpty()).isTrue();

    map.put(STRING_KEY, "value");
    assertThat(map.isEmpty()).isFalse();
  }

  @Test
  void testCopyConstructor() {
    EventAttributeMap original = new EventAttributeMap();
    original.put(STRING_KEY, "value");

    EventAttributeMap copy = new EventAttributeMap(original);

    assertThat(copy.getRequired(STRING_KEY)).isEqualTo("value");

    // Verify independence
    original.put(INT_KEY, 42);
    assertThat(copy.contains(INT_KEY)).isFalse();
  }

  @Test
  void testPutReturnsThis() {
    EventAttributeMap map = new EventAttributeMap();

    EventAttributeMap result = map.put(STRING_KEY, "value");

    assertThat(result).isSameAs(map);
  }
}
