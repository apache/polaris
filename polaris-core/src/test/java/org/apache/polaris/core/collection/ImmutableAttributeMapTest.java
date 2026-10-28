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

package org.apache.polaris.core.collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.polaris.core.collection.AttributeMap.Attribute;
import org.junit.jupiter.api.Test;

class ImmutableAttributeMapTest extends AbstractAttributeMapTest<ImmutableAttributeMap> {

  @Override
  protected ImmutableAttributeMap.Builder emptyBuilder() {
    return ImmutableAttributeMap.builder();
  }

  @Test
  void copyOf() {
    // from immutable
    AttributeMap original = ImmutableAttributeMap.builder().put(ATTR1).build();
    AttributeMap copy = AttributeMap.copyOf(original);
    assertThat(copy).isSameAs(original);
    // from mutable
    original = MutableAttributeMap.builder().put(ATTR1).build();
    copy = AttributeMap.copyOf(original);
    assertThat(copy).isNotSameAs(original).isEqualTo(original);
    original.put(KEY2, 42);
    assertThat(copy.containsKey(KEY2)).isFalse();
  }

  @Test
  void of() {
    AttributeMap map = AttributeMap.of(ATTR1, ATTR2, ATTR3);
    assertThat(map).isEqualTo(threeEntryMap());
  }

  @Test
  void copyConstructor() {
    AttributeMap original = MutableAttributeMap.builder().put(ATTR1).build();
    AttributeMap copy = new ImmutableAttributeMap(original);
    assertThat(copy).isEqualTo(original);
    original.put(KEY2, 42);
    assertThat(copy.containsKey(KEY2)).isFalse();
  }

  @Test
  void mutationMethodsThrow() {
    ImmutableAttributeMap map = threeEntryMap();
    assertThatThrownBy(() -> map.put(KEY1, "x")).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> map.put(new Attribute<>(KEY1, "x")))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> map.putAll(AttributeMap.EMPTY))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> map.remove(KEY1)).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(map::clear).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  @Override
  void keySet() {
    super.keySet();
    ImmutableAttributeMap map = threeEntryMap();
    assertThatThrownBy(() -> map.keySet().remove(KEY1))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> map.keySet().iterator().remove())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> map.keySet().clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  @Override
  void values() {
    super.values();
    ImmutableAttributeMap map = threeEntryMap();
    assertThatThrownBy(() -> map.values().remove(VALUE1))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> map.values().iterator().remove())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> map.values().clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  @Override
  @SuppressWarnings("DataFlowIssue")
  void entrySet() {
    super.entrySet();
    ImmutableAttributeMap map = threeEntryMap();
    assertThatThrownBy(() -> map.entrySet().remove(ATTR1))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> map.entrySet().iterator().remove())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> map.entrySet().clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
