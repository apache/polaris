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

import java.util.Iterator;
import java.util.List;
import org.apache.polaris.core.collection.AttributeMap.Attribute;
import org.apache.polaris.core.collection.AttributeMap.AttributeKey;
import org.junit.jupiter.api.Test;

class MutableAttributeMapTest extends AbstractAttributeMapTest<MutableAttributeMap> {

  @Override
  protected MutableAttributeMap.Builder emptyBuilder() {
    return MutableAttributeMap.builder();
  }

  @Test
  void copyConstructor() {
    AttributeMap original = MutableAttributeMap.builder().put(KEY1, "1").build();
    AttributeMap copy = new ImmutableAttributeMap(original);
    assertThat(copy).isEqualTo(original);
    original.put(KEY2, 42);
    assertThat(copy.containsKey(KEY2)).isFalse();
  }

  @Test
  void putKeyValue() {
    MutableAttributeMap map = new MutableAttributeMap();
    String old1 = map.put(KEY1, "hello");
    assertThat(old1).isNull();
    String old2 = map.put(KEY1, "world");
    assertThat(old2).isEqualTo("hello");
    assertThat(map.get(KEY1)).isEqualTo("world");
  }

  @Test
  void putEntry() {
    MutableAttributeMap map = new MutableAttributeMap();
    String old1 = map.put(new Attribute<>(KEY1, "hello"));
    assertThat(old1).isNull();
    String old2 = map.put(new Attribute<>(KEY1, "world"));
    assertThat(old2).isEqualTo("hello");
    assertThat(map.get(KEY1)).isEqualTo("world");
  }

  @Test
  void remove() {
    MutableAttributeMap map = threeEntryMap();
    String removed = map.remove(KEY1);
    assertThat(removed).isEqualTo("1");
    assertThat(map.containsKey(KEY1)).isFalse();
    assertThat(map.size()).isEqualTo(2);
  }

  @Test
  void clear() {
    MutableAttributeMap map = threeEntryMap();
    map.clear();
    assertThat(map.size()).isZero();
    assertThat(map.isEmpty()).isTrue();
  }

  @Test
  @Override
  void keySet() {
    super.keySet();

    MutableAttributeMap map = threeEntryMap();
    assertThat(map.keySet().remove(KEY1)).isTrue();
    assertThat(map.keySet().remove(KEY2)).isTrue();
    assertThat(map.keySet().remove(KEY3)).isTrue();
    assertThat(map.keySet().remove(NONEXISTENT_KEY)).isFalse();
    assertThat(map.size()).isEqualTo(0);

    map = threeEntryMap();
    Iterator<AttributeKey<?>> iterator = map.keySet().iterator();
    for (int i = 0; i < 3; i++) {
      AttributeKey<?> key = iterator.next();
      assertThat(map.containsKey(key)).isTrue();
      iterator.remove();
      assertThat(map.containsKey(key)).isFalse();
    }
    assertThat(map.size()).isEqualTo(0);

    map = threeEntryMap();
    map.keySet().clear();
    assertThat(map.size()).isZero();
  }

  @Test
  @Override
  void values() {
    super.values();
    MutableAttributeMap map = threeEntryMap();
    assertThat(map.values().remove("1")).isTrue();
    assertThat(map.values().remove(null)).isTrue();
    assertThat(map.values().remove(List.of(true, false))).isTrue();
    assertThat(map.values().remove("x")).isFalse();
    assertThat(map.size()).isEqualTo(0);

    map = threeEntryMap();
    Iterator<Object> iterator = map.values().iterator();
    for (int i = 0; i < 3; i++) {
      Object value = iterator.next();
      assertThat(map.containsValue(value)).isTrue();
      iterator.remove();
      assertThat(map.containsValue(value)).isFalse();
    }
    assertThat(map.size()).isEqualTo(0);

    map = threeEntryMap();
    map.values().clear();
    assertThat(map.size()).isZero();
  }

  @Test
  @Override
  void entrySet() {
    super.entrySet();

    MutableAttributeMap map = threeEntryMap();
    assertThat(map.entrySet().remove(new Attribute<>(KEY1, "1"))).isTrue();
    assertThat(map.entrySet().remove(new Attribute<>(KEY2, null))).isTrue();
    assertThat(map.entrySet().remove(new Attribute<>(KEY3, List.of(true, false)))).isTrue();
    assertThat(map.entrySet().remove(new Attribute<>(KEY1, "x"))).isFalse();
    assertThat(map.entrySet().remove(new Attribute<>(NONEXISTENT_KEY, "x"))).isFalse();
    assertThat(map.size()).isEqualTo(0);

    map = threeEntryMap();
    Iterator<Attribute<?>> iterator = map.entrySet().iterator();
    for (int i = 0; i < 3; i++) {
      Attribute<?> attribute = iterator.next();
      assertThat(map.containsKey(attribute.key())).isTrue();
      assertThat(map.containsValue(attribute.value())).isTrue();
      iterator.remove();
      assertThat(map.containsKey(attribute.key())).isFalse();
      assertThat(map.containsValue(attribute.value())).isFalse();
    }
    assertThat(map.size()).isEqualTo(0);

    map = threeEntryMap();
    map.entrySet().clear();
    assertThat(map.size()).isZero();
  }
}
