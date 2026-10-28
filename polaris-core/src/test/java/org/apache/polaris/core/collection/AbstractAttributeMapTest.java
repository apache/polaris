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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.polaris.core.collection.AttributeMap.Attribute;
import org.apache.polaris.core.collection.AttributeMap.AttributeKey;
import org.junit.jupiter.api.Test;

/**
 * Shared read-path and builder tests executed against both {@link MutableAttributeMap} and {@link
 * ImmutableAttributeMap}.
 */
abstract class AbstractAttributeMapTest<T extends AttributeMap> {

  static final AttributeKey<String> KEY1 = new AttributeKey<>("attr1");
  static final AttributeKey<Integer> KEY2 = new AttributeKey<>("attr2");
  static final AttributeKey<List<Boolean>> KEY3 = new AttributeKey<>("attr3");

  static final String VALUE1 = "1";
  static final Integer VALUE2 = null;
  static final List<Boolean> VALUE3 = List.of(true, false);

  static final Attribute<String> ATTR1 = new Attribute<>(KEY1, VALUE1);
  static final Attribute<Integer> ATTR2 = new Attribute<>(KEY2, VALUE2);
  static final Attribute<List<Boolean>> ATTR3 = new Attribute<>(KEY3, VALUE3);

  static final AttributeKey<String> NONEXISTENT_KEY = new AttributeKey<>("nonexistent");

  protected abstract AbstractAttributeMap.Builder<T, ?> emptyBuilder();

  protected T emptyMap() {
    return emptyBuilder().build();
  }

  protected T threeEntryMap() {
    return emptyBuilder().put(KEY1, VALUE1).put(KEY2, VALUE2).put(KEY3, VALUE3).build();
  }

  @Test
  void size() {
    assertThat(threeEntryMap().size()).isEqualTo(3);
    assertThat(emptyMap().size()).isEqualTo(0);
  }

  @Test
  void isEmpty() {
    assertThat(threeEntryMap().isEmpty()).isFalse();
    assertThat(emptyMap().isEmpty()).isTrue();
  }

  @Test
  void get() {
    AttributeMap map = threeEntryMap();
    assertThat(map.get(KEY1)).isEqualTo(VALUE1);
    assertThat(map.get(KEY2)).isEqualTo(VALUE2);
    assertThat(map.get(KEY3)).isEqualTo(VALUE3);
    assertThat(map.get(NONEXISTENT_KEY)).isNull();
  }

  @Test
  void getOrDefault() {
    AttributeMap map = threeEntryMap();
    assertThat(map.getOrDefault(KEY1, "default")).isEqualTo(VALUE1);
    assertThat(map.getOrDefault(KEY2, 0)).isEqualTo(VALUE2);
    assertThat(map.getOrDefault(KEY3, List.of(false, true))).isEqualTo(VALUE3);
    assertThat(map.getOrDefault(NONEXISTENT_KEY, "default")).isEqualTo("default");
    assertThat(map.getOrDefault(NONEXISTENT_KEY, null)).isNull();
  }

  @Test
  void getOptional() {
    AttributeMap map = threeEntryMap();
    assertThat(map.getOptional(KEY1)).contains(VALUE1);
    assertThat(map.getOptional(KEY2)).isEmpty();
    assertThat(map.getOptional(KEY3)).contains(VALUE3);
    assertThat(map.getOptional(NONEXISTENT_KEY)).isEmpty();
  }

  @Test
  void getRequired() {
    AttributeMap map = threeEntryMap();
    assertThat(map.getRequired(KEY1)).isEqualTo(VALUE1);
    assertThat(map.getRequired(KEY3)).isEqualTo(VALUE3);
    // mapped to null
    assertThatThrownBy(() -> map.getRequired(KEY2))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("attr2");
    // missing
    assertThatThrownBy(() -> map.getRequired(NONEXISTENT_KEY))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("nonexistent");
  }

  @Test
  void containsKey() {
    AttributeMap map = threeEntryMap();
    assertThat(map.containsKey(KEY1)).isTrue();
    assertThat(map.containsKey(KEY2)).isTrue();
    assertThat(map.containsKey(KEY3)).isTrue();
    assertThat(map.containsKey(NONEXISTENT_KEY)).isFalse();
  }

  @Test
  void containsValue() {
    AttributeMap map = threeEntryMap();
    assertThat(map.containsValue(VALUE1)).isTrue();
    assertThat(map.containsValue(VALUE2)).isTrue();
    assertThat(map.containsValue(VALUE3)).isTrue();
    assertThat(map.containsValue("x")).isFalse();
  }

  @Test
  void keySet() {
    AttributeMap map = threeEntryMap();
    assertThat(map.keySet()).containsExactlyInAnyOrder(KEY1, KEY2, KEY3);
  }

  @Test
  void values() {
    AttributeMap map = threeEntryMap();
    assertThat(map.values()).containsExactlyInAnyOrder(VALUE1, VALUE2, VALUE3);
  }

  @Test
  void entrySet() {
    AttributeMap map = threeEntryMap();
    assertThat(map.entrySet()).containsExactlyInAnyOrder(ATTR1, ATTR2, ATTR3);
  }

  @Test
  void forEach() {
    AttributeMap map = threeEntryMap();
    Set<Attribute<?>> collected = new HashSet<>();
    map.forEach(collected::add);
    assertThat(collected).containsExactlyInAnyOrder(ATTR1, ATTR2, ATTR3);
  }

  @Test
  void builderPutKeyValue() {
    AttributeMap map = emptyBuilder().put(KEY1, VALUE1).put(KEY2, VALUE2).put(KEY3, VALUE3).build();
    assertThat(map.get(KEY1)).isEqualTo(VALUE1);
    assertThat(map.get(KEY2)).isEqualTo(VALUE2);
    assertThat(map.get(KEY3)).isEqualTo(VALUE3);
  }

  @Test
  void builderPutEntry() {
    AttributeMap map = emptyBuilder().put(ATTR1).put(ATTR2).put(ATTR3).build();
    assertThat(map.get(KEY1)).isEqualTo(VALUE1);
    assertThat(map.get(KEY2)).isEqualTo(VALUE2);
    assertThat(map.get(KEY3)).isEqualTo(VALUE3);
  }

  @Test
  void builderPutAll() {
    AttributeMap copy = emptyBuilder().putAll(threeEntryMap()).build();
    assertThat(copy.get(KEY1)).isEqualTo(VALUE1);
    assertThat(copy.get(KEY2)).isEqualTo(VALUE2);
    assertThat(copy.get(KEY3)).isEqualTo(VALUE3);
    assertThat(copy.size()).isEqualTo(3);
  }

  @Test
  void equalsHashCode() {
    AttributeMap a = threeEntryMap();
    AttributeMap b = threeEntryMap();
    assertThat(a).isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
  }

  @Test
  void builderReused() {
    AbstractAttributeMap.Builder<T, ?> builder = emptyBuilder().put(KEY1, VALUE1);
    AttributeMap map1 = builder.build();
    builder.put(KEY2, VALUE2);
    AttributeMap map2 = builder.build();
    assertThat(map1.keySet()).containsExactly(KEY1);
    assertThat(map2.keySet()).containsExactlyInAnyOrder(KEY1, KEY2);
  }

  @Test
  void equalsCrossType() {
    AttributeMap mutable = MutableAttributeMap.builder().put(ATTR1).put(ATTR2).put(ATTR3).build();
    AttributeMap immutable = ImmutableAttributeMap.builder().putAll(mutable).build();
    assertThat(mutable).isEqualTo(immutable);
    assertThat(immutable).isEqualTo(mutable);
    assertThat(mutable).isNotEqualTo(null);
    assertThat(immutable).isNotEqualTo(null);
  }

  @Test
  void hashCodeCrossType() {
    AttributeMap mutable = MutableAttributeMap.builder().put(ATTR1).put(ATTR2).put(ATTR3).build();
    AttributeMap immutable = ImmutableAttributeMap.builder().putAll(mutable).build();
    assertThat(mutable.hashCode()).isEqualTo(immutable.hashCode());
  }

  @Test
  void equalsEmptyMaps() {
    assertThat((AttributeMap) new MutableAttributeMap())
        .isEqualTo(new ImmutableAttributeMap())
        .isEqualTo(AttributeMap.EMPTY);
  }

  @Test
  void notEqualWhenDifferentEntries() {
    AttributeMap a = emptyBuilder().put(KEY1, "x").build();
    AttributeMap b = emptyBuilder().put(KEY1, "y").build();
    AttributeMap c = emptyBuilder().put(KEY2, 1).build();
    assertThat(a).isNotEqualTo(b);
    assertThat(a).isNotEqualTo(c);
    assertThat(a).isNotEqualTo(emptyMap());
  }

  @Test
  void testToString() {
    AttributeMap map = emptyBuilder().put(KEY1, "secret-value").build();
    assertThat(map.toString())
        .contains(map.getClass().getSimpleName())
        .contains("attr1")
        .doesNotContain("secret-value");
    Attribute<?> attribute = map.entrySet().iterator().next();
    assertThat(attribute.toString()).contains("attr1").doesNotContain("secret-value");
  }
}
