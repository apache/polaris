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

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.jspecify.annotations.Nullable;

/**
 * A map-like structure containing typed attributes, keyed by {@link AttributeKey}.
 *
 * <p>Unlike a raw {@code Map<String, Object>}, attribute maps guarantee that, for any attribute
 * key, the corresponding value has the right type, eliminating unchecked casts in callers.
 *
 * <p>Note: this guarantee is only valid as long as callers never create two instances of {@link
 * AttributeKey} having the same string key, but different types.
 *
 * <p>Two implementations are available: {@link MutableAttributeMap} and {@link
 * ImmutableAttributeMap}.
 */
public sealed interface AttributeMap
    permits AbstractAttributeMap, ImmutableAttributeMap, MutableAttributeMap {

  /** A shared empty, immutable instance. */
  ImmutableAttributeMap EMPTY = new ImmutableAttributeMap();

  /** Returns an immutable copy of {@code map}. */
  static ImmutableAttributeMap copyOf(AttributeMap map) {
    return map instanceof ImmutableAttributeMap immutable
        ? immutable
        : new ImmutableAttributeMap(map);
  }

  /** Returns an immutable map containing the given attributes. */
  static ImmutableAttributeMap of(Attribute<?> attribute, Attribute<?>... attributes) {
    ImmutableAttributeMap.Builder builder = ImmutableAttributeMap.builder().put(attribute);
    for (Attribute<?> attr : attributes) {
      builder.put(attr);
    }
    return builder.build();
  }

  /**
   * A typed attribute key.
   *
   * @param <T> the type of the value associated with this key
   * @param key the string identifier for this attribute key.
   */
  @SuppressWarnings({"UnusedTypeParameter", "unused"})
  record AttributeKey<T>(String key) {

    public AttributeKey {
      Objects.requireNonNull(key, "key");
    }

    @Override
    public String toString() {
      return key;
    }
  }

  /**
   * An entry associating an {@link AttributeKey} with a value of its type. Used to iterate over
   * attributes in the map.
   *
   * @see #entrySet()
   * @see #forEach(Consumer)
   * @see #put(Attribute)
   */
  record Attribute<T>(AttributeKey<T> key, @Nullable T value) {

    public Attribute {
      Objects.requireNonNull(key, "key");
    }

    @Override
    public String toString() {
      // Do not print attribute values!
      return "Attribute{key=" + key + "}";
    }
  }

  /** Returns the number of key-value mappings in this map. */
  int size();

  /** Returns {@code true} if this map contains no key-value mappings. */
  boolean isEmpty();

  /** Returns the value associated with {@code key}, or {@code null} if absent. */
  @Nullable <T> T get(AttributeKey<T> key);

  /**
   * Returns the value for {@code key} if present, otherwise {@code defaultValue}.
   *
   * <p>Unlike {@link #get}, this distinguishes between a missing key and a key explicitly mapped to
   * {@code null}.
   */
  @Nullable <V> V getOrDefault(AttributeKey<V> key, @Nullable V defaultValue);

  /**
   * Returns the value associated with {@code key} wrapped in an {@link Optional}, or an empty
   * {@link Optional} if the key is absent.
   */
  default <T> Optional<T> getOptional(AttributeKey<T> key) {
    return Optional.ofNullable(get(key));
  }

  /**
   * Returns the value associated with {@code key}, throwing {@link IllegalStateException} if the
   * key is absent.
   */
  @Nullable
  default <T> T getRequired(AttributeKey<T> key) {
    if (!containsKey(key)) {
      throw new IllegalStateException("Required attribute " + key.key() + " not found");
    }
    return get(key);
  }

  /**
   * Associates the specified value with the specified key in this map. Returns the previous value
   * associated with {@code key}, or {@code null} if there was no mapping for {@code key}.
   */
  @Nullable <T> T put(AttributeKey<T> key, @Nullable T value);

  /**
   * Adds the given attribute to this map. Returns the previous value associated with the
   * attribute's key, or {@code null} if there was no mapping for that key. This is just a shortcut
   * for {@code put(attribute.getKey(), attribute.getValue())}.
   */
  @Nullable
  default <T> T put(Attribute<T> attribute) {
    return put(attribute.key(), attribute.value());
  }

  /** Copies all the attributes from the specified map to this map. */
  void putAll(AttributeMap map);

  /**
   * Removes the mapping for the given typed {@code key}, if present. Returns the previous value
   * associated with {@code key}, or {@code null} if there was no mapping for {@code key}.
   */
  @Nullable <T> T remove(AttributeKey<T> key);

  /** Returns {@code true} if this map contains an attribute with the specified {@code key}. */
  boolean containsKey(AttributeKey<?> key);

  /** Returns {@code true} if this map contains an attribute with the specified {@code value} */
  boolean containsValue(@Nullable Object value);

  /**
   * Returns a {@link Set} view of the keys contained in this map.
   *
   * <p>For mutable implementations, the set is backed by the map and supports removals which remove
   * the corresponding mapping from the map. Immutable implementations may return an unmodifiable
   * view that throws {@link UnsupportedOperationException} on mutation attempts.
   */
  Set<AttributeKey<?>> keySet();

  /**
   * Returns a {@link Collection} view of the values contained in this map.
   *
   * <p>For mutable implementations, the collection is backed by the map and supports removals which
   * remove the corresponding mapping from the map. Immutable implementations may return an
   * unmodifiable view that throws {@link UnsupportedOperationException} on mutation attempts.
   */
  Collection<@Nullable Object> values();

  /**
   * Returns a {@link Set} view of the attribute entries contained in this map.
   *
   * <p>For mutable implementations, the set is backed by the map and supports removals which remove
   * the corresponding mapping from the map. Immutable implementations may return an unmodifiable
   * view that throws {@link UnsupportedOperationException} on mutation attempts.
   */
  Set<Attribute<?>> entrySet();

  /** Invokes {@code action} for every attribute in this map. */
  default void forEach(Consumer<Attribute<?>> action) {
    Objects.requireNonNull(action);
    entrySet().forEach(action);
  }

  /** Removes all mappings from this map. The map will be empty after this call returns. */
  void clear();
}
