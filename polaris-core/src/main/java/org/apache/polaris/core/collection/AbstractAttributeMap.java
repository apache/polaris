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

import com.google.common.collect.Iterators;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/** Base implementation of {@link AttributeMap} backed by a {@link HashMap}. */
public abstract sealed class AbstractAttributeMap implements AttributeMap
    permits MutableAttributeMap, ImmutableAttributeMap {

  protected abstract static class Builder<M extends AttributeMap, B extends Builder<M, B>> {

    protected final MutableAttributeMap map = new MutableAttributeMap();

    /** Inserts the given {@link Attribute} in the map. */
    public <T> B put(Attribute<T> entry) {
      return put(entry.key(), entry.value());
    }

    /** Associates {@code key} with {@code value}, replacing any existing mapping. */
    public <T> B put(AttributeKey<T> key, @Nullable T value) {
      map.put(key, value);
      return self();
    }

    /** Puts all attributes from the given map. */
    public B putAll(AttributeMap other) {
      map.putAll(other);
      return self();
    }

    protected abstract B self();

    /** Builds and returns the attribute map. */
    public abstract M build();
  }

  private final Map<AttributeKey<?>, Object> delegate = new HashMap<>();

  protected AbstractAttributeMap() {}

  protected AbstractAttributeMap(AttributeMap toCopy) {
    toCopy.entrySet().forEach(entry -> delegate.put(entry.key(), entry.value()));
  }

  protected final Map<AttributeKey<?>, Object> delegate() {
    return delegate;
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  @Nullable
  public <T> T get(AttributeKey<T> key) {
    return cast(delegate.get(key));
  }

  @Override
  public @Nullable <V> V getOrDefault(AttributeKey<V> key, @Nullable V defaultValue) {
    return cast(delegate.getOrDefault(key, defaultValue));
  }

  @Override
  public <T> T put(AttributeKey<T> key, @Nullable T value) {
    return cast(delegate.put(key, value));
  }

  @Nullable
  @Override
  public <T> T remove(AttributeKey<T> key) {
    return cast(delegate.remove(key));
  }

  @Override
  public void putAll(AttributeMap map) {
    map.entrySet().forEach(entry -> delegate.put(entry.key(), entry.value()));
  }

  @Override
  public boolean containsKey(AttributeKey<?> key) {
    return delegate.containsKey(key);
  }

  @Override
  public boolean containsValue(@Nullable Object value) {
    return delegate.containsValue(value);
  }

  @Override
  public void clear() {
    delegate.clear();
  }

  @Override
  @NonNull
  public Set<AttributeKey<?>> keySet() {
    return delegate.keySet();
  }

  @Override
  @NonNull
  public Collection<Object> values() {
    return delegate.values();
  }

  @Override
  @NonNull
  public Set<Attribute<?>> entrySet() {
    return new AttributeSet();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof AbstractAttributeMap that)) {
      return false;
    }
    return Objects.equals(this.delegate, that.delegate);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(delegate);
  }

  @Override
  public String toString() {
    // Do not print attribute values!
    return this.getClass().getSimpleName() + "{keys=" + delegate.keySet() + '}';
  }

  private class AttributeSet extends AbstractSet<Attribute<?>> {

    @Override
    public int size() {
      return delegate.size();
    }

    @Override
    @NonNull
    public Iterator<Attribute<?>> iterator() {
      return Iterators.transform(
          delegate.entrySet().iterator(),
          key -> new Attribute<>(cast(key.getKey()), key.getValue()));
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T cast(Object o) {
    return (T) o;
  }
}
