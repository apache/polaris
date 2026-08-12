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
import java.util.Collections;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/**
 * An immutable, type-safe {@link AttributeMap}. Immutable attribute maps are thread-safe and can be
 * used concurrently.
 */
public final class ImmutableAttributeMap extends AbstractAttributeMap implements AttributeMap {

  /** Fluent builder for {@link ImmutableAttributeMap}. */
  public static class Builder extends AbstractAttributeMap.Builder<ImmutableAttributeMap, Builder> {

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    public ImmutableAttributeMap build() {
      return map.toImmutableAttributeMap();
    }
  }

  /** Returns a new, empty {@link Builder}. */
  public static ImmutableAttributeMap.Builder builder() {
    return new ImmutableAttributeMap.Builder();
  }

  /** Creates an empty immutable map. */
  public ImmutableAttributeMap() {
    super();
  }

  /** Copy constructor. */
  public ImmutableAttributeMap(AttributeMap toCopy) {
    super(toCopy);
  }

  @Override
  public <T> T put(AttributeKey<T> key, @Nullable T value) {
    throw new UnsupportedOperationException("this attribute map is immutable");
  }

  @Override
  public void putAll(AttributeMap map) {
    throw new UnsupportedOperationException("this attribute map is immutable");
  }

  @Nullable
  @Override
  public <T> T remove(AttributeKey<T> key) {
    throw new UnsupportedOperationException("this attribute map is immutable");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("this attribute map is immutable");
  }

  @Override
  public Set<AttributeKey<?>> keySet() {
    return Collections.unmodifiableMap(delegate()).keySet();
  }

  @Override
  public Collection<@Nullable Object> values() {
    return Collections.unmodifiableMap(delegate()).values();
  }

  @Override
  public Set<Attribute<?>> entrySet() {
    return Collections.unmodifiableSet(super.entrySet());
  }
}
