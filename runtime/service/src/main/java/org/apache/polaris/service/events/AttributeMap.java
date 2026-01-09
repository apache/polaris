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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** A type-safe container for event attributes. This class is mutable and not thread-safe! */
public final class AttributeMap {
  private final Map<AttributeKey<?>, Object> attributes;

  public AttributeMap() {
    this.attributes = new HashMap<>();
  }

  public AttributeMap(AttributeMap other) {
    this.attributes = new HashMap<>(other.attributes);
  }

  @SuppressWarnings("unchecked")
  public <T> Optional<T> get(AttributeKey<T> key) {
    return Optional.ofNullable((T) attributes.get(key));
  }

  public <T> T getRequired(AttributeKey<T> key) {
    return get(key)
        .orElseThrow(
            () -> new IllegalStateException("Required attribute " + key.name() + " not found"));
  }

  public <T> AttributeMap put(AttributeKey<T> key, T value) {
    if (value != null) {
      attributes.put(key, value);
    }
    return this;
  }

  public boolean contains(AttributeKey<?> key) {
    return attributes.containsKey(key);
  }

  public int size() {
    return attributes.size();
  }

  public boolean isEmpty() {
    return attributes.isEmpty();
  }
}
