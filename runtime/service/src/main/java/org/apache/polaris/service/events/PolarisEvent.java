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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Represents an event emitted by Polaris. Events have a type, metadata, and a map of typed
 * attributes. Use {@link #builder(PolarisEventType, PolarisEventMetadata)} to create instances.
 */
public record PolarisEvent(
    PolarisEventType type, PolarisEventMetadata metadata, Map<AttributeKey<?>, Object> attributes) {

  public PolarisEvent {
    attributes = Collections.unmodifiableMap(new HashMap<>(attributes));
  }

  public <T> Optional<T> attribute(AttributeKey<T> key) {
    Object value = attributes.get(key);
    if (value == null) {
      return Optional.empty();
    }
    return Optional.of(key.cast(value));
  }

  public boolean hasAttribute(AttributeKey<?> key) {
    return attributes.containsKey(key);
  }

  public static Builder builder(PolarisEventType type, PolarisEventMetadata metadata) {
    return new Builder(type, metadata);
  }

  public static final class Builder {
    private final PolarisEventType type;
    private final PolarisEventMetadata metadata;
    private final HashMap<AttributeKey<?>, Object> attributes = new HashMap<>();

    private Builder(PolarisEventType type, PolarisEventMetadata metadata) {
      this.type = type;
      this.metadata = metadata;
    }

    public <T> Builder attribute(AttributeKey<T> key, T value) {
      if (value != null) {
        attributes.put(key, value);
      }
      return this;
    }

    public PolarisEvent build() {
      return new PolarisEvent(type, metadata, Map.copyOf(attributes));
    }
  }
}
