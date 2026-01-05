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

import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Objects;

/**
 * A type-safe key for event attributes. This allows for strongly-typed attribute access while
 * maintaining flexibility for custom attributes.
 *
 * <p>Attribute types are validated at key creation time to ensure they can be serialized. See
 * {@link AllowedAttributeTypes} for the list of allowed types.
 *
 * @param <T> the type of the attribute value
 */
public final class AttributeKey<T> {
  private final String name;
  private final Class<T> type;

  private AttributeKey(String name, Class<T> type) {
    this.name = Objects.requireNonNull(name, "name");
    this.type = Objects.requireNonNull(type, "type");
    if (!AllowedAttributeTypes.isAllowed(type)) {
      throw new IllegalArgumentException(
          "Type " + type.getName() + " is not allowed for event attributes");
    }
  }

  public static <T> AttributeKey<T> of(String name, Class<T> type) {
    return new AttributeKey<>(name, type);
  }

  @JsonValue
  public String name() {
    return name;
  }

  public Class<T> type() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AttributeKey<?> that)) {
      return false;
    }
    return name.equals(that.name) && type.equals(that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type);
  }

  @Override
  public String toString() {
    return "AttributeKey{" + name + ", " + type.getSimpleName() + "}";
  }
}
