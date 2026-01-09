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
package org.apache.polaris.service.context;

import jakarta.enterprise.context.RequestScoped;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Request-scoped holder for arbitrary event attributes that can be populated throughout the request
 * lifecycle and later used in event generation.
 *
 * <p>This allows storing contextual information like entity IDs, operation metadata, or any other
 * key-value pairs that should be available across the call stack.
 */
@RequestScoped
public class EventAttributesHolder {

  private final Map<String, Object> attributes = new ConcurrentHashMap<>();

  /**
   * Sets an attribute value. If the key already exists, it will be overwritten.
   *
   * @param key the attribute key
   * @param value the attribute value (can be null)
   */
  public void set(String key, Object value) {
    attributes.put(key, value);
  }

  /**
   * Gets an attribute value.
   *
   * @param key the attribute key
   * @return Optional containing the value if present, empty otherwise
   */
  public Optional<Object> get(String key) {
    return Optional.ofNullable(attributes.get(key));
  }

  /**
   * Gets an attribute value as a specific type.
   *
   * @param key the attribute key
   * @param type the expected type
   * @return Optional containing the typed value if present and type matches, empty otherwise
   */
  @SuppressWarnings("unchecked")
  public <T> Optional<T> getAs(String key, Class<T> type) {
    Object value = attributes.get(key);
    if (value != null && type.isInstance(value)) {
      return Optional.of((T) value);
    }
    return Optional.empty();
  }

  /**
   * Gets a String attribute value.
   *
   * @param key the attribute key
   * @return Optional containing the String value if present, empty otherwise
   */
  public Optional<String> getString(String key) {
    return getAs(key, String.class);
  }

  /**
   * Gets a Long attribute value.
   *
   * @param key the attribute key
   * @return Optional containing the Long value if present, empty otherwise
   */
  public Optional<Long> getLong(String key) {
    return getAs(key, Long.class);
  }

  /**
   * Checks if an attribute exists.
   *
   * @param key the attribute key
   * @return true if the attribute exists, false otherwise
   */
  public boolean contains(String key) {
    return attributes.containsKey(key);
  }

  /**
   * Removes an attribute.
   *
   * @param key the attribute key
   * @return Optional containing the removed value if it existed, empty otherwise
   */
  public Optional<Object> remove(String key) {
    return Optional.ofNullable(attributes.remove(key));
  }

  /**
   * Gets all attributes as an immutable map.
   *
   * @return immutable map of all attributes
   */
  public Map<String, Object> getAll() {
    return Collections.unmodifiableMap(new HashMap<>(attributes));
  }

  /**
   * Gets all attributes as a map of String keys to String values. Non-string values will be
   * converted using toString().
   *
   * @return map of string key-value pairs
   */
  public Map<String, String> getAllAsStrings() {
    Map<String, String> result = new HashMap<>();
    attributes.forEach(
        (key, value) -> {
          if (value != null) {
            result.put(key, value.toString());
          }
        });
    return result;
  }

  /**
   * Clears all attributes.
   */
  public void clear() {
    attributes.clear();
  }

  /**
   * Gets the number of attributes.
   *
   * @return the number of attributes
   */
  public int size() {
    return attributes.size();
  }
}
