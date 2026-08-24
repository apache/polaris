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
package org.apache.polaris.core.tag;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.exceptions.BadRequestException;

/**
 * Structural validation for a tag definition's list-valued fields.
 *
 * <p>Unlike policies, whose content is validated against JSON schemas by a pluggable validator
 * framework, a tag definition carries only two lists whose rules fit in one place. An unknown
 * target-type member reaches this class as a null list member: the service's deserializer maps
 * unknown wire values to null, and the null-member rule here rejects them with the documented error
 * type.
 */
public final class TagValidation {

  /**
   * Upper bound on a tag value, in UTF-8 bytes. Values are stored in an indexed column, and index
   * entries carry a per-page size limit on the relational backends; bounding the value keeps every
   * accepted definition value assignable.
   */
  public static final int MAX_VALUE_BYTES = 2000;

  private TagValidation() {}

  /**
   * Rejects a value longer than {@link #MAX_VALUE_BYTES} in UTF-8, naming the limit. Shared by the
   * definition's allowed-values check and the assignment's selected-value check so both sides
   * enforce the same bound.
   */
  public static void validateValueLength(String value, String what) {
    int bytes = value.getBytes(StandardCharsets.UTF_8).length;
    if (bytes > MAX_VALUE_BYTES) {
      throw new BadRequestException(
          "%s must be at most %d bytes in UTF-8, got %d", what, MAX_VALUE_BYTES, bytes);
    }
  }

  /**
   * Validates a tag's allowed values: the list must be present and non-empty, no member may be
   * empty or longer than {@link #MAX_VALUE_BYTES}, and members must be distinct. Values are
   * compared exactly, so case and surrounding whitespace are significant.
   */
  public static void validateAllowedValues(List<String> allowedValues) {
    if (allowedValues == null || allowedValues.isEmpty()) {
      throw new BadRequestException("Allowed values must contain at least one value");
    }
    Set<String> seen = new HashSet<>();
    for (String value : allowedValues) {
      if (value == null || value.isEmpty()) {
        throw new BadRequestException("Allowed values must not contain an empty value");
      }
      validateValueLength(value, "An allowed value");
      if (!seen.add(value)) {
        throw new BadRequestException("Allowed values must not contain duplicates: %s", value);
      }
    }
  }

  /** Validates a tag's target types: the list must be present, non-empty and free of duplicates. */
  public static void validateTargetTypes(List<String> targetTypes) {
    if (targetTypes == null || targetTypes.isEmpty()) {
      throw new BadRequestException("Target types must contain at least one type");
    }
    Set<String> seen = new HashSet<>();
    for (String targetType : targetTypes) {
      if (targetType == null) {
        throw new BadRequestException("Target types must contain only known, non-null values");
      }
      if (!seen.add(targetType)) {
        throw new BadRequestException("Target types must not contain duplicates: %s", targetType);
      }
    }
  }
}
