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

  private TagValidation() {}

  /**
   * Validates a tag's allowed values: the list must be present and non-empty, no member may be
   * empty, and members must be distinct. Values are compared exactly, so case and surrounding
   * whitespace are significant.
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
