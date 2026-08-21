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
import java.util.regex.Pattern;
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
   * The longest a single value may be. The limit is measured on the decoded value in UTF-8 bytes,
   * not on characters and not on the request text: a multi-byte character reaches it sooner than
   * its length suggests, and JSON escaping in the body is invisible to it because the value is
   * measured after the body has been read.
   */
  private static final int MAX_VALUE_BYTES = 2000;

  /** The same expression the API declares for a tag name. */
  private static final Pattern NAME_PATTERN = Pattern.compile("^[A-Za-z0-9\\-_]+$");

  private TagValidation() {}

  /**
   * Validates a tag name against the pattern the API declares. The generated model carries the same
   * pattern as a bean-validation constraint, but that constraint is answered by the shared
   * constraint-violation mapper, which reports a generic schema failure. The contract names an
   * invalid tag name as a Tag validation error instead, so the name is checked here, during
   * deserialization, where the Tag-specific answer can still be given.
   */
  public static void validateName(String name) {
    if (name != null && !NAME_PATTERN.matcher(name).matches()) {
      throw new BadRequestException(
          "Tag name must contain only letters, digits, hyphens and underscores: %s", name);
    }
  }

  /**
   * Validates a tag's values: the list must be present and non-empty, no member may be empty, and
   * members must be distinct. Values are compared exactly, so case and surrounding whitespace are
   * significant.
   */
  public static void validateValues(List<String> values) {
    if (values == null || values.isEmpty()) {
      throw new BadRequestException("Values must contain at least one value");
    }
    Set<String> seen = new HashSet<>();
    for (String value : values) {
      if (value == null || value.isEmpty()) {
        throw new BadRequestException("Values must not contain an empty value");
      }
      if (!seen.add(value)) {
        throw new BadRequestException("Values must not contain duplicates: %s", value);
      }
      if (value.getBytes(StandardCharsets.UTF_8).length > MAX_VALUE_BYTES) {
        throw new BadRequestException(
            "Values must not exceed %d bytes, measured on the decoded value in UTF-8",
            MAX_VALUE_BYTES);
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
