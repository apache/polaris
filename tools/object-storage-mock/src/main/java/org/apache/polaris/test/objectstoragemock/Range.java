/*
 * Copyright (C) 2024 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// CODE_COPIED_TO_POLARIS from Project Nessie 0.106.1
package org.apache.polaris.test.objectstoragemock;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.immutables.value.Value;

@JsonSerialize(as = ImmutableRange.class)
@JsonDeserialize(as = ImmutableRange.class)
@Value.Immutable
public interface Range {

  String REQUESTED_RANGE_REGEXP = "^bytes[= ]((\\d*)-(\\d*))((,\\d*-\\d*)*)(/(\\d*))?";

  Pattern REQUESTED_RANGE_PATTERN = Pattern.compile(REQUESTED_RANGE_REGEXP);

  long start();

  long end();

  OptionalLong total();

  default boolean everything() {
    return start() == 0 && end() == Long.MAX_VALUE;
  }

  default long length() {
    var len = end() - start() + 1L;
    return len < 0 ? Long.MAX_VALUE : len;
  }

  /** Can parse AWS style and "standard" {@code Range} header values. */
  @SuppressWarnings("unused") // JAX-RS factory function
  static Range fromString(String rangeString) {
    requireNonNull(rangeString, "range string argument is null");

    // parsing a range specification of format: "bytes=start-end" - multiple ranges not supported
    rangeString = rangeString.trim();
    final Matcher matcher = REQUESTED_RANGE_PATTERN.matcher(rangeString);
    if (matcher.matches()) {
      String rangeStart = matcher.group(2);
      String rangeEnd = matcher.group(3);
      String total = matcher.group(7);

      if (matcher.groupCount() == 5 && !"".equals(matcher.group(4))) {
        throw new IllegalArgumentException(
            "Unsupported range specification. Only single range specifications allowed");
      }

      ImmutableRange.Builder range =
          ImmutableRange.builder()
              .start(rangeStart == null ? 0L : Long.parseLong(rangeStart))
              .end(rangeEnd.isEmpty() ? Long.MAX_VALUE : Long.parseLong(rangeEnd));

      if (total != null) {
        range.total(Long.parseLong(total));
      }

      return range.build();
    }
    throw new IllegalArgumentException(
        "Range header is malformed. Only bytes supported as range type.");
  }

  @Value.Check
  default void check() {
    if (start() < 0) {
      throw new IllegalArgumentException(
          "Unsupported range specification. A start byte must be supplied");
    }

    if (end() != -1 && end() < start()) {
      throw new IllegalArgumentException(
          "Range header is malformed. End byte is smaller than start byte.");
    }
  }
}
