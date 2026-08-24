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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.exceptions.BadRequestException;
import org.junit.jupiter.api.Test;

class TagValidationTest {

  @Test
  void valueLengthIsBoundedInUtf8Bytes() {
    String atLimit = "a".repeat(TagValidation.MAX_VALUE_BYTES);
    String overLimit = "a".repeat(TagValidation.MAX_VALUE_BYTES + 1);
    // two-byte characters: half the count reaches the same byte length
    String multiByteAtLimit = "\u00e9".repeat(TagValidation.MAX_VALUE_BYTES / 2);
    String multiByteOverLimit = "\u00e9".repeat(TagValidation.MAX_VALUE_BYTES / 2 + 1);

    assertThatCode(() -> TagValidation.validateValueLength(atLimit, "A value"))
        .doesNotThrowAnyException();
    assertThatCode(() -> TagValidation.validateValueLength(multiByteAtLimit, "A value"))
        .doesNotThrowAnyException();
    assertThatThrownBy(() -> TagValidation.validateValueLength(overLimit, "A value"))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining(String.valueOf(TagValidation.MAX_VALUE_BYTES))
        .hasMessageContaining("bytes");
    assertThatThrownBy(() -> TagValidation.validateValueLength(multiByteOverLimit, "A value"))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void allowedValuesApplyTheSameBound() {
    assertThatCode(
            () ->
                TagValidation.validateAllowedValues(
                    List.of("a".repeat(TagValidation.MAX_VALUE_BYTES))))
        .doesNotThrowAnyException();
    assertThatThrownBy(
            () ->
                TagValidation.validateAllowedValues(
                    List.of("a".repeat(TagValidation.MAX_VALUE_BYTES + 1))))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("bytes");
  }
}
