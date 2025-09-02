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
package org.apache.polaris.core.policy;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PredefinedPolicyTypesTest {

  @Test
  void fromCodeReturnsNullForNegative() {
    assertThat(PredefinedPolicyTypes.fromCode(-1)).isNull();
  }

  @Test
  void fromCodeReturnsTypeForValid() {
    int code = PredefinedPolicyTypes.DATA_COMPACTION.getCode();
    assertThat(PredefinedPolicyTypes.fromCode(code))
        .isEqualTo(PredefinedPolicyTypes.DATA_COMPACTION);
  }

  @Test
  void fromNameReturnsNullForUnknown() {
    assertThat(PredefinedPolicyTypes.fromName("__unknown__")).isNull();
  }
}

