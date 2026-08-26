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
package org.apache.polaris.service.it.env;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Pins the cleanup posture of {@link TagApi#skipPurgeForStatus}: only 406 (feature disabled) skips
 * quietly; 200 proceeds; every other status is a regression and must fail with the response body.
 */
public class TagApiTest {

  @Test
  public void testFeatureDisabledSkipsQuietly() {
    assertThat(TagApi.skipPurgeForStatus(406, () -> "unused")).isTrue();
  }

  @Test
  public void testOkProceeds() {
    assertThat(TagApi.skipPurgeForStatus(200, () -> "unused")).isFalse();
  }

  @ParameterizedTest
  @ValueSource(ints = {400, 401, 403, 404, 500})
  public void testAnyOtherStatusFailsWithBody(int status) {
    assertThatThrownBy(() -> TagApi.skipPurgeForStatus(status, () -> "the response body"))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(String.valueOf(status))
        .hasMessageContaining("the response body");
  }
}
