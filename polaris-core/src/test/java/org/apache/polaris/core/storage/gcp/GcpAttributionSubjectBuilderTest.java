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
package org.apache.polaris.core.storage.gcp;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class GcpAttributionSubjectBuilderTest {

  @Test
  void simpleSubject() {
    assertThat(GcpAttributionSubjectBuilder.buildSubject("tenant1", "etl_writer"))
        .isEqualTo("tenant1/etl_writer");
  }

  @Test
  void neverExceedsGcpLimit() {
    assertThat(GcpAttributionSubjectBuilder.buildSubject("r".repeat(500), "p".repeat(500)))
        .hasSize(GcpAttributionSubjectBuilder.MAX_SUBJECT_LENGTH)
        .contains("/");
  }

  @Test
  void shortRealmGivesPrincipalMoreBudget() {
    String subject = GcpAttributionSubjectBuilder.buildSubject("t1", "p".repeat(500));
    // 127 - "t1/" (3 chars) = 124 chars of principal
    assertThat(subject).isEqualTo("t1/" + "p".repeat(124));
  }

  @Test
  void shortPrincipalGivesRealmMoreBudget() {
    String subject = GcpAttributionSubjectBuilder.buildSubject("r".repeat(500), "me");
    assertThat(subject).isEqualTo("r".repeat(124) + "/me");
  }

  @Test
  void bothLongSplitBudgetEvenly() {
    String subject = GcpAttributionSubjectBuilder.buildSubject("r".repeat(500), "p".repeat(500));
    // budget = 126; realm gets floor(126/2)=63, principal gets the remaining 63.
    assertThat(subject).isEqualTo("r".repeat(63) + "/" + "p".repeat(63));
  }

  @Test
  void nullAndEmptyBecomeUnknown() {
    assertThat(GcpAttributionSubjectBuilder.buildSubject(null, "p")).isEqualTo("unknown/p");
    assertThat(GcpAttributionSubjectBuilder.buildSubject("r", "")).isEqualTo("r/unknown");
    assertThat(GcpAttributionSubjectBuilder.buildSubject(null, null)).isEqualTo("unknown/unknown");
  }

  @Test
  void controlCharsAndSeparatorStripped() {
    assertThat(GcpAttributionSubjectBuilder.buildSubject("ten\r\nant", "etl\twriter"))
        .isEqualTo("tenant/etlwriter");
    // A '/' in a field must not introduce a second separator.
    String subject = GcpAttributionSubjectBuilder.buildSubject("tenant1", "a/b/c");
    assertThat(subject).isEqualTo("tenant1/abc");
    assertThat(subject.chars().filter(c -> c == '/').count()).isEqualTo(1);
  }
}
