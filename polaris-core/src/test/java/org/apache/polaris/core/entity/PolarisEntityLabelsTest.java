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
package org.apache.polaris.core.entity;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for the entity-label helpers on {@link PolarisBaseEntity}: {@code getLabels()}, {@code
 * matchesLabelFilter()}, and {@code BaseBuilder.setLabels()}.
 */
class PolarisEntityLabelsTest {

  private PolarisEntity entityWithLabels(Map<String, String> labels) {
    PolarisEntity.Builder builder =
        new PolarisEntity.Builder()
            .setType(PolarisEntityType.CATALOG)
            .setSubType(PolarisEntitySubType.NULL_SUBTYPE)
            .setName("test");
    if (!labels.isEmpty()) {
      builder.setLabels(labels);
    }
    return builder.build();
  }

  @Test
  void testGetLabels_noLabels_returnsEmpty() {
    PolarisEntity entity = entityWithLabels(Map.of());
    assertThat(entity.getLabels()).isEmpty();
  }

  @Test
  void testGetLabels_withLabels_returnsCorrectMap() {
    Map<String, String> labels = Map.of("env", "prod", "team", "data");
    PolarisEntity entity = entityWithLabels(labels);
    assertThat(entity.getLabels()).containsAllEntriesOf(labels);
  }

  @Test
  void testSetLabels_replacesExistingLabels() {
    PolarisEntity original = entityWithLabels(Map.of("env", "dev"));
    PolarisEntity updated =
        new PolarisEntity.Builder(original).setLabels(Map.of("env", "prod")).build();
    assertThat(updated.getLabels()).containsEntry("env", "prod").hasSize(1);
  }

  @Test
  void testSetLabels_emptyMap_clearsLabels() {
    PolarisEntity original = entityWithLabels(Map.of("env", "prod"));
    PolarisEntity cleared = new PolarisEntity.Builder(original).setLabels(Map.of()).build();
    assertThat(cleared.getLabels()).isEmpty();
    assertThat(cleared.getInternalPropertiesAsMap())
        .doesNotContainKey(PolarisBaseEntity.LABELS_INTERNAL_KEY);
  }

  @Test
  void testMatchesLabelFilter_emptyFilter_alwaysTrue() {
    PolarisEntity entity = entityWithLabels(Map.of("env", "prod"));
    assertThat(entity.matchesLabelFilter(Map.of())).isTrue();
  }

  @Test
  void testMatchesLabelFilter_exactMatch_returnsTrue() {
    PolarisEntity entity = entityWithLabels(Map.of("env", "prod", "team", "data"));
    assertThat(entity.matchesLabelFilter(Map.of("env", "prod"))).isTrue();
  }

  @Test
  void testMatchesLabelFilter_wrongValue_returnsFalse() {
    PolarisEntity entity = entityWithLabels(Map.of("env", "prod"));
    assertThat(entity.matchesLabelFilter(Map.of("env", "dev"))).isFalse();
  }

  @Test
  void testMatchesLabelFilter_missingKey_returnsFalse() {
    PolarisEntity entity = entityWithLabels(Map.of("env", "prod"));
    assertThat(entity.matchesLabelFilter(Map.of("team", "data"))).isFalse();
  }

  @Test
  void testMatchesLabelFilter_multipleFilters_allMustMatch() {
    PolarisEntity entity = entityWithLabels(Map.of("env", "prod", "team", "data"));
    assertThat(entity.matchesLabelFilter(Map.of("env", "prod", "team", "data"))).isTrue();
    assertThat(entity.matchesLabelFilter(Map.of("env", "prod", "team", "wrong"))).isFalse();
  }

  @Test
  void testLabelsStoredInInternalProperties() {
    PolarisEntity entity = entityWithLabels(Map.of("env", "prod"));
    assertThat(entity.getInternalPropertiesAsMap())
        .containsKey(PolarisBaseEntity.LABELS_INTERNAL_KEY);
    // User-visible properties map must NOT contain labels
    assertThat(entity.getPropertiesAsMap())
        .doesNotContainKey(PolarisBaseEntity.LABELS_INTERNAL_KEY);
  }
}
