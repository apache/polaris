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
package org.apache.polaris.core.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Comparator;
import java.util.List;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.jspecify.annotations.NonNull;

/** Shared helpers for persistence integration tests. */
final class PolarisPersistenceTestSupport {

  private static final Comparator<PolarisBaseEntity> SEMANTIC_ENTITY_COMPARATOR =
      (left, right) -> entitiesSemanticallyEquivalent(left, right) ? 0 : 1;

  private PolarisPersistenceTestSupport() {}

  /**
   * Asserts two entities are equivalent, comparing JSON-backed fields by parsed map content instead
   * of raw serialized strings. This avoids false negatives when backends such as PostgreSQL JSONB
   * rewrite JSON formatting while preserving semantics.
   */
  static void assertEntitiesEquivalent(
      @NonNull PolarisBaseEntity expected, @NonNull PolarisBaseEntity actual) {
    assertThat(PolarisEntity.toCore(actual)).isEqualTo(PolarisEntity.toCore(expected));
    assertThat(actual.getSubTypeCode()).isEqualTo(expected.getSubTypeCode());
    assertThat(actual.getCreateTimestamp()).isEqualTo(expected.getCreateTimestamp());
    assertThat(actual.getDropTimestamp()).isEqualTo(expected.getDropTimestamp());
    assertThat(actual.getPurgeTimestamp()).isEqualTo(expected.getPurgeTimestamp());
    assertThat(actual.getToPurgeTimestamp()).isEqualTo(expected.getToPurgeTimestamp());
    assertThat(actual.getLastUpdateTimestamp()).isEqualTo(expected.getLastUpdateTimestamp());
    assertThat(actual.getGrantRecordsVersion()).isEqualTo(expected.getGrantRecordsVersion());
    assertThat(actual.getPropertiesAsMap()).isEqualTo(expected.getPropertiesAsMap());
    assertThat(actual.getInternalPropertiesAsMap())
        .isEqualTo(expected.getInternalPropertiesAsMap());
  }

  /**
   * Like {@link #assertEntitiesEquivalent(PolarisBaseEntity, PolarisBaseEntity)} for unordered
   * lists. Uses a semantic comparator so {@code containsExactlyInAnyOrderElementsOf} still works
   * when PostgreSQL JSONB rewrites JSON formatting.
   */
  static void assertEntitiesEquivalentInAnyOrder(
      @NonNull List<PolarisBaseEntity> expected, @NonNull List<PolarisBaseEntity> actual) {
    assertThat(actual)
        .usingElementComparator(SEMANTIC_ENTITY_COMPARATOR)
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  /**
   * Unlike {@link PolarisBaseEntity#equals(Object)}, compares JSON-backed fields by parsed map
   * content rather than raw serialized strings.
   */
  private static boolean entitiesSemanticallyEquivalent(
      @NonNull PolarisBaseEntity left, @NonNull PolarisBaseEntity right) {
    return PolarisEntity.toCore(left).equals(PolarisEntity.toCore(right))
        && left.getSubTypeCode() == right.getSubTypeCode()
        && left.getCreateTimestamp() == right.getCreateTimestamp()
        && left.getDropTimestamp() == right.getDropTimestamp()
        && left.getPurgeTimestamp() == right.getPurgeTimestamp()
        && left.getToPurgeTimestamp() == right.getToPurgeTimestamp()
        && left.getLastUpdateTimestamp() == right.getLastUpdateTimestamp()
        && left.getGrantRecordsVersion() == right.getGrantRecordsVersion()
        && left.getPropertiesAsMap().equals(right.getPropertiesAsMap())
        && left.getInternalPropertiesAsMap().equals(right.getInternalPropertiesAsMap());
  }
}
