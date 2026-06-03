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

import java.util.ArrayList;
import java.util.List;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.jspecify.annotations.NonNull;

/** Shared helpers for persistence integration tests. */
final class PolarisPersistenceTestSupport {

  private PolarisPersistenceTestSupport() {}

  /**
   * Asserts two entities are equivalent, comparing JSON-backed fields by parsed map content instead
   * of raw serialized strings. This avoids false negatives when backends such as PostgreSQL JSONB
   * rewrite JSON formatting while preserving semantics.
   */
  static void assertEntitiesEquivalent(
      @NonNull PolarisBaseEntity actual, @NonNull PolarisBaseEntity expected) {
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

  static void assertEntitiesEquivalentInAnyOrder(
      @NonNull List<PolarisBaseEntity> actual, @NonNull List<PolarisBaseEntity> expected) {
    assertThat(actual).hasSameSizeAs(expected);
    List<PolarisBaseEntity> unmatched = new ArrayList<>(expected);
    for (PolarisBaseEntity actualEntity : actual) {
      PolarisBaseEntity match =
          unmatched.stream()
              .filter(candidate -> entitiesEquivalent(actualEntity, candidate))
              .findFirst()
              .orElse(null);
      assertThat(match)
          .as("No equivalent entity found in expected list for %s", actualEntity)
          .isNotNull();
      unmatched.remove(match);
    }
  }

  private static boolean entitiesEquivalent(
      @NonNull PolarisBaseEntity actual, @NonNull PolarisBaseEntity expected) {
    return PolarisEntity.toCore(actual).equals(PolarisEntity.toCore(expected))
        && actual.getSubTypeCode() == expected.getSubTypeCode()
        && actual.getCreateTimestamp() == expected.getCreateTimestamp()
        && actual.getDropTimestamp() == expected.getDropTimestamp()
        && actual.getPurgeTimestamp() == expected.getPurgeTimestamp()
        && actual.getToPurgeTimestamp() == expected.getToPurgeTimestamp()
        && actual.getLastUpdateTimestamp() == expected.getLastUpdateTimestamp()
        && actual.getGrantRecordsVersion() == expected.getGrantRecordsVersion()
        && actual.getPropertiesAsMap().equals(expected.getPropertiesAsMap())
        && actual.getInternalPropertiesAsMap().equals(expected.getInternalPropertiesAsMap());
  }
}
