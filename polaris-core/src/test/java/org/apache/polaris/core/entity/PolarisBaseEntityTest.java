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

import java.util.List;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link PolarisBaseEntity} focusing on correct value preservation through the Builder,
 * especially during JSON deserialization scenarios.
 */
class PolarisBaseEntityTest {

  /**
   * Regression test: Builder must preserve an explicitly set grantRecordsVersion=0 rather than
   * silently converting it to 1. A stored entity with grantRecordsVersion=0 (e.g. a newly-created
   * principal role that has never had grants modified) was being deserialized with
   * grantRecordsVersion=1, causing a grants_version_going_backward crash in ResolvedPolarisEntity
   * when the top-level grantsVersion in the same response remained 0.
   */
  @Test
  void builderPreservesExplicitGrantRecordsVersionZero() {
    PolarisBaseEntity entity =
        new PolarisBaseEntity.Builder()
            .catalogId(0L)
            .id(100L)
            .typeCode(PolarisEntityType.PRINCIPAL_ROLE.getCode())
            .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
            .parentId(0L)
            .name("test-principal-role")
            .entityVersion(1)
            .grantRecordsVersion(0)
            .build();

    Assertions.assertThat(entity.getGrantRecordsVersion())
        .as("explicitly set grantRecordsVersion(0) must be preserved without converting to 1")
        .isEqualTo(0);
  }

  /**
   * Verifies that a Builder with no explicit grantRecordsVersion call still produces a new entity
   * with grantRecordsVersion=1, preserving the intended initial value for brand-new entities.
   */
  @Test
  void builderDefaultsGrantRecordsVersionToOneWhenUnset() {
    PolarisBaseEntity entity =
        new PolarisBaseEntity.Builder()
            .catalogId(0L)
            .id(100L)
            .typeCode(PolarisEntityType.PRINCIPAL_ROLE.getCode())
            .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
            .parentId(0L)
            .name("test-principal-role")
            .entityVersion(1)
            .build();

    Assertions.assertThat(entity.getGrantRecordsVersion())
        .as("new entity with unset grantRecordsVersion must default to 1")
        .isEqualTo(1);
  }

  /**
   * Verifies that ResolvedPolarisEntity can be successfully constructed when entity
   * grantRecordsVersion and the provided grantsVersion are both 0. This was previously crashing
   * with grants_version_going_backward because the Builder silently converted the entity's version
   * from 0 to 1 while the top-level grantsVersion remained 0.
   */
  @Test
  void resolvedPolarisEntitySucceedsWhenGrantsVersionIsZero() {
    PolarisBaseEntity entity =
        new PolarisBaseEntity.Builder()
            .catalogId(0L)
            .id(100L)
            .typeCode(PolarisEntityType.PRINCIPAL_ROLE.getCode())
            .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
            .parentId(0L)
            .name("test-principal-role")
            .entityVersion(1)
            .grantRecordsVersion(0)
            .build();

    var diagnostics = new PolarisDefaultDiagServiceImpl();

    Assertions.assertThatNoException()
        .isThrownBy(() -> new ResolvedPolarisEntity(diagnostics, entity, List.of(), 0));
  }
}
