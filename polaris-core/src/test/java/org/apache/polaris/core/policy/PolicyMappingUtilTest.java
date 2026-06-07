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

import java.util.stream.Stream;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class PolicyMappingUtilTest {

  static Stream<Arguments> validTargetEntityTypes() {
    return Stream.of(
        Arguments.of(PolarisEntityType.CATALOG, null),
        Arguments.of(PolarisEntityType.NAMESPACE, null),
        Arguments.of(PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_TABLE));
  }

  static Stream<Arguments> invalidTargetEntityTypes() {
    return Stream.of(
        Arguments.of(null, null),
        Arguments.of(PolarisEntityType.PRINCIPAL_ROLE, null),
        Arguments.of(PolarisEntityType.TABLE_LIKE, null),
        Arguments.of(PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_VIEW));
  }

  @ParameterizedTest
  @MethodSource("validTargetEntityTypes")
  public void testValidTargetEntityTypes(
      PolarisEntityType entityType, PolarisEntitySubType entitySubType) {
    assertThat(PolicyMappingUtil.isValidTargetEntityType(entityType, entitySubType)).isTrue();
  }

  @ParameterizedTest
  @MethodSource("invalidTargetEntityTypes")
  public void testInvalidTargetEntityTypes(
      PolarisEntityType entityType, PolarisEntitySubType entitySubType) {
    assertThat(PolicyMappingUtil.isValidTargetEntityType(entityType, entitySubType)).isFalse();
  }
}
