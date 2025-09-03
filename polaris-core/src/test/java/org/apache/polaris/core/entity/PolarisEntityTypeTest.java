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

import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class PolarisEntityTypeTest {

  static Stream<Arguments> entityTypes() {
    return Stream.of(
        Arguments.of(-1, null),
        Arguments.of(0, PolarisEntityType.NULL_TYPE),
        Arguments.of(1, PolarisEntityType.ROOT),
        Arguments.of(2, PolarisEntityType.PRINCIPAL),
        Arguments.of(3, PolarisEntityType.PRINCIPAL_ROLE),
        Arguments.of(4, PolarisEntityType.CATALOG),
        Arguments.of(5, PolarisEntityType.CATALOG_ROLE),
        Arguments.of(6, PolarisEntityType.NAMESPACE),
        Arguments.of(7, PolarisEntityType.TABLE_LIKE),
        Arguments.of(8, PolarisEntityType.TASK),
        Arguments.of(9, PolarisEntityType.FILE),
        Arguments.of(10, PolarisEntityType.POLICY),
        Arguments.of(11, null));
  }

  @ParameterizedTest
  @MethodSource("entityTypes")
  public void testFromCode(int code, PolarisEntityType expected) {
    Assertions.assertThat(PolarisEntityType.fromCode(code)).isEqualTo(expected);
  }
}
