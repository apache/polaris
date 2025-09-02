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
package org.apache.polaris.core.persistence.dao.entity;

import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ReturnStatusTest {

  static Stream<Arguments> returnStatuses() {
    return Stream.of(
        Arguments.of(-1, null),
        Arguments.of(1, BaseResult.ReturnStatus.SUCCESS),
        Arguments.of(2, BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED),
        Arguments.of(3, BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED),
        Arguments.of(4, BaseResult.ReturnStatus.ENTITY_CANNOT_BE_RESOLVED),
        Arguments.of(5, BaseResult.ReturnStatus.ENTITY_NOT_FOUND),
        Arguments.of(6, BaseResult.ReturnStatus.GRANT_NOT_FOUND),
        Arguments.of(7, BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS),
        Arguments.of(8, BaseResult.ReturnStatus.ENTITY_UNDROPPABLE),
        Arguments.of(9, BaseResult.ReturnStatus.NAMESPACE_NOT_EMPTY),
        Arguments.of(10, BaseResult.ReturnStatus.CATALOG_NOT_EMPTY),
        Arguments.of(11, BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED),
        Arguments.of(12, BaseResult.ReturnStatus.ENTITY_CANNOT_BE_RENAMED),
        Arguments.of(13, BaseResult.ReturnStatus.SUBSCOPE_CREDS_ERROR),
        Arguments.of(14, BaseResult.ReturnStatus.POLICY_MAPPING_NOT_FOUND),
        Arguments.of(15, BaseResult.ReturnStatus.POLICY_MAPPING_OF_SAME_TYPE_ALREADY_EXISTS),
        Arguments.of(16, BaseResult.ReturnStatus.POLICY_HAS_MAPPINGS),
        Arguments.of(17, null));
  }

  @ParameterizedTest
  @MethodSource("returnStatuses")
  public void testReturnStatusFromCode(int code, BaseResult.ReturnStatus expected) {
    BaseResult.ReturnStatus actual = BaseResult.ReturnStatus.getStatus(code);
    Assertions.assertThat(actual).isEqualTo(expected);
  }
}


