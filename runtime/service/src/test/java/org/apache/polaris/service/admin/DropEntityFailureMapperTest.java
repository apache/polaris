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
package org.apache.polaris.service.admin;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class DropEntityFailureMapperTest {

  private static final String ENTITY_LABEL = "Principal 'alice'";
  private static final String UNDROPPABLE_MESSAGE = "Root principal cannot be dropped";

  @Test
  void throwIfFailed_success_doesNotThrow() {
    assertThatCode(
            () ->
                DropEntityFailureMapper.throwIfFailed(
                    new DropEntityResult(), () -> ENTITY_LABEL, UNDROPPABLE_MESSAGE))
        .doesNotThrowAnyException();
  }

  @Test
  void throwIfFailed_success_doesNotEvaluateLabel() {
    assertThatCode(
            () ->
                DropEntityFailureMapper.throwIfFailed(
                    new DropEntityResult(),
                    () -> {
                      throw new AssertionError("Label should not be evaluated on success");
                    },
                    UNDROPPABLE_MESSAGE))
        .doesNotThrowAnyException();
  }

  @Test
  void throwIfFailed_entityUndroppable_usesProtectedEntityMessage() {
    assertThatThrownBy(
            () ->
                DropEntityFailureMapper.throwIfFailed(
                    new DropEntityResult(BaseResult.ReturnStatus.ENTITY_UNDROPPABLE, null),
                    () -> ENTITY_LABEL,
                    UNDROPPABLE_MESSAGE))
        .isInstanceOf(BadRequestException.class)
        .hasMessage(UNDROPPABLE_MESSAGE);
  }

  @Test
  void throwIfFailed_concurrentModification_usesEntityLabelNotProtectedMessage() {
    assertThatThrownBy(
            () ->
                DropEntityFailureMapper.throwIfFailed(
                    new DropEntityResult(
                        BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED, null),
                    () -> ENTITY_LABEL,
                    UNDROPPABLE_MESSAGE))
        .isInstanceOf(BadRequestException.class)
        .hasMessage(
            "Principal 'alice' cannot be dropped, concurrent modification detected. Please try again")
        .hasMessageNotContaining("Root principal");
  }

  @Test
  void throwIfFailed_entityNotFound_throwsNotFoundException() {
    assertThatThrownBy(
            () ->
                DropEntityFailureMapper.throwIfFailed(
                    new DropEntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null),
                    () -> ENTITY_LABEL,
                    UNDROPPABLE_MESSAGE))
        .isInstanceOf(NotFoundException.class)
        .hasMessage("Principal 'alice' not found");
  }

  @ParameterizedTest
  @EnumSource(
      value = BaseResult.ReturnStatus.class,
      names = {"CATALOG_NOT_EMPTY", "NAMESPACE_NOT_EMPTY"})
  void throwIfFailed_notEmpty_reportsNotEmpty(BaseResult.ReturnStatus status) {
    assertThatThrownBy(
            () ->
                DropEntityFailureMapper.throwIfFailed(
                    new DropEntityResult(status, null), () -> "Catalog 'my-catalog'", null))
        .isInstanceOf(BadRequestException.class)
        .hasMessage("Catalog 'my-catalog' cannot be dropped, it is not empty");
  }

  @Test
  void throwIfFailed_unknownStatus_reportsGenericMessage() {
    assertThatThrownBy(
            () ->
                DropEntityFailureMapper.throwIfFailed(
                    new DropEntityResult(
                        BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED, null),
                    () -> ENTITY_LABEL,
                    UNDROPPABLE_MESSAGE))
        .isInstanceOf(BadRequestException.class)
        .hasMessage("Principal 'alice' cannot be dropped: CATALOG_PATH_CANNOT_BE_RESOLVED");
  }
}
