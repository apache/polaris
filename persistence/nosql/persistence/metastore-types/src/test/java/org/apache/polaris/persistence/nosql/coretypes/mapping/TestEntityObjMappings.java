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

package org.apache.polaris.persistence.nosql.coretypes.mapping;

import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestEntityObjMappings {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void checkCatalogId() {
    soft.assertThatCode(() -> EntityObjMappings.checkCatalogId(1)).doesNotThrowAnyException();
    soft.assertThatCode(() -> EntityObjMappings.checkCatalogId(Long.MAX_VALUE))
        .doesNotThrowAnyException();
    soft.assertThatIllegalArgumentException().isThrownBy(() -> EntityObjMappings.checkCatalogId(0));
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> EntityObjMappings.checkCatalogId(-1));
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> EntityObjMappings.checkCatalogId(Long.MIN_VALUE));
  }

  @Test
  public void invalidTypes() {
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                EntityObjMappings.objTypeForPolarisType(
                    PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.NULL_SUBTYPE))
        .withMessage("Invalid subType NULL_SUBTYPE for TABLE_LIKE");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                EntityObjMappings.objTypeForPolarisType(
                    PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.NULL_SUBTYPE))
        .withMessage("Invalid subType NULL_SUBTYPE for TABLE_LIKE");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> EntityObjMappings.byEntityType(PolarisEntityType.NULL_TYPE))
        .withMessage("No type mapping for entity type NULL_TYPE");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> EntityObjMappings.byEntityTypeCode(PolarisEntityType.NULL_TYPE.getCode()))
        .withMessage("No type mapping for entity type code 0");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> EntityObjMappings.byEntityTypeCode(666))
        .withMessage("No type mapping for entity type code 666");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> EntityObjMappings.byEntityTypeCode(-42))
        .withMessage("No type mapping for entity type code -42");
  }

  @ParameterizedTest
  @MethodSource("entityObjMapping")
  public void entityObjMapping(PolarisEntityType entityType, PolarisEntitySubType subType) {
    soft.assertThat(EntityObjMappings.byEntityType(entityType)).isNotNull();
    soft.assertThat(EntityObjMappings.typeFromCode(entityType.getCode())).isSameAs(entityType);
    soft.assertThat(EntityObjMappings.byEntityTypeCode(entityType.getCode()))
        .isNotNull()
        .isSameAs(EntityObjMappings.byEntityType(entityType));

    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                EntityObjMappings.objTypeForPolarisType(
                    entityType, PolarisEntitySubType.ANY_SUBTYPE))
        .withMessage("Unresolvable subtype ANY_SUBTYPE for exact match for %s", entityType);
    soft.assertThat(
            EntityObjMappings.objTypeForPolarisTypeForFiltering(
                entityType, PolarisEntitySubType.ANY_SUBTYPE))
        .isNotNull();
    soft.assertThat(EntityObjMappings.objTypeForPolarisTypeForFiltering(entityType, subType))
        .isNotNull();

    var objType = EntityObjMappings.objTypeForPolarisType(entityType, subType);
    soft.assertThat(objType).isNotNull();
    var entityTypeAndSubType = EntityObjMappings.entityTypeAndSubType(objType);
    soft.assertThat(entityTypeAndSubType)
        .isNotNull()
        .extracting(
            EntityObjMappings.EntityTypeAndSubType::entityType,
            EntityObjMappings.EntityTypeAndSubType::subType)
        .containsExactlyInAnyOrder(entityType, subType);
    soft.assertThat(entityTypeAndSubType.typeMapping())
        .extracting(BaseMapping::entityType)
        .isEqualTo(entityType);
    soft.assertThatCode(() -> entityTypeAndSubType.typeMapping().validateSubType(subType))
        .doesNotThrowAnyException();
    soft.assertThat(entityTypeAndSubType.typeMapping().objTypeForSubType(subType))
        .isEqualTo(objType);
  }

  static Stream<Arguments> entityObjMapping() {
    return Arrays.stream(PolarisEntityType.values())
        .filter(t -> t != PolarisEntityType.NULL_TYPE)
        .flatMap(
            t -> {
              var subTypes =
                  Arrays.stream(PolarisEntitySubType.values())
                      .filter(s -> s.getParentType() == t)
                      .toList();
              if (subTypes.isEmpty()) {
                return Stream.of(Arguments.of(t, PolarisEntitySubType.NULL_SUBTYPE));
              } else {
                return subTypes.stream().map(s -> Arguments.of(t, s));
              }
            });
  }
}
