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

import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class PolarisEntityCoreTest {

  static Stream<Arguments> reservedAdminEntities() {
    return Stream.of(
        Arguments.of(
            Named.of(
                "root principal",
                new PrincipalEntity.Builder()
                    .setId(1L)
                    .setName(PolarisEntityConstants.getRootPrincipalName())
                    .build())),
        Arguments.of(
            Named.of(
                "service_admin principal role",
                new PrincipalRoleEntity.Builder()
                    .setId(2L)
                    .setName(PolarisEntityConstants.getNameOfPrincipalServiceAdminRole())
                    .build())),
        Arguments.of(
            Named.of(
                "catalog_admin catalog role",
                new CatalogRoleEntity.Builder()
                    .setId(3L)
                    .setName(PolarisEntityConstants.getNameOfCatalogAdminRole())
                    .build())));
  }

  static Stream<Arguments> ordinaryEntities() {
    return Stream.of(
        Arguments.of(
            Named.of(
                "non-root principal",
                new PrincipalEntity.Builder().setId(4L).setName("alice").build())),
        Arguments.of(
            Named.of(
                "non-admin principal role",
                new PrincipalRoleEntity.Builder().setId(5L).setName("data_engineer").build())),
        Arguments.of(
            Named.of(
                "non-admin catalog role",
                new CatalogRoleEntity.Builder().setId(6L).setName("reader").build())));
  }

  /**
   * The root principal must be protected alongside the admin roles: realm bootstrap identifies a
   * bootstrapped realm by the presence of a principal named {@code root}, so dropping or renaming
   * it breaks newly started processes for that realm.
   */
  @ParameterizedTest
  @MethodSource("reservedAdminEntities")
  void reservedAdminEntitiesCannotBeDroppedOrRenamed(PolarisEntityCore entity) {
    assertThat(entity.cannotBeDroppedOrRenamed()).isTrue();
  }

  @ParameterizedTest
  @MethodSource("ordinaryEntities")
  void ordinaryEntitiesCanBeDroppedOrRenamed(PolarisEntityCore entity) {
    assertThat(entity.cannotBeDroppedOrRenamed()).isFalse();
  }
}
