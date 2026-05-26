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

import java.util.List;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.core.entity.CatalogEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tests for {@link CatalogEntity#validateBaseLocationAgainstAllowedList}, the helper used by
 * {@code updateCatalog} (to validate the effective default-base-location against the effective
 * allowed-locations) and by {@code CatalogEntity.Builder.setStorageConfigurationInfo} (to
 * validate the user-supplied allowed-locations on create / storage-config replacement).
 */
public class BaseLocationValidationTest {

  @ParameterizedTest(name = "[{index}] base={1} within allowed={0}")
  @CsvSource({"s3://bucket/, s3://bucket/path/to/data", "s3://bucket/, s3://bucket/"})
  public void testBaseWithinAllowed_accepted(String allowed, String base) {
    assertThatCode(
            () -> CatalogEntity.validateBaseLocationAgainstAllowedList(List.of(allowed), base))
        .doesNotThrowAnyException();
  }

  @Test
  public void testAcceptedUnderAnyOfMultipleAllowedLocations() {
    assertThatCode(
            () ->
                CatalogEntity.validateBaseLocationAgainstAllowedList(
                    List.of("s3://bucket-a/", "s3://bucket-b/warehouse/"),
                    "s3://bucket-b/warehouse/data"))
        .doesNotThrowAnyException();
  }

  @Test
  public void testBaseOutsideAllowed_rejected() {
    assertThatThrownBy(
            () ->
                CatalogEntity.validateBaseLocationAgainstAllowedList(
                    List.of("s3://bucket/"), "s3://other-bucket/data"))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("s3://other-bucket/data")
        .hasMessageContaining("s3://bucket/");
  }

  @Test
  public void testDifferentSchemeRejected() {
    assertThatThrownBy(
            () ->
                CatalogEntity.validateBaseLocationAgainstAllowedList(
                    List.of("s3://bucket/"), "gs://bucket/data"))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  public void testEmptyAllowedList_rejected() {
    // Empty allowed-list means no location is allowed at runtime
    // (see InMemoryStorageIntegration line 99-100); validation must match.
    assertThatThrownBy(
            () ->
                CatalogEntity.validateBaseLocationAgainstAllowedList(List.of(), "s3://anything/"))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("empty")
        .hasMessageContaining("s3://anything/");
  }

  @Test
  public void testNullAllowedList_rejected() {
    assertThatThrownBy(
            () -> CatalogEntity.validateBaseLocationAgainstAllowedList(null, "s3://anything/"))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("null")
        .hasMessageContaining("s3://anything/");
  }
}
